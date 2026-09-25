/*
Copyright © 2025 Dell Inc. or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	drv1 "github.com/Ecosystems/container-storage-modules/src/csm-dr/api/v1"
	"github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage"
	"github.com/Ecosystems/container-storage-modules/src/csmlog"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/proto"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
)

var k8sErrorNotFoundFunc = k8sErrors.IsNotFound

type (
	contextKey    string
	CSIMethodFunc func(context.Context, *VolumeJournalReconciler, drv1.JournalEntry) (interface{}, error)
)

const (
	requeueAfterDuration                    = 1 * time.Minute
	DeferredKey                  contextKey = "isDeferredOperation"
	volumeJournalDriverTypeLabel            = "dr.storage.dell.com/driver-type"
	powerMaxDriverType                      = "powermax"
)

// shouldReconcileVolumeJournal keeps CSM-DR from handling journals owned by
// the PowerMax Metro controller. Resources without an ownership label remain
// supported for backward compatibility with older CSM-DR journal writers.
func shouldReconcileVolumeJournal(obj client.Object) bool {
	return obj.GetLabels()[volumeJournalDriverTypeLabel] != powerMaxDriverType
}

// VolumeJournalReconciler reconciles a VolumeJournal object
type VolumeJournalReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	CSIControllerServer csi.ControllerServer
	CSINodeServer       csi.NodeServer
	NodeName            string
	Mode                string

	*storage.ArrayPinger
}

// +kubebuilder:rbac:groups=dr.storage.dell.com,resources=volumejournals,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dr.storage.dell.com,resources=volumejournals/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dr.storage.dell.com,resources=volumejournals/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the VolumeJournal object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/reconcile
func (r *VolumeJournalReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	csmlog.Infof("Reconciling %v VolumeJournal", req.NamespacedName)

	var volumeJournal drv1.VolumeJournal
	err := r.Get(ctx, req.NamespacedName, &volumeJournal)
	if err != nil {
		if k8sErrorNotFoundFunc(err) {
			csmlog.Info("[CSM-DR] VolumeJournal resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		csmlog.Errorf("Failed to get VolumeJournal: %v", err)
		return ctrl.Result{}, err
	}
	if !shouldReconcileVolumeJournal(&volumeJournal) {
		csmlog.Infof("[CSM-DR] Ignoring VolumeJournal owned by another driver")
		return ctrl.Result{}, nil
	}

	for i, journalEntry := range volumeJournal.Spec.JournalEntries {
		csmlog.Infof("Volume Operation: %v", journalEntry.Operation)
		var execute CSIMethodFunc

		// Requeue until both the original and failover arrays are online
		if !r.ArrayPinger.IsArrayOnline(ctx, volumeJournal.Spec.OriginalArray) || !r.ArrayPinger.IsArrayOnline(ctx, volumeJournal.Spec.FailoverArray) {
			csmlog.Infof("Array %s or Remote Array %s is offline", volumeJournal.Spec.OriginalArray, volumeJournal.Spec.FailoverArray)
			result := ctrl.Result{RequeueAfter: requeueAfterDuration}
			return result, nil
		}

		if journalEntry.Status == "pending-reconciliation" {

			var isValidNode bool
			var reconciliationLog string

			if strings.Contains(journalEntry.Operation, "Node") && strings.EqualFold(r.Mode, "node") {
				if journalEntry.Host == r.NodeName {
					// Node Deferred Operation should be handled in the node pod mentioned in the journal entry
					isValidNode = true
				} else {
					isValidNode = false
					reconciliationLog = fmt.Sprintf("[CSM-DR] Reconciliation exiting as the host %s is different from the node %s", journalEntry.Host, r.NodeName)
				}
			} else if strings.Contains(journalEntry.Operation, "Controller") && strings.EqualFold(r.Mode, "controller") {
				// Controller Deferred Operation should be handled in the controller pod
				isValidNode = true
			} else {
				isValidNode = false
				reconciliationLog = "[CSM-DR] Exiting Reconciliation"
			}

			if !isValidNode {
				csmlog.Info(reconciliationLog)
				return ctrl.Result{}, nil
			}

			patch := client.MergeFrom(volumeJournal.DeepCopy())

			execute, err = getCSIFunction(journalEntry.Operation)
			if err != nil {
				csmlog.Errorf("Failed to get volume operation: %v", err)
				return ctrl.Result{}, err
			}

			_, err = execute(ctx, r, journalEntry)
			if err != nil {
				csmlog.Errorf("Failed to perform volume operation successfully: %v", err)
				return ctrl.Result{}, err
			}
			volumeJournal.Spec.JournalEntries[i].Status = "reconciled"
			if err := r.Patch(ctx, &volumeJournal, patch); err != nil {
				if k8sErrors.IsConflict(err) {
					csmlog.Warnf("Conflict while patching the VolumeJournal, requeueing: %v", err)
					return ctrl.Result{Requeue: true}, nil
				}
				csmlog.Errorf("Failed to patch VolumeJournal: %v", err)
				return ctrl.Result{}, err
			}
			csmlog.Infof("Reconciliation completed successfully for the Journal Entry: %v", journalEntry.Operation)
		}
	}

	allReconciled := true
	for _, journalEntry := range volumeJournal.Spec.JournalEntries {
		if journalEntry.Status == "pending-reconciliation" {
			allReconciled = false
			break
		}
	}
	if allReconciled {
		csmlog.Infof("All Journal Entries are reconciled. Deleting VolumeJournal")
		err = r.Delete(ctx, &volumeJournal)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *VolumeJournalReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&drv1.VolumeJournal{}).
		WithEventFilter(predicate.NewPredicateFuncs(shouldReconcileVolumeJournal)).
		Named("volumejournal" + r.Mode).
		Complete(r)
}

var csiFunctionsMap = map[string]CSIMethodFunc{
	"NodeStageVolume": func(ctx context.Context, r *VolumeJournalReconciler, volumeJournal drv1.JournalEntry) (interface{}, error) {
		request := &csi.NodeStageVolumeRequest{}
		err := proto.Unmarshal(volumeJournal.Request, request)
		if err != nil {
			return err, nil
		}

		csmlog.Infof("[CSM-DR] NodeStageVolume Replay request=%v", request)

		modifiedContext := context.WithValue(ctx, DeferredKey, true)

		return r.CSINodeServer.NodeStageVolume(modifiedContext, request)
	},

	"NodePublishVolume": func(ctx context.Context, r *VolumeJournalReconciler, volumeJournal drv1.JournalEntry) (interface{}, error) {
		request := &csi.NodePublishVolumeRequest{}
		err := proto.Unmarshal(volumeJournal.Request, request)
		if err != nil {
			return err, nil
		}

		csmlog.Infof("[CSM-DR] NodePublishVolume Replay request=%v", request)

		modifiedContext := context.WithValue(ctx, DeferredKey, true)
		return r.CSINodeServer.NodePublishVolume(modifiedContext, request)
	},

	"NodeUnpublishVolume": func(ctx context.Context, r *VolumeJournalReconciler, volumeJournal drv1.JournalEntry) (interface{}, error) {
		request := &csi.NodeUnpublishVolumeRequest{}
		err := proto.Unmarshal(volumeJournal.Request, request)
		if err != nil {
			return err, nil
		}

		csmlog.Infof("[CSM-DR] NodeUnpublishVolume Replay request=%v", request)

		modifiedContext := context.WithValue(ctx, DeferredKey, true)
		return r.CSINodeServer.NodeUnpublishVolume(modifiedContext, request)
	},

	"NodeUnstageVolume": func(ctx context.Context, r *VolumeJournalReconciler, volumeJournal drv1.JournalEntry) (interface{}, error) {
		request := &csi.NodeUnstageVolumeRequest{}
		err := proto.Unmarshal(volumeJournal.Request, request)
		if err != nil {
			return err, nil
		}

		csmlog.Infof("[CSM-DR] NodeUnstageVolume Replay request=%v", request)

		modifiedContext := context.WithValue(ctx, DeferredKey, true)
		return r.CSINodeServer.NodeUnstageVolume(modifiedContext, request)
	},

	"ControllerPublishVolume": func(ctx context.Context, r *VolumeJournalReconciler, volumeJournal drv1.JournalEntry) (interface{}, error) {
		request := &csi.ControllerPublishVolumeRequest{}
		err := proto.Unmarshal(volumeJournal.Request, request)
		if err != nil {
			return err, nil
		}

		csmlog.Infof("[CSM-DR] ControllerPublishVolume Replay request=%v", request)

		modifiedContext := context.WithValue(ctx, DeferredKey, true)
		return r.CSIControllerServer.ControllerPublishVolume(modifiedContext, request)
	},

	"ControllerUnpublishVolume": func(ctx context.Context, r *VolumeJournalReconciler, volumeJournal drv1.JournalEntry) (interface{}, error) {
		request := &csi.ControllerUnpublishVolumeRequest{}
		err := proto.Unmarshal(volumeJournal.Request, request)
		if err != nil {
			return err, nil
		}

		csmlog.Infof("[CSM-DR] ControllerUnpublishVolume Replay request=%v", request)

		modifiedContext := context.WithValue(ctx, DeferredKey, true)
		return r.CSIControllerServer.ControllerUnpublishVolume(modifiedContext, request)
	},
}

func getCSIFunction(volumeOperation string) (CSIMethodFunc, error) {
	csmlog.Infof("CSI Volume Operation: %s", volumeOperation)
	csiMethod, ok := csiFunctionsMap[volumeOperation]
	if !ok {
		return nil, errors.New("CSI volume operation not found")
	}
	return csiMethod, nil
}
