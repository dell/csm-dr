/*
Copyright 2025.

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
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Ecosystems/container-storage-modules/src/csmlog"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"

	drv1 "github.com/Ecosystems/container-storage-modules/src/csm-dr/api/v1"
	"github.com/Ecosystems/container-storage-modules/src/csm-dr/internal/controller"
	"github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsServer "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

var (
	scheme = runtime.NewScheme()

	createManagerFunc   = createManagerInstance
	SetupControllerFunc = setupControllerManager
	AddHealthChecksFunc = addHealthChecks
	signalHandlerOnce   sync.Once
	globalSignalCtx     context.Context
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(drv1.AddToScheme(scheme))
}

func GetScheme() *runtime.Scheme {
	return scheme
}

func createManagerInstance(probeAddr string, enableLeaderElection bool, metricsAddr string) (ctrl.Manager, error) {
	metricsOptions := metricsServer.Options{BindAddress: "0"}
	if metricsAddr != "" {
		metricsOptions = metricsServer.Options{BindAddress: metricsAddr}
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsOptions,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "csm-dr-manager",
	})
	if err != nil {
		csmlog.Errorf("unable to start manager: %v", err)
		return nil, err
	}
	return mgr, nil
}

func setupControllerManager(setupFunc func(ctrl.Manager) error, mgr ctrl.Manager) error {
	if err := setupFunc(mgr); err != nil {
		csmlog.Errorf("unable to create controller: %v", err)
		return err
	}
	return nil
}

func setupLogger() {
	ctrl.SetLogger(logr.New(&csmLogSink{}))
}

func Initialize(
	nodeService csi.NodeServer,
	controllerService csi.ControllerServer,
	arrays storage.ArrayList,
	mode, nodeName, probeAddr string,
	enableLeaderElection bool,
	metricsAddr ...string,
) (client.Client, error) {
	setupLogger()
	csmlog.Info("Starting initialize")

	var metricsAddress string
	if len(metricsAddr) > 0 {
		metricsAddress = metricsAddr[0]
	}

	mgr, err := createManagerFunc(probeAddr, enableLeaderElection, metricsAddress)
	if err != nil {
		csmlog.Errorf("Unable to create manager: %v", err)
		return nil, err
	}

	var volumeJournalReconciler *controller.VolumeJournalReconciler
	if strings.EqualFold(mode, "node") {
		csmlog.Infof("Using NodeService, nodeName=%s", nodeName)
		volumeJournalReconciler = &controller.VolumeJournalReconciler{
			Client:        mgr.GetClient(),
			Scheme:        mgr.GetScheme(),
			CSINodeServer: nodeService,
			NodeName:      nodeName,
			Mode:          mode,
			ArrayPinger:   storage.NewArrayPinger(arrays, 1*time.Minute),
		}
	} else if strings.EqualFold(mode, "controller") {
		csmlog.Info("Using Controller Service")
		volumeJournalReconciler = &controller.VolumeJournalReconciler{
			Client:              mgr.GetClient(),
			Scheme:              mgr.GetScheme(),
			CSIControllerServer: controllerService,
			Mode:                mode,
			ArrayPinger:         storage.NewArrayPinger(arrays, 1*time.Minute),
		}
	}

	if err = SetupControllerFunc(volumeJournalReconciler.SetupWithManager, mgr); err != nil {
		csmlog.Errorf("Unable to setup controller: %v", err)
		return nil, err
	}

	if err := AddHealthChecksFunc(mgr); err != nil {
		return nil, err
	}

	// Since this is baked in within a different application, start and run in the background.
	csmlog.Info("Starting Manager")
	go func() {
		if err := mgr.Start(getSignalHandler()); err != nil {
			os.Exit(1)
		}
	}()

	return mgr.GetClient(), nil
}

func getSignalHandler() context.Context {
	signalHandlerOnce.Do(func() {
		globalSignalCtx = ctrl.SetupSignalHandler()
	})
	return globalSignalCtx
}

func addHealthChecks(mgr ctrl.Manager) error {
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		csmlog.Errorf("unable to set up health check: %v", err)
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		csmlog.Errorf("unable to set up ready check: %v", err)
		return err
	}
	return nil
}

// csmLogSink implements logr.LogSink to bridge controller-runtime logging to csmlog.
type csmLogSink struct {
	name   string
	values []interface{}
}

func (s *csmLogSink) Init(_ logr.RuntimeInfo) {}

func (s *csmLogSink) Enabled(_ int) bool {
	return true
}

func (s *csmLogSink) Info(level int, msg string, keysAndValues ...interface{}) {
	l := csmlog.WithFields(s.fields(keysAndValues...))
	if level == 0 {
		l.Info(msg)
	} else {
		l.Debug(msg)
	}
}

func (s *csmLogSink) Error(err error, msg string, keysAndValues ...interface{}) {
	l := csmlog.WithFields(s.fields(keysAndValues...))
	if err != nil {
		l = l.WithFields(csmlog.Fields{"error": err.Error()})
	}
	l.Error(msg)
}

func (s *csmLogSink) WithValues(keysAndValues ...interface{}) logr.LogSink {
	newValues := make([]interface{}, len(s.values)+len(keysAndValues))
	copy(newValues, s.values)
	copy(newValues[len(s.values):], keysAndValues)
	return &csmLogSink{
		name:   s.name,
		values: newValues,
	}
}

func (s *csmLogSink) WithName(name string) logr.LogSink {
	newName := name
	if s.name != "" {
		newName = s.name + "." + name
	}
	return &csmLogSink{
		name:   newName,
		values: s.values,
	}
}

func (s *csmLogSink) fields(keysAndValues ...interface{}) csmlog.Fields {
	fields := csmlog.Fields{}
	if s.name != "" {
		fields["logger"] = s.name
	}
	allKV := append(s.values, keysAndValues...)
	for i := 0; i < len(allKV)-1; i += 2 {
		key, ok := allKV[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", allKV[i])
		}
		fields[key] = allKV[i+1]
	}
	return fields
}
