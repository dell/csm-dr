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
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/mock"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	drv1 "github.com/Ecosystems/container-storage-modules/src/csm-dr/api/v1"
	"github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage"
	mock_storage "github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage/mocks"
	"github.com/Ecosystems/container-storage-modules/src/csmlog"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type MockClient struct {
	mock.Mock
	client.Client
}

func (m *MockClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	args := m.Called(ctx, obj, patch)
	return args.Error(0)
}

func (m *MockClient) Delete(ctx context.Context, obj client.Object, _ ...client.DeleteOption) error {
	args := m.Called(ctx, obj)
	return args.Error(0)
}

func SetupScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = drv1.AddToScheme(scheme)
	return scheme
}

func TestReconcile(t *testing.T) {
	tests := []struct {
		name          string
		volumeJournal drv1.VolumeJournal
		arrays        func(tt *testing.T) *storage.ArrayPinger
		expectErr     error
		nodeName      string
		mode          string
		k8sErrorFunc  func(error) bool
		requestFunc   func() []byte
		patchError    error
		deleteError   error
	}{
		{
			name:     "NodeStageVolume Operation: Pending Reconciliation",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeStageVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			patchError: nil,
			expectErr:  nil,
		},
		{
			name:     "NodeUnstageVolume Operation: Pending Reconciliation",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeUnstageVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeUnstageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name:     "NodePublishVolume Operation: Pending Reconciliation",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodePublishVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodePublishVolumeRequest{
					VolumeId: "testVolume",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name:     "NodeUnpublishVolume Operation: Pending Reconciliation",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeUnpublishVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeUnpublishVolumeRequest{
					VolumeId: "testVolume",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name: "ControllerPublishVolume Operation: Pending Reconciliation",
			mode: "controller",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "ControllerPublishVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.ControllerPublishVolumeRequest{
					VolumeId: "testVolume",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name: "ControllerUnpublishVolume Operation: Pending Reconciliation",
			mode: "controller",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "ControllerUnpublishVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.ControllerUnpublishVolumeRequest{
					VolumeId: "testVolume",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name:     "NodeStageVolume Operation on Node different from Journal Entry Host",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeStageVolume",
							Host:      "node2",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name:     "NodeStageVolume Operation on Controller Pod",
			mode:     "controller",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeStageVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name:     "NodeStageVolume Operation: Pending Reconciliation Failure",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeStageVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: errors.New("Volume ID cannot be empty"),
		},
		{
			name: "Offline Array",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "TestOperation",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(false)

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: nil,
		},
		{
			name:     "IsNotFound error",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource1",
					Namespace: "namespace1",
				},
				Spec: drv1.VolumeJournalSpec{
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "TestOperation",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				return nil
			},
			arrays: func(_ *testing.T) *storage.ArrayPinger {
				return nil
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return true
			},
			expectErr: nil,
		},
		{
			name:     "Error getting the VolumeJournal",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource1",
					Namespace: "namespace1",
				},
				Spec: drv1.VolumeJournalSpec{
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "TestOperation",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			arrays: func(_ *testing.T) *storage.ArrayPinger {
				return nil
			},
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			patchError: nil,
			requestFunc: func() []byte {
				return nil
			},
			expectErr: errors.New("volumejournals.dr.storage.dell.com \"test-resource\" not found"),
		},
		{
			name:     "Invalid Volume Operation",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeGetCapabilities",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeGetCapabilitiesRequest{}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			patchError: nil,
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			expectErr: errors.New("CSI volume operation not found"),
		},
		{
			name:     "NodeStageVolume Operation: Non-Conflict Patch Error",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeStageVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			patchError: errors.New("internal server error"),
			expectErr:  errors.New("internal server error"),
		},
		{
			name:     "Delete VolumeJournal Failure",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeStageVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			patchError:  nil,
			deleteError: errors.New("delete failed"),
			expectErr:   errors.New("delete failed"),
		},
		{
			name:     "NodeStageVolume Operation: Conflicting Error updating the VolumeJournal",
			mode:     "node",
			nodeName: "node1",
			volumeJournal: drv1.VolumeJournal{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-resource",
					Namespace: "default",
				},
				Spec: drv1.VolumeJournalSpec{
					OriginalArray: "array1",
					FailoverArray: "array2",
					JournalEntries: []drv1.JournalEntry{
						{
							Operation: "NodeStageVolume",
							Host:      "node1",
							Array:     "array1",
							Time:      "2022-01-01T00:00:00Z",
							Status:    "pending-reconciliation",
							Request:   []byte(""),
						},
					},
				},
			},
			requestFunc: func() []byte {
				request := csi.NodeStageVolumeRequest{
					VolumeId:          "testVolume",
					StagingTargetPath: "/staging",
				}
				data, err := proto.Marshal(&request)
				if err != nil {
					return nil
				}
				return data
			},
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				healthChecker := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				healthChecker.EXPECT().IsOnline(gomock.Any()).Return(true).AnyTimes()

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array1").Return(healthChecker, nil)
				arrays.EXPECT().Get("array2").Return(healthChecker, nil)

				return storage.NewArrayPinger(arrays, 0)
			},
			k8sErrorFunc: func(_ error) bool {
				return false
			},
			patchError: k8sErrors.NewConflict(schema.GroupResource{Group: "dr", Resource: "VolumeJournal"}, "test-resource", errors.New("conflict error")),
			expectErr:  nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := k8sErrorNotFoundFunc
			defer t.Cleanup(func() {
				k8sErrorNotFoundFunc = original
			})
			csmlog.Infof("Data: %v", tt.volumeJournal.Spec.JournalEntries[0])
			tt.volumeJournal.Spec.JournalEntries[0].Request = tt.requestFunc()
			volumeJournal := &tt.volumeJournal
			ctx := context.Background()
			k8sErrorNotFoundFunc = tt.k8sErrorFunc

			scheme := SetupScheme()
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(volumeJournal).Build()
			client := &MockClient{
				Client: fakeClient,
			}
			client.On("Patch", mock.Anything, mock.Anything, mock.Anything).
				Return(tt.patchError)
			client.On("Delete", mock.Anything, mock.Anything).
				Return(tt.deleteError).Maybe()

			reconciler := &VolumeJournalReconciler{
				Scheme:              scheme,
				Client:              client,
				Mode:                tt.mode,
				NodeName:            tt.nodeName,
				CSINodeServer:       &testNodeServer{},
				CSIControllerServer: &TestControllerServer{},
				ArrayPinger:         tt.arrays(t),
			}

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      "test-resource",
					Namespace: "default",
				},
			})

			if err != nil && tt.expectErr != nil {
				if err.Error() != tt.expectErr.Error() {
					t.Errorf("Reconcile() error = %v, wantErr %v", err, tt.expectErr)
				}
			} else {
				if err != tt.expectErr {
					t.Errorf("Reconcile() error = %v, wantErr %v", err, tt.expectErr)
				}
			}
		})
	}
}

func TestShouldReconcileVolumeJournal(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   bool
	}{
		{
			name: "legacy resource without driver label",
			want: true,
		},
		{
			name: "CSM-DR resource",
			labels: map[string]string{
				volumeJournalDriverTypeLabel: "powerstore",
			},
			want: true,
		},
		{
			name: "PowerMax resource",
			labels: map[string]string{
				volumeJournalDriverTypeLabel: "powermax",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			journal := &drv1.VolumeJournal{ObjectMeta: metav1.ObjectMeta{Labels: tt.labels}}
			if got := shouldReconcileVolumeJournal(journal); got != tt.want {
				t.Errorf("shouldReconcileVolumeJournal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetupWithManager(t *testing.T) {
	volumeJournal := &drv1.VolumeJournal{
		Spec: drv1.VolumeJournalSpec{
			JournalEntries: []drv1.JournalEntry{
				{
					Operation: "TestOperation",
					Host:      "node1",
					Array:     "array1",
					Time:      "2022-01-01T00:00:00Z",
					Status:    "pending-reconciliation",
				},
			},
		},
	}
	scheme := SetupScheme()
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(volumeJournal).Build()

	reconciler := &VolumeJournalReconciler{
		Scheme: scheme,
		Client: client,
	}

	managerOptions := ctrl.Options{
		Scheme:    SetupScheme(),
		Cache:     cache.Options{},
		NewClient: nil,
	}
	manager, err := ctrl.NewManager(&rest.Config{}, managerOptions)
	if err != nil {
		t.Error(err)
	}
	err = reconciler.SetupWithManager(manager)
	if err != nil {
		t.Error(err)
	}
}

type testNodeServer struct {
	csi.UnimplementedNodeServer
}

func (f *testNodeServer) NodeStageVolume(_ context.Context, req *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	if req.VolumeId == "" {
		return nil, errors.New("Volume ID cannot be empty")
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (f *testNodeServer) NodeUnstageVolume(context.Context, *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (f *testNodeServer) NodePublishVolume(context.Context, *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	return &csi.NodePublishVolumeResponse{}, nil
}

func (f *testNodeServer) NodeUnpublishVolume(context.Context, *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (f *testNodeServer) NodeGetCapabilities(context.Context, *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{}, nil
}

func (f *testNodeServer) NodeGetInfo(context.Context, *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{}, nil
}

func (f *testNodeServer) NodeGetVolumeStats(context.Context, *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return &csi.NodeGetVolumeStatsResponse{}, nil
}

func (f *testNodeServer) NodeExpandVolume(context.Context, *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return &csi.NodeExpandVolumeResponse{}, nil
}

type TestControllerServer struct {
	csi.UnimplementedControllerServer
}

func (f *TestControllerServer) ControllerPublishVolume(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return &csi.ControllerPublishVolumeResponse{}, nil
}

func (f *TestControllerServer) ControllerUnpublishVolume(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (f *TestControllerServer) CreateVolume(context.Context, *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return &csi.CreateVolumeResponse{}, nil
}

func (f *TestControllerServer) DeleteVolume(context.Context, *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return &csi.DeleteVolumeResponse{}, nil
}

func (f *TestControllerServer) ValidateVolumeCapabilities(context.Context, *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return &csi.ValidateVolumeCapabilitiesResponse{}, nil
}

func (f *TestControllerServer) ListVolumes(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return &csi.ListVolumesResponse{}, nil
}

func (f *TestControllerServer) GetCapacity(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return &csi.GetCapacityResponse{}, nil
}

func (f *TestControllerServer) ControllerGetCapabilities(context.Context, *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{}, nil
}

func (f *TestControllerServer) CreateSnapshot(context.Context, *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return &csi.CreateSnapshotResponse{}, nil
}

func (f *TestControllerServer) DeleteSnapshot(context.Context, *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return &csi.DeleteSnapshotResponse{}, nil
}

func (f *TestControllerServer) ListSnapshots(context.Context, *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return &csi.ListSnapshotsResponse{}, nil
}

func (f *TestControllerServer) ControllerExpandVolume(context.Context, *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	return &csi.ControllerExpandVolumeResponse{}, nil
}

func (f *TestControllerServer) ControllerGetVolume(context.Context, *csi.ControllerGetVolumeRequest) (*csi.ControllerGetVolumeResponse, error) {
	return &csi.ControllerGetVolumeResponse{}, nil
}
