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
	"errors"
	"net"
	"testing"

	"github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage"
	mock_storage "github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage/mocks"
	"github.com/go-logr/logr"
	"go.uber.org/mock/gomock"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

func getRandomPort() string {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	return listener.Addr().String()
}

type mockManager struct {
	ctrl.Manager
	addHealthzCheckErr error
	addReadyzCheckErr  error
	client             client.Client
	scheme             *runtime.Scheme
}

func (m *mockManager) AddHealthzCheck(_ string, _ healthz.Checker) error {
	return m.addHealthzCheckErr
}

func (m *mockManager) AddReadyzCheck(_ string, _ healthz.Checker) error {
	return m.addReadyzCheckErr
}

func (m *mockManager) GetClient() client.Client {
	return m.client
}

func (m *mockManager) GetScheme() *runtime.Scheme {
	if m.scheme != nil {
		return m.scheme
	}
	return scheme
}

func (m *mockManager) Start(_ context.Context) error {
	return nil
}

func TestCreateManager(t *testing.T) {
	tests := []struct {
		name                 string
		probeAddr            string
		enableLeaderElection bool
		wantErr              bool
	}{
		{
			name:                 "Success Creating Manager",
			probeAddr:            ":8080",
			enableLeaderElection: false,
			wantErr:              false,
		},
		{
			name:                 "Error Creating Manager",
			probeAddr:            ":8080",
			enableLeaderElection: false,
			wantErr:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldGetConfigOrDie := ctrl.GetConfigOrDie
			defer func() {
				ctrl.GetConfigOrDie = oldGetConfigOrDie
			}()
			if tt.wantErr {
				ctrl.GetConfigOrDie = func() *rest.Config {
					return nil
				}
			} else {
				ctrl.GetConfigOrDie = func() *rest.Config {
					return &rest.Config{}
				}
			}

			_, err := createManagerInstance(tt.probeAddr, tt.enableLeaderElection, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateManager() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAddHealthChecks(t *testing.T) {
	tests := []struct {
		name    string
		mgr     ctrl.Manager
		wantErr bool
	}{
		{
			name: "Success",
			mgr: &mockManager{
				addHealthzCheckErr: nil,
				addReadyzCheckErr:  nil,
			},
			wantErr: false,
		}, {
			name: "Healthz Check Error",
			mgr: &mockManager{
				addHealthzCheckErr: errors.New("Healthz Check Error"),
				addReadyzCheckErr:  nil,
			},
			wantErr: true,
		}, {
			name: "Readyz Check Error",
			mgr: &mockManager{
				addHealthzCheckErr: nil,
				addReadyzCheckErr:  errors.New("Readyz Check Error"),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := addHealthChecks(tt.mgr)
			if (err != nil) != tt.wantErr {
				t.Errorf("addHealthChecks() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSetupController(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(ctrl.Manager) error
		wantErr   error
	}{
		{
			name: "Success",
			setupFunc: func(_ ctrl.Manager) error {
				return nil
			},
			wantErr: nil,
		},
		{
			name: "setupFunc returns error",
			setupFunc: func(_ ctrl.Manager) error {
				return errors.New("controller setup failed")
			},
			wantErr: errors.New("controller setup failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := setupControllerManager(tt.setupFunc, nil)
			if err != nil {
				if err.Error() != tt.wantErr.Error() {
					t.Errorf("Reconcile() error = %v, wantErr %v", err, tt.wantErr)
				}
			} else {
				if err != tt.wantErr {
					t.Errorf("Reconcile() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}

func Test_Initialize(t *testing.T) {
	defaultCreateManagerFunc := createManagerFunc
	defaultSetupControllerFunc := SetupControllerFunc
	defaultAddHealthChecksFunc := AddHealthChecksFunc
	type args struct {
		arrays         storage.ArrayList
		probeAddr      string
		leaderElection bool
		metricsAddr    []string
	}
	tests := []struct {
		name    string
		init    func(tt *testing.T)
		args    args
		want    client.Client
		wantErr bool
		mode    string
	}{
		{
			name: "fail to create manager",
			init: func(tt *testing.T) {
				createManagerFunc = func(_ string, _ bool, _ string) (ctrl.Manager, error) {
					return nil, errors.New("mock failure")
				}
				tt.Cleanup(func() {
					createManagerFunc = defaultCreateManagerFunc
				})
			},
			args: args{
				arrays:         mock_storage.NewMockArrayList(gomock.NewController(t)),
				probeAddr:      getRandomPort(),
				leaderElection: false,
				metricsAddr:    []string{getRandomPort()},
			},
			mode:    "node",
			want:    nil,
			wantErr: true,
		},
		{
			name: "setup controller fails",
			init: func(tt *testing.T) {
				createManagerFunc = func(_ string, _ bool, _ string) (ctrl.Manager, error) {
					return &mockManager{
						client: fake.NewClientBuilder().WithScheme(scheme).Build(),
					}, nil
				}
				SetupControllerFunc = func(_ func(ctrl.Manager) error, _ ctrl.Manager) error {
					return errors.New("mock failure")
				}
				tt.Cleanup(func() {
					createManagerFunc = defaultCreateManagerFunc
					SetupControllerFunc = defaultSetupControllerFunc
				})
			},
			args: args{
				arrays:         mock_storage.NewMockArrayList(gomock.NewController(t)),
				probeAddr:      getRandomPort(),
				leaderElection: false,
				metricsAddr:    []string{getRandomPort()},
			},
			mode:    "node",
			want:    nil,
			wantErr: true,
		},
		{
			name: "Success: Initialization using NodeService",
			init: func(tt *testing.T) {
				createManagerFunc = func(_ string, _ bool, _ string) (ctrl.Manager, error) {
					return &mockManager{
						client: fake.NewClientBuilder().WithScheme(scheme).Build(),
					}, nil
				}
				SetupControllerFunc = func(_ func(ctrl.Manager) error, _ ctrl.Manager) error {
					return nil
				}
				AddHealthChecksFunc = func(_ ctrl.Manager) error {
					return nil
				}
				tt.Cleanup(func() {
					createManagerFunc = defaultCreateManagerFunc
					SetupControllerFunc = defaultSetupControllerFunc
					AddHealthChecksFunc = defaultAddHealthChecksFunc
				})
			},
			args: args{
				arrays:         mock_storage.NewMockArrayList(gomock.NewController(t)),
				probeAddr:      getRandomPort(),
				metricsAddr:    []string{getRandomPort()},
				leaderElection: false,
			},
			mode:    "node",
			want:    fake.NewClientBuilder().Build(),
			wantErr: false,
		},
		{
			name: "Success: Initialization using ControllerService",
			init: func(tt *testing.T) {
				createManagerFunc = func(_ string, _ bool, _ string) (ctrl.Manager, error) {
					return &mockManager{
						client: fake.NewClientBuilder().WithScheme(scheme).Build(),
					}, nil
				}
				SetupControllerFunc = func(_ func(ctrl.Manager) error, _ ctrl.Manager) error {
					return nil
				}
				AddHealthChecksFunc = func(_ ctrl.Manager) error {
					return nil
				}
				tt.Cleanup(func() {
					createManagerFunc = defaultCreateManagerFunc
					SetupControllerFunc = defaultSetupControllerFunc
					AddHealthChecksFunc = defaultAddHealthChecksFunc
				})
			},
			args: args{
				probeAddr:      getRandomPort(),
				metricsAddr:    []string{getRandomPort()},
				leaderElection: false,
			},
			mode:    "controller",
			want:    fake.NewClientBuilder().Build(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.init(t)

			got, err := Initialize(nil, nil, tt.args.arrays, tt.mode, "node1", tt.args.probeAddr, tt.args.leaderElection, tt.args.metricsAddr...)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Initialize() failed: %v", err)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Initialize() succeeded unexpectedly")
			}

			if (tt.want != nil) != (got != nil) {
				t.Errorf("Initialize() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCsmLogSink_Init(_ *testing.T) {
	sink := &csmLogSink{}
	sink.Init(logr.RuntimeInfo{})
}

func TestCsmLogSink_Enabled(t *testing.T) {
	sink := &csmLogSink{}
	if !sink.Enabled(0) {
		t.Error("Enabled() should return true for level 0")
	}
	if !sink.Enabled(1) {
		t.Error("Enabled() should return true for level 1")
	}
}

func TestCsmLogSink_Info(_ *testing.T) {
	sink := &csmLogSink{}
	sink.Info(0, "info level message", "key", "value")
	sink.Info(1, "debug level message", "key", "value")
}

func TestCsmLogSink_Error(_ *testing.T) {
	sink := &csmLogSink{}
	sink.Error(errors.New("test error"), "error message", "key", "value")
	sink.Error(nil, "error message without error")
}

func TestCsmLogSink_WithValues(t *testing.T) {
	sink := &csmLogSink{}
	newSink := sink.WithValues("key1", "val1", "key2", "val2")
	casted, ok := newSink.(*csmLogSink)
	if !ok {
		t.Fatal("WithValues() should return a *csmLogSink")
	}
	if len(casted.values) != 4 {
		t.Errorf("WithValues() values length = %d, want 4", len(casted.values))
	}

	chainedSink := newSink.(*csmLogSink).WithValues("key3", "val3")
	casted2 := chainedSink.(*csmLogSink)
	if len(casted2.values) != 6 {
		t.Errorf("Chained WithValues() values length = %d, want 6", len(casted2.values))
	}
}

func TestCsmLogSink_WithName(t *testing.T) {
	sink := &csmLogSink{}

	named := sink.WithName("controller").(*csmLogSink)
	if named.name != "controller" {
		t.Errorf("WithName() name = %q, want %q", named.name, "controller")
	}

	nested := named.WithName("reconciler").(*csmLogSink)
	if nested.name != "controller.reconciler" {
		t.Errorf("Nested WithName() name = %q, want %q", nested.name, "controller.reconciler")
	}
}

func TestCsmLogSink_LoggerWithContext(t *testing.T) {
	sink := &csmLogSink{name: "test"}
	l := sink.fields("stringKey", "value", 42, "non-string-key")
	if l == nil {
		t.Fatal("fields() returned nil")
	}

	sinkNoName := &csmLogSink{}
	l2 := sinkNoName.fields()
	if l2 == nil {
		t.Fatal("fields() with no name returned nil")
	}
}

func TestGetScheme(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		want *runtime.Scheme
	}{
		{
			name: "success",
			want: scheme,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetScheme()

			if got != tt.want {
				t.Errorf("GetScheme() = %v, want %v", got, tt.want)
			}
		})
	}
}
