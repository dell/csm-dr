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

package client

import (
	"context"
	"errors"
	"testing"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestGet(t *testing.T) {
	defaultGetConfigFunc := getConfigFunc
	defaultNewClientFunc := newClientFunc

	tests := []struct {
		name    string // description of this test case
		init    func(tt *testing.T)
		want    client.Client
		wantErr bool
	}{
		{
			name: "fail to get config",
			init: func(tt *testing.T) {
				getConfigFunc = func() (*rest.Config, error) {
					return nil, errors.New("fail to get config")
				}
				tt.Cleanup(func() { getConfigFunc = defaultGetConfigFunc })
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "fail to create new client",
			init: func(tt *testing.T) {
				newClientFunc = func(_ *rest.Config, _ client.Options) (client.Client, error) {
					return nil, errors.New("fail to create new client")
				}
				tt.Cleanup(func() { newClientFunc = defaultNewClientFunc })
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "success",
			init: func(tt *testing.T) {
				getConfigFunc = func() (*rest.Config, error) {
					return &rest.Config{}, nil
				}
				newClientFunc = func(_ *rest.Config, _ client.Options) (client.Client, error) {
					return fake.NewClientBuilder().Build(), nil
				}
				tt.Cleanup(func() {
					getConfigFunc = defaultGetConfigFunc
					newClientFunc = defaultNewClientFunc
				})
			},
			want:    fake.NewClientBuilder().Build(),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.init(t)

			got, gotErr := Get(context.Background())
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("Get() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("Get() succeeded unexpectedly")
			}

			// just check if we expected a client and if we got one
			if (tt.want != nil) != (got != nil) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}
