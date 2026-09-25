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

package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage"
	mock_storage "github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage/mocks"
	"go.uber.org/mock/gomock"
)

func TestStatus_IsArrayOnline(t *testing.T) {
	tests := []struct {
		name    string
		arrayID string
		arrays  func(tt *testing.T) *storage.ArrayPinger
		want    bool
	}{
		{
			name:    "return cached response",
			arrayID: "array-1",
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				pinger := storage.NewArrayPinger(mock_storage.NewMockArrayList(gomock.NewController(tt)), 1*time.Minute)
				pinger.LastSuccess["array-1"] = time.Now()
				return pinger
			},

			want: true,
		},
		{
			name:    "fail to get array",
			arrayID: "array-1",
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array-1").Return(nil, errors.New("foo error"))

				return &storage.ArrayPinger{
					ArrayList: arrays,
					LastSuccess: map[string]time.Time{
						// make sure the time is older than the expiry time
						"array-1": time.Now().Add(-1 * time.Minute),
					},
				}
			},
			want: false,
		},
		{
			name:    "array is online",
			arrayID: "array-1",
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				array := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				array.EXPECT().IsOnline(gomock.Any()).Return(true)

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array-1").Return(array, nil)

				return &storage.ArrayPinger{
					ArrayList: arrays,
					LastSuccess: map[string]time.Time{
						// make sure the time is older than the expiry time
						"array-1": time.Now().Add(-1 * time.Minute),
					},
				}
			},
			want: true,
		},
		{
			name:    "array is offline",
			arrayID: "array-1",
			arrays: func(tt *testing.T) *storage.ArrayPinger {
				array := mock_storage.NewMockHealthChecker(gomock.NewController(tt))
				array.EXPECT().IsOnline(gomock.Any()).Return(false)

				arrays := mock_storage.NewMockArrayList(gomock.NewController(tt))
				arrays.EXPECT().Get("array-1").Return(array, nil)

				return &storage.ArrayPinger{
					ArrayList: arrays,
					LastSuccess: map[string]time.Time{
						// make sure the time is older than the expiry time
						"array-1": time.Now().Add(-1 * time.Minute),
					},
				}
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			arrays := tt.arrays(t)

			got := arrays.IsArrayOnline(context.Background(), tt.arrayID)

			if got != tt.want {
				t.Errorf("IsArrayOnline() = %v, want %v", got, tt.want)
			}
		})
	}
}
