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

package storage

import (
	"context"
	"time"

	"github.com/Ecosystems/container-storage-modules/src/csmlog"
)

//go:generate mockgen -destination=./mocks/array.go . HealthChecker,ArrayList

// HealthChecker provides methods for checking on the status of a storage array.
type HealthChecker interface {
	// IsOnline	returns true if the array, specified by arrayID, is online
	// and responsive; false, otherwise.
	IsOnline(ctx context.Context) bool
}

// ArrayList provides an interface for managing arrays in the context
// of a VolumeJournal.
type ArrayList interface {
	// Get uses the given arrayID to return an array that implements
	// the HealthChecker interface. The arrayID used is pulled from
	// the VolumeJournal JournalEntry Array.
	//
	// Primarily used to get an array and check if it is online.
	Get(arrayID string) (HealthChecker, error)
}

// ArrayPinger is used to track the online status of a set of storage arrays.
type ArrayPinger struct {
	// ArrayList is a list of storage arrays the driver is utilizing
	// that will be pinged by the reconciliation process of the VolumeJournal
	// if the array is mentioned in the VolumeJournal being reconciled.
	ArrayList

	// LastSuccess maps an arrayID to the time of it's last
	// susccessful ping
	LastSuccess map[string]time.Time

	// cacheExpiry sets the duration for which a cached, successful ping is valid.
	cacheExpiry time.Duration
}

// NewArrayPinger creates a new ArrayPinger with the given ArrayList.
func NewArrayPinger(arrays ArrayList, cacheExpiry time.Duration) *ArrayPinger {
	return &ArrayPinger{
		ArrayList:   arrays,
		LastSuccess: map[string]time.Time{},
		cacheExpiry: cacheExpiry,
	}
}

// IsArrayOnline checks if the array is online at least once per minute.
// If the array is not online, it checks it as frequently as requested.
func (arrays *ArrayPinger) IsArrayOnline(ctx context.Context, arrayID string) bool {
	// cache the successful response for some time
	// helps avoid spamming REST requests
	lastSuccess, ok := arrays.LastSuccess[arrayID]
	if ok && time.Since(lastSuccess) < arrays.cacheExpiry {
		csmlog.Debugf("returning cached success for array %q", arrayID)
		return true
	}

	array, err := arrays.Get(arrayID)
	if err != nil {
		csmlog.Warnf("failed to get array %q when checking its online status: %v", arrayID, err)
		return false
	}

	if array.IsOnline(ctx) {
		csmlog.Debugf("array %q is online", arrayID)
		arrays.LastSuccess[arrayID] = time.Now()

		return true
	}

	return false
}
