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
	"fmt"

	"github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

var (
	// for unit testing
	getConfigFunc = config.GetConfig
	newClientFunc = client.New
)

// Get returns a client that knows how to perform CRUD operations for a VolumeJournal CR.
func Get(_ context.Context) (client.Client, error) {
	config, err := getConfigFunc()
	if err != nil {
		return nil, fmt.Errorf("failed to get config for volume journal client: %v", err)
	}

	client, err := newClientFunc(config, client.Options{Scheme: controller.GetScheme()})
	if err != nil {
		return nil, fmt.Errorf("failed to create volume journal client: %v", err)
	}

	return client, nil
}
