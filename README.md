<!--
 Copyright © 2026 Dell Inc. or its subsidiaries. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Dell Container Storage Modules (CSM) for Disaster Recovery

[![GitHub Contributors](https://dell-shield-io.cec.delllabs.net/github/contributors/CSM/csm-dr.svg)](https://github.com/Ecosystems/container-storage-modules/src/csm-dr/graphs/contributors)
[![Go version](https://dell-shield-io.cec.delllabs.net/github/go-mod/go-version/CSM/csm-dr)](go.mod)
[![GitHub Release](https://dell-shield-io.cec.delllabs.net/github/v/release/CSM/csm-dr)](https://github.com/Ecosystems/container-storage-modules/src/csm-dr/releases/latest)
[![GitHub branch check runs](https://dell-shield-io.cec.delllabs.net/github/check-runs/CSM/csm-dr/main)](https://github.com/Ecosystems/container-storage-modules/src/csm-dr/actions/workflows/common-workflows.yaml?query=branch%3Amain)

Dell CSM for Disaster Recovery is part of the [CSM (Container Storage Modules)](https://github.com/Ecosystems/container-storage-modules/src/csm) open-source suite of Kubernetes storage enablers for Dell products.

CSM DR is a Go library that provides disaster recovery capabilities for Dell CSI drivers by journaling and replaying CSI volume operations during storage array site failures. It is designed to be embedded directly within a CSI driver process (not deployed as a standalone service) and uses a Kubernetes Custom Resource Definition (CRD) called `VolumeJournal` to track and reconcile volume operations that must be replayed when a metro replication session becomes fractured.

This project has been created using [kubebuilder](https://github.com/kubernetes-sigs/kubebuilder), and the Kubernetes controller has been implemented using [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime). Additionally, this project uses [kustomize](https://github.com/kubernetes-sigs/kustomize) for generating various `yaml` manifests for Kubernetes objects.

For documentation, please visit [Container Storage Modules documentation](https://dell.github.io/csm-docs/).

## Table of Contents

* [Code of Conduct](https://github.com/Ecosystems/container-storage-modules/src/csm/blob/main/docs/CODE_OF_CONDUCT.md)
* [Maintainer Guide](https://github.com/Ecosystems/container-storage-modules/src/csm/blob/main/docs/MAINTAINER_GUIDE.md)
* [Committer Guide](https://github.com/Ecosystems/container-storage-modules/src/csm/blob/main/docs/COMMITTER_GUIDE.md)
* [Contributing Guide](https://github.com/Ecosystems/container-storage-modules/src/csm/blob/main/docs/CONTRIBUTING.md)
* [List of Adopters](https://github.com/Ecosystems/container-storage-modules/src/csm/blob/main/docs/ADOPTERS.md)
* [Dell support](https://www.dell.com/support/incidents-online/en-us/contactus/product/container-storage-modules)
* [Security](https://github.com/Ecosystems/container-storage-modules/src/csm/blob/main/docs/SECURITY.md)

## Overview

When a metro replication session fractures (i.e., a site failure occurs), CSI volume operations such as `ControllerPublishVolume`, `NodeStageVolume`, and `NodePublishVolume` may need to be replayed against a failover storage array. CSM DR handles this by:

1. **Journaling** -- The CSI driver creates `VolumeJournal` custom resources containing serialized CSI requests that need to be deferred and replayed.
2. **Reconciling** -- The embedded controller watches `VolumeJournal` resources, waits until both the original and failover arrays are online, then replays the journaled CSI operations.
3. **Cleanup** -- Once all journal entries are reconciled, the `VolumeJournal` resource is automatically deleted.

### Custom Resource Definition

The library defines a single cluster-scoped CRD:

- **VolumeJournal** (`dr.storage.dell.com/v1`) -- Represents a set of deferred CSI volume operations to be replayed during disaster recovery. Each journal tracks the volume handle, source/target clusters, original/failover arrays, and a list of journal entries with their reconciliation status.

### Supported CSI Operations

The following CSI operations can be journaled and replayed:

| Operation | Mode |
| --------- | ---- |
| `ControllerPublishVolume` | Controller |
| `ControllerUnpublishVolume` | Controller |
| `NodeStageVolume` | Node |
| `NodeUnstageVolume` | Node |
| `NodePublishVolume` | Node |
| `NodeUnpublishVolume` | Node |

### Components

CSM DR exposes three public packages for consumption by CSI drivers:

| Package | Description |
| ------- | ----------- |
| `pkg/controller` | Provides the `Initialize` function to start the embedded controller manager and VolumeJournal reconciler within a CSI driver process. |
| `pkg/client` | Provides a `Get` function that returns a `controller-runtime` client configured with the VolumeJournal scheme for CRUD operations on VolumeJournal resources. |
| `pkg/storage` | Defines the `HealthChecker` and `ArrayList` interfaces that CSI drivers must implement to enable array health monitoring during reconciliation. |

## Usage

CSM DR is consumed as a library dependency by Dell CSI drivers. The following example is based on how [csi-powerstore](https://github.com/Ecosystems/container-storage-modules/src/csi-powerstore) integrates the library.

### 1. Implement the Storage Interfaces

The CSI driver must implement two interfaces from `pkg/storage`:

```go
import "github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/storage"

// HealthChecker -- implement on your array type
// IsOnline returns true if the array is reachable.
type HealthChecker interface {
    IsOnline(ctx context.Context) bool
}

// ArrayList -- implement on your array manager
// Get returns a HealthChecker for the given array ID.
type ArrayList interface {
    Get(arrayID string) (HealthChecker, error)
}
```

For example, in csi-powerstore the `Locker` type implements `ArrayList` and `PowerStoreArray` implements `HealthChecker`:

```go
// Satisfies the ArrayList interface for csm-dr
func (s *Locker) Get(globalID string) (storage.HealthChecker, error) {
    return s.GetOneArray(globalID)
}

// Satisfies the HealthChecker interface for csm-dr
func (psa *PowerStoreArray) IsOnline(ctx context.Context) bool {
    _, err := psa.GetClient().GetCluster(ctx)
    return err == nil
}
```

### 2. Initialize the Controller

Call `pkg/controller.Initialize` during CSI driver startup to start the embedded VolumeJournal reconciler. The function accepts the CSI node and controller service implementations, the array list, the driver mode, and configuration options:

```go
import drController "github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/controller"

// In your CSI driver's main() function:
_, err := drController.Initialize(
    nodeService,          // csi.NodeServer (nil in controller mode)
    controllerService,    // csi.ControllerServer (nil in node mode)
    arrayLocker,          // storage.ArrayList implementation
    mode,                 // "node" or "controller"
    nodeName,             // Kubernetes node name (node mode only)
    probeAddr,            // Health probe bind address (e.g., ":8082")
    enableLeaderElection, // Enable leader election for HA
    metricsAddr...,       // Optional metrics bind address
)
```

The controller starts in a background goroutine and returns a `controller-runtime` client that can be used for additional Kubernetes API operations.

### 3. Create VolumeJournal Entries

When a metro replication session fractures and CSI operations need to be deferred, use the `pkg/client` package to create or update `VolumeJournal` resources:

```go
import (
    drv1 "github.com/Ecosystems/container-storage-modules/src/csm-dr/api/v1"
    drClient "github.com/Ecosystems/container-storage-modules/src/csm-dr/pkg/client"
)

// Get a client configured with the VolumeJournal scheme
client, err := drClient.Get(ctx)

// Create a VolumeJournal with deferred operations
journal := drv1.VolumeJournal{
    ObjectMeta: metav1.ObjectMeta{
        Name: "journal-" + volumeName,
    },
    Spec: drv1.VolumeJournalSpec{
        VolumeUUID:    volumeUUID,
        VolumeHandle:  volumeHandle,
        SourceCluster: sourceCluster,
        TargetCluster: targetCluster,
        OriginalArray: originalArrayID,
        FailoverArray: failoverArrayID,
        JournalEntries: []drv1.JournalEntry{
            {
                Operation: "ControllerPublishVolume",
                Status:    "pending-reconciliation",
                Time:      time.Now().Format(time.RFC3339),
                Host:      nodeName,
                Array:     failoverArrayID,
                Request:   serializedCSIRequest, // protobuf-marshaled CSI request
            },
        },
    },
}

err = client.Create(ctx, &journal)
```

## Installation and Configuration

CSM DR is embedded within the CSI driver and does not require a separate deployment. However, the `VolumeJournal` CRD must be installed in the Kubernetes cluster, and the driver must be configured with the appropriate environment variables. Both the [CSM Operator](https://github.com/Ecosystems/container-storage-modules/src/csm-operator) and [Helm charts](https://github.com/dell/helm-charts) handle this automatically.

### Configuration Parameters

| Parameter | Environment Variable | Default | Description |
| --------- | -------------------- | ------- | ----------- |
| DR Enabled | `X_CSM_DR_ENABLED` | `true` | Enable or disable CSM Disaster Recovery. Set to `false` to disable the VolumeJournal reconciler. |
| Bind Port | `X_CSM_DR_BIND_PORT` | `8082` | Health probe bind port for the embedded DR controller manager. Must be a valid port number (1-65535). |

### Installing with CSM Operator

The [CSM Operator](https://github.com/Ecosystems/container-storage-modules/src/csm-operator) automatically installs the `VolumeJournal` CRD and configures the DR environment variables when deploying the CSI PowerStore driver. CSM DR requires driver version `v2.16.0` or later.

In the `ContainerStorageModule` custom resource, configure DR under `spec.driver.common.envs`:

```yaml
apiVersion: storage.dell.com/v1
kind: ContainerStorageModule
metadata:
  name: powerstore
  namespace: powerstore
spec:
  driver:
    csiDriverType: "powerstore"
    common:
      envs:
        # X_CSM_DR_ENABLED: Enable/Disable CSM Disaster Recovery.
        # Allowed values:
        #   true: enable CSM Disaster Recovery
        #   false: disable CSM Disaster Recovery
        # Default value: true
        - name: "X_CSM_DR_ENABLED"
          value: "true"
        # X_CSM_DR_BIND_PORT: Bind port for CSM-DR controller initialization.
        # Allowed values: valid port number (e.g., "8082")
        # Default value: "8082"
        - name: "X_CSM_DR_BIND_PORT"
          value: "8082"
```

When `X_CSM_DR_ENABLED` is set to `true` and the driver type is PowerStore, the operator will:
1. Install the `VolumeJournal` CRD (`volumejournals.dr.storage.dell.com`) in the cluster.
2. Configure the appropriate RBAC permissions for the controller and node pods to manage `VolumeJournal` resources.
3. Set the `X_CSM_DR_ENABLED` and `X_CSM_DR_BIND_PORT` environment variables on both the controller and node containers.

When the driver is removed, the operator will also clean up the `VolumeJournal` CRD.

For complete sample CR files, see the [CSM Operator samples](https://github.com/Ecosystems/container-storage-modules/src/csm-operator/tree/main/samples).

### Installing with Helm

The [csi-powerstore Helm chart](https://github.com/dell/helm-charts/tree/main/charts/csi-powerstore) includes CSM DR support through a dependency on the `csm-disaster-recovery` sub-chart, which installs the `VolumeJournal` CRD.

In your `values.yaml`, configure the `disasterRecovery` section:

```yaml
# Enable installation of Custom Resource Definitions (CRDs) required for Disaster Recovery.
# This is necessary when using the PowerStore Metro feature.
disasterRecovery:
  # enabled: Install the Disaster Recovery CRDs and enable DR in the driver.
  # Allowed values:
  #   true: Installs the Disaster Recovery CRDs and sets X_CSM_DR_ENABLED=true.
  #   false: Does not install the Disaster Recovery CRDs and sets X_CSM_DR_ENABLED=false.
  # Default value: true
  enabled: true
  # bindPort: Defines the port for the Disaster Recovery controller health probe.
  # Allowed values: Any valid and free port
  # Default value: 8082
  bindPort: 8082
```

When `disasterRecovery.enabled` is set to `true`, the Helm chart will:
1. Install the `csm-disaster-recovery` sub-chart, which creates the `VolumeJournal` CRD.
2. Set `X_CSM_DR_ENABLED=true` and `X_CSM_DR_BIND_PORT` environment variables on both the controller Deployment and node DaemonSet.
3. Configure RBAC rules granting the controller and node `ServiceAccounts` permissions to manage `VolumeJournal` resources.

Install or upgrade with Helm:

```bash
helm repo add dell https://dell.github.io/helm-charts
helm repo update
helm install powerstore dell/csi-powerstore -n powerstore --create-namespace -f values.yaml
```

To disable DR during installation, set `disasterRecovery.enabled` to `false` in your `values.yaml` or pass it as an override:

```bash
helm install powerstore dell/csi-powerstore -n powerstore --create-namespace \
  --set disasterRecovery.enabled=false
```

## Build

### Dependencies

This project relies on the following tools which have to be installed in order to generate certain manifests.

| Tool | Version |
| ---- | ------- |
| controller-gen | v0.19.0 |
| kustomize | v5.7.1 |

### Custom Resource Definitions

Run the command `make manifests` to build the VolumeJournal Custom Resource Definition (CRD). This command invokes `controller-gen` to generate the CRD. The API code is annotated with `kubebuilder` tags which are used by the `controller-gen` generators. The generated definitions are present in the form of a _kustomize_ recipe in the `config/crd` folder.

### Binaries

To build the manager binary, run `make build`.

### CRD Installation

You can run the command `make install` to install the Custom Resource Definitions in your Kubernetes cluster.

To remove the CRDs, run `make uninstall`.

## Testing

To run all tests with coverage:

```
make test
```

This generates a `cover.out` file with coverage data. Tests use `testify` for assertions and `go.uber.org/mock` for mock generation.
