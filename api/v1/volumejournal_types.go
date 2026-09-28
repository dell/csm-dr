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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// VolumeJournalSpec defines the desired state of VolumeJournal
type VolumeJournalSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "make" to regenerate code after modifying this file
	// The following markers will use OpenAPI v3 schema to validate the value
	// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

	// VolumeHandle is the CSI volume handle used to identify the metro volume during a site failure
	VolumeHandle string `json:"volumeHandle"`

	// VolumeUUID is the unique identifier of the volume
	VolumeUUID string `json:"volumeUUID"`

	// SourceCluster is the name of the cluster with nodes where the volume is originally provisioned
	SourceCluster string `json:"sourceCluster"`

	// TargetCluster is the name of the cluster with nodes where the volume will be rescheduled during a site failure
	TargetCluster string `json:"targetCluster"`

	// OriginalArray is the preferred storage array when the volume is originally provisioned
	OriginalArray string `json:"originalArray"`

	// FailoverArray is the preferred storage array during a site failure
	FailoverArray string `json:"failoverArray"`

	// JournalEntries is the list of volume operations performed during a site failure
	JournalEntries []JournalEntry `json:"journalEntries"`
}

type JournalEntry struct {
	// Operation is the type of volume operation performed during a site failure
	// Example: "ControllerPublish", "NodeStage", "NodePublish"
	Operation string `json:"operation"`

	// Host is the name of the node where the volume gets provisioned
	Host string `json:"host"`

	// Array is the storage array where the volume operation is performed during a site failure
	Array string `json:"array"`

	// Time is the time at which the volume operation was performed during a site failure
	Time string `json:"time"`

	// Status is the status of the volume operation during a site failure
	// Example: "pending-reconcilation", "reconciled"
	Status string `json:"status"`

	// Serialized request from each of the Operations.
	Request []byte `json:"request"`
}

// VolumeJournalStatus defines the observed state of VolumeJournal.
type VolumeJournalStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster

// VolumeJournal is the Schema for the volumejournals API
type VolumeJournal struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	Spec   VolumeJournalSpec   `json:"spec"`
	Status VolumeJournalStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// VolumeJournalList contains a list of VolumeJournal
type VolumeJournalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VolumeJournal `json:"items"`
}

func init() {
	SchemeBuilder.Register(&VolumeJournal{}, &VolumeJournalList{})
}
