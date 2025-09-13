package types

import (
	"encoding/json"
	"fmt"
	"net"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Provider",type="string",JSONPath=".spec.provider"
// +kubebuilder:printcolumn:name="Region",type="string",JSONPath=".spec.region"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Network represents a generic network resource that can be implemented
// across different cloud providers (AWS VPC, GCP VPC, Azure VNet)
type Network struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NetworkSpec   `json:"spec,omitempty"`
	Status NetworkStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NetworkList contains a list of Network
type NetworkList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Network `json:"items"`
}

// NetworkSpec defines the desired state of a Network
type NetworkSpec struct {
	// CIDR block for the network (e.g., "10.0.0.0/16")
	// +kubebuilder:validation:Pattern=`^([0-9]{1,3}\.){3}[0-9]{1,3}\/[0-9]{1,2}$`
	// +kubebuilder:validation:Required
	CIDR string `json:"cidr"`

	// Provider configuration
	// +kubebuilder:validation:Enum=aws;gcp;azure
	// +kubebuilder:validation:Required
	Provider string `json:"provider"`

	// Region where the network should be created
	// +kubebuilder:validation:Required
	Region string `json:"region"`

	// Subnets configuration
	// +optional
	Subnets []SubnetSpec `json:"subnets,omitempty"`

	// Tags to be applied to the network
	// +optional
	Tags map[string]string `json:"tags,omitempty"`
}

// SubnetSpec defines a subnet within the network
type SubnetSpec struct {
	// Name of the subnet
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// CIDR block for the subnet (must be within the network CIDR)
	// +kubebuilder:validation:Pattern=`^([0-9]{1,3}\.){3}[0-9]{1,3}\/[0-9]{1,2}$`
	// +kubebuilder:validation:Required
	CIDR string `json:"cidr"`

	// Availability zone or region for the subnet
	// +optional
	AvailabilityZone string `json:"availabilityZone,omitempty"`

	// Whether this subnet is public (has internet gateway access)
	// +optional
	Public bool `json:"public,omitempty"`
}

// NetworkStatus defines the observed state of a Network
type NetworkStatus struct {
	// Current phase of the network
	// +kubebuilder:validation:Enum=Pending;Creating;Ready;Failed;Deleting
	Phase NetworkPhase `json:"phase"`

	// Provider-specific network ID
	// +optional
	ProviderID string `json:"providerID,omitempty"`

	// List of created subnets with their provider IDs
	// +optional
	Subnets []SubnetStatus `json:"subnets,omitempty"`

	// Conditions represent the latest available observations
	// +optional
	Conditions []NetworkCondition `json:"conditions,omitempty"`

	// Last reconciliation time
	// +optional
	LastReconcileTime *metav1.Time `json:"lastReconcileTime,omitempty"`

	// Error message if any
	// +optional
	ErrorMessage string `json:"errorMessage,omitempty"`
}

// SubnetStatus represents the status of a subnet
type SubnetStatus struct {
	// Name of the subnet
	Name string `json:"name"`

	// Provider-specific subnet ID
	// +optional
	ProviderID string `json:"providerID,omitempty"`

	// Current state of the subnet
	// +optional
	State string `json:"state,omitempty"`

	// Availability zone where the subnet is located
	// +optional
	AvailabilityZone string `json:"availabilityZone,omitempty"`
}

// NetworkPhase represents the current phase of a network
// +kubebuilder:validation:Enum=Pending;Creating;Ready;Failed;Deleting
type NetworkPhase string

const (
	// NetworkPhasePending indicates the network is being created
	NetworkPhasePending NetworkPhase = "Pending"

	// NetworkPhaseCreating indicates the network is being created
	NetworkPhaseCreating NetworkPhase = "Creating"

	// NetworkPhaseReady indicates the network is ready
	NetworkPhaseReady NetworkPhase = "Ready"

	// NetworkPhaseFailed indicates the network creation failed
	NetworkPhaseFailed NetworkPhase = "Failed"

	// NetworkPhaseDeleting indicates the network is being deleted
	NetworkPhaseDeleting NetworkPhase = "Deleting"
)

// NetworkCondition describes the state of a network at a certain point
type NetworkCondition struct {
	// Type of network condition
	Type NetworkConditionType `json:"type"`

	// Status of the condition, one of True, False, Unknown
	Status metav1.ConditionStatus `json:"status"`

	// Last time the condition transitioned from one status to another
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`

	// The reason for the condition's last transition
	// +optional
	Reason string `json:"reason,omitempty"`

	// A human readable message indicating details about the transition
	// +optional
	Message string `json:"message,omitempty"`
}

// NetworkConditionType is a valid value for NetworkCondition.Type
// +kubebuilder:validation:Enum=Ready;SubnetsReady;DNSReady
type NetworkConditionType string

const (
	// NetworkConditionReady indicates the network is ready
	NetworkConditionReady NetworkConditionType = "Ready"

	// NetworkConditionSubnetsReady indicates all subnets are ready
	NetworkConditionSubnetsReady NetworkConditionType = "SubnetsReady"

	// NetworkConditionDNSReady indicates DNS is configured
	NetworkConditionDNSReady NetworkConditionType = "DNSReady"
)

// Validate validates the Network specification
func (n *Network) Validate() error {
	if n.Spec.CIDR == "" {
		return fmt.Errorf("network CIDR is required")
	}

	_, _, err := net.ParseCIDR(n.Spec.CIDR)
	if err != nil {
		return fmt.Errorf("invalid network CIDR: %v", err)
	}

	if n.Spec.Provider == "" {
		return fmt.Errorf("provider name is required")
	}

	for i, subnet := range n.Spec.Subnets {
		if err := subnet.Validate(n.Spec.CIDR); err != nil {
			return fmt.Errorf("subnet %d validation failed: %v", i, err)
		}
	}

	return nil
}

// Validate validates the SubnetSpec
func (s *SubnetSpec) Validate(networkCIDR string) error {
	if s.Name == "" {
		return fmt.Errorf("subnet name is required")
	}

	if s.CIDR == "" {
		return fmt.Errorf("subnet CIDR is required")
	}

	_, subnetNet, err := net.ParseCIDR(s.CIDR)
	if err != nil {
		return fmt.Errorf("invalid subnet CIDR: %v", err)
	}

	_, networkNet, err := net.ParseCIDR(networkCIDR)
	if err != nil {
		return fmt.Errorf("invalid network CIDR: %v", err)
	}

	if !networkNet.Contains(subnetNet.IP) {
		return fmt.Errorf("subnet CIDR %s is not within network CIDR %s", s.CIDR, networkCIDR)
	}

	return nil
}

// ToJSON converts the Network to JSON
func (n *Network) ToJSON() ([]byte, error) {
	return json.Marshal(n)
}

// FromJSON creates a Network from JSON
func (n *Network) FromJSON(data []byte) error {
	return json.Unmarshal(data, n)
}

// DeepCopyObject returns a generically typed copy of an object
func (n *Network) DeepCopyObject() runtime.Object {
	return n.DeepCopy()
}

// DeepCopy returns a deep copy of the Network
func (n *Network) DeepCopy() *Network {
	if n == nil {
		return nil
	}
	out := new(Network)
	n.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies all properties of this object into another object of the same type
func (n *Network) DeepCopyInto(out *Network) {
	*out = *n
	out.TypeMeta = n.TypeMeta
	n.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	n.Spec.DeepCopyInto(&out.Spec)
	n.Status.DeepCopyInto(&out.Status)
}

// DeepCopyInto copies all properties of this object into another object of the same type
func (n *NetworkSpec) DeepCopyInto(out *NetworkSpec) {
	*out = *n
	if n.Subnets != nil {
		in, out := &n.Subnets, &out.Subnets
		*out = make([]SubnetSpec, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if n.Tags != nil {
		in, out := &n.Tags, &out.Tags
		*out = make(map[string]string, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
}

// DeepCopyInto copies all properties of this object into another object of the same type
func (s *SubnetSpec) DeepCopyInto(out *SubnetSpec) {
	*out = *s
}

// DeepCopyInto copies all properties of this object into another object of the same type
func (n *NetworkStatus) DeepCopyInto(out *NetworkStatus) {
	*out = *n
	if n.Subnets != nil {
		in, out := &n.Subnets, &out.Subnets
		*out = make([]SubnetStatus, len(*in))
		copy(*out, *in)
	}
	if n.Conditions != nil {
		in, out := &n.Conditions, &out.Conditions
		*out = make([]NetworkCondition, len(*in))
		copy(*out, *in)
	}
	if n.LastReconcileTime != nil {
		in, out := &n.LastReconcileTime, &out.LastReconcileTime
		*out = (*in).DeepCopy()
	}
}
