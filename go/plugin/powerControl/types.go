package main

import "time"

type NodeState string

const (
	Active      NodeState = "active"
	Idle        NodeState = "idle"
	Sleep       NodeState = "sleep"
	PoweredOff  NodeState = "powered_off"
	ToSleeping  NodeState = "to_sleeping"
	Wakingup    NodeState = "waking_up"
	PoweringOn  NodeState = "powering_on"
	PoweringOff NodeState = "powering_off"
)

type NodeInfo struct {
	Exclude             bool
	State               NodeState
	LastStateChangeTime time.Time
	Jobs                map[string]struct{}
}

type PredictionResponse struct {
	Prediction int    `json:"prediction"`
	Error      string `json:"error,omitempty"`
}

type NetworkInterface struct {
	MAC string
	IP  string
}

type PowerTool interface {
	RegisterNode(nodeID string, interfaces []NetworkInterface) error
	GetPowerState(nodeID string) (bool, error)
	CheckNodeAlive(nodeID string) bool

	WakeUp(nodeID string) error
	Sleep(nodeID string) error
	PowerOn(nodeID string) error
	PowerOff(nodeID string) error
}
