package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
)

const (
	maxRetries    = 3
	retryInterval = 5 * time.Second
)

type PowerManager struct {
	config    *Config
	powerTool PowerTool

	nodesInfo      sync.Map
	nodesInfoMutex sync.Mutex

	ctldClient protos.CraneCtldClient

	stopChan chan struct{}
}

type PredictionRequest struct {
	TotalNodes int `json:"total_nodes"`
}

func NewPowerManager(config *Config) *PowerManager {
	manager := &PowerManager{
		config:    config,
		powerTool: NewIPMITool(config),
		stopChan:  make(chan struct{}),
	}

	manager.initCtldClient()

	manager.startPowerStateMonitor()

	// Log NodeStateChangeFile configuration status
	if config.PowerControl.NodeStateChangeFile == "" {
		log.Info("NodeStateChangeFile is not configured - node state change recording is disabled")
	} else {
		log.Infof("NodeStateChangeFile is configured - node state changes will be recorded to: %s", config.PowerControl.NodeStateChangeFile)
	}

	return manager
}

func (c *PowerManager) RegisterNode(nodeID string) {
	if _, exists := c.nodesInfo.Load(nodeID); exists {
		log.Debugf("Node %s already registered, skip registration", nodeID)
		return
	}

	isExcluded := false
	for _, excludeNode := range c.config.IPMI.ExcludeNodes {
		if excludeNode == nodeID {
			isExcluded = true
			break
		}
	}

	c.nodesInfo.Store(nodeID, &NodeInfo{
		Exclude:             isExcluded,
		State:               Idle,
		LastStateChangeTime: time.Now(),
		Jobs:                make(map[string]struct{}),
	})
	c.recordStateChange(time.Now(), nodeID, "", Idle)
	c.notifyCtldPowerStateChange(nodeID, Idle)

	if isExcluded {
		log.Infof("Initialized node %s in idle state (excluded from power management)", nodeID)
	} else {
		log.Infof("Initialized node %s in idle state", nodeID)
	}
}

func (c *PowerManager) SetNodeExclude(nodeID string, exclude bool) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	info.Exclude = exclude
	c.nodesInfo.Store(nodeID, info)

	if exclude {
		log.Infof("Node %s is now excluded from power management", nodeID)
	} else {
		log.Infof("Node %s is now included in power management", nodeID)
	}

	return nil
}

func (c *PowerManager) StartAutoPowerManager() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.config.PowerControl.CheckIntervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				wake, on, sleep, off := c.start()
				log.Infof("Auto power control executed: wake=%d, on=%d, sleep=%d, off=%d",
					wake, on, sleep, off)
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *PowerManager) StopAutoPowerManager() {
	close(c.stopChan)
}

func (c *PowerManager) AddJobToNode(nodeID string, jobID string) bool {
	return c.updateNodeJob(nodeID, jobID, true)
}

func (c *PowerManager) RemoveJobFromNode(nodeID string, jobID string) bool {
	return c.updateNodeJob(nodeID, jobID, false)
}

func (c *PowerManager) GetNodesByState(states ...NodeState) []string {
	var nodes []string
	c.nodesInfo.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		info := value.(*NodeInfo)
		for _, state := range states {
			if info.State == state {
				nodes = append(nodes, nodeID)
				break
			}
		}
		return true
	})
	return nodes
}

func (c *PowerManager) initCtldClient() {
	configPath := util.DefaultConfigPath
	config := util.ParseConfig(configPath)
	c.ctldClient = util.GetStubToCtldByConfig(config)
}

func (c *PowerManager) start() (int, int, int, int) {
	nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff := c.makeDecision()

	if !c.config.PowerControl.EnableSleep {
		if len(nodesToWake) > 0 {
			log.Warnf("EnableSleep is disabled, Try to power on nodes: %v", nodesToWake)
			nodesToPowerOn = append(nodesToPowerOn, nodesToWake...)
			nodesToWake = nil
		}

		if len(nodesToSleep) > 0 {
			log.Warnf("EnableSleep is disabled, Try to power off nodes: %v", nodesToSleep)
			nodesToPowerOff = append(nodesToPowerOff, nodesToSleep...)
			nodesToSleep = nil
		}
	}

	if len(nodesToWake) > 0 {
		if err := c.wakeupNodes(nodesToWake); err != nil {
			log.Errorf("Failed to wake up nodes: %v", err)
		}
	}

	if len(nodesToPowerOn) > 0 {
		if err := c.powerOnNodes(nodesToPowerOn); err != nil {
			log.Errorf("Failed to power on nodes: %v", err)
		}
	}

	if len(nodesToSleep) > 0 {
		if err := c.sleepNodes(nodesToSleep); err != nil {
			log.Errorf("Failed to sleep nodes: %v", err)
		}
	}

	if len(nodesToPowerOff) > 0 {
		if err := c.powerOffNodes(nodesToPowerOff); err != nil {
			log.Errorf("Failed to power off nodes: %v", err)
		}
	}

	return len(nodesToWake), len(nodesToPowerOn),
		len(nodesToSleep), len(nodesToPowerOff)
}

func (c *PowerManager) updateNodeJob(nodeID string, jobID string, isAdd bool) bool {
	c.nodesInfoMutex.Lock()
	defer c.nodesInfoMutex.Unlock()

	value, exists := c.nodesInfo.Load(nodeID)

	if !exists {
		log.Errorf("Node %s not found in nodesInfo, please check the node registration", nodeID)
		return false
	}

	info := value.(*NodeInfo)
	if info.State != Active && info.State != Idle {
		log.Errorf("Node %s is not in Active or Idle state, please check the node state", nodeID)
		return false
	}

	if info.Jobs == nil {
		info.Jobs = make(map[string]struct{})
	}

	if isAdd {
		log.Debugf("Add job %s to node %s", jobID, nodeID)
		info.Jobs[jobID] = struct{}{}
		c.nodesInfo.Store(nodeID, info)
		if len(info.Jobs) == 1 {
			log.Debugf("Node %s has only one job, set to Active", nodeID)
			c.updateNodeState(nodeID, Active)
		}
	} else {
		log.Debugf("Remove job %s from node %s", jobID, nodeID)
		delete(info.Jobs, jobID)
		c.nodesInfo.Store(nodeID, info)
		if len(info.Jobs) == 0 {
			c.updateNodeState(nodeID, Idle)
		}
	}
	return true
}

func (c *PowerManager) updateNodeState(nodeID string, newState NodeState) {
	currentTime := time.Now()

	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		log.Warnf("Node %s not found in nodesInfo, please check the node registration", nodeID)

		newInfo := &NodeInfo{
			State:               newState,
			LastStateChangeTime: currentTime,
			Jobs:                make(map[string]struct{}),
		}
		c.nodesInfo.Store(nodeID, newInfo)
		return
	}

	info := value.(*NodeInfo)
	if info.State != newState {
		// Log at Debug level for routine Idle <-> Active transitions, Info for power state changes
		if (info.State == Idle && newState == Active) || (info.State == Active && newState == Idle) {
			log.Debugf("Node %s state changed from %s to %s", nodeID, info.State, newState)
		} else {
			log.Infof("Node %s state changed from %s to %s", nodeID, info.State, newState)
		}
		oldState := info.State
		newInfo := &NodeInfo{
			State:               newState,
			LastStateChangeTime: currentTime,
			Jobs:                info.Jobs,
		}
		c.nodesInfo.Store(nodeID, newInfo)

		c.recordStateChange(currentTime, nodeID, oldState, newState)
		c.notifyCtldPowerStateChange(nodeID, newState)
	} else {
		log.Warnf("Node %s state is already %s, skip state change", nodeID, newState)
	}
}

func (c *PowerManager) notifyCtldPowerStateChange(nodeID string, state NodeState) {
	if c.ctldClient == nil {
		c.initCtldClient()
	}

	var powerType protos.CranedPowerState
	switch state {
	case Active:
		powerType = protos.CranedPowerState_CRANE_POWER_ACTIVE
	case Idle:
		powerType = protos.CranedPowerState_CRANE_POWER_IDLE
	case Sleep:
		powerType = protos.CranedPowerState_CRANE_POWER_SLEEPING
	case PoweredOff:
		powerType = protos.CranedPowerState_CRANE_POWER_POWEREDOFF
	case ToSleeping:
		powerType = protos.CranedPowerState_CRANE_POWER_TO_SLEEPING
	case Wakingup:
		powerType = protos.CranedPowerState_CRANE_POWER_WAKING_UP
	case PoweringOn:
		powerType = protos.CranedPowerState_CRANE_POWER_POWERING_ON
	case PoweringOff:
		powerType = protos.CranedPowerState_CRANE_POWER_POWERING_OFF
	default:
		log.Warnf("Unknown node state: %s, defaulting to ACTIVE", state)
		powerType = protos.CranedPowerState_CRANE_POWER_ACTIVE
	}

	req := &protos.PowerStateChangeRequest{
		CranedId: nodeID,
		State:    powerType,
		Reason:   fmt.Sprintf("Node %s power state changed to %s by power manager(plugin/powerControl)", nodeID, state),
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			log.Warnf("Retrying PowerStateChange for node %s, attempt %d/%d", nodeID, attempt, maxRetries)
			time.Sleep(retryInterval)
			c.initCtldClient()
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		reply, err := c.ctldClient.PowerStateChange(ctx, req)
		cancel()

		if err != nil {
			log.Warnf("Attempt %d/%d: Failed to call PowerStateChange RPC for node %s: %v",
				attempt, maxRetries, nodeID, err)

			if attempt == maxRetries {
				log.Errorf("All retry attempts failed for PowerStateChange RPC for node %s", nodeID)
			}
			continue
		}

		if !reply.Ok {
			log.Errorf("Failed to update node %s power state in CraneCtld", nodeID)
		} else {
			// Log at Debug level for routine Idle <-> Active transitions, Info for power state changes
			if state == Idle || state == Active {
				log.Debugf("Successfully updated node %s power state to %s in CraneCtld", nodeID, powerType)
			} else {
				log.Infof("Successfully updated node %s power state to %s in CraneCtld", nodeID, powerType)
			}
		}

		return
	}
}

func (c *PowerManager) getPredictedActiveNodeCount() int {
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	totalNodes := 0
	c.nodesInfo.Range(func(key, value interface{}) bool {
		totalNodes++
		return true
	})

	reqBody := PredictionRequest{
		TotalNodes: totalNodes,
	}
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		log.Errorf("Failed to marshal prediction request: %v", err)
		return -1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/predict", c.config.Predictor.URL),
		bytes.NewBuffer(jsonData))
	if err != nil {
		log.Errorf("Failed to create request: %v", err)
		return -1
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("Failed to get prediction: %v", err)
		return -1
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Failed to read response body: %v", err)
		return -1
	}

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Prediction request failed with status %d: %s", resp.StatusCode, string(body))
		return -1
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		log.Errorf("Unexpected content type %s: %s", contentType, string(body))
		return -1
	}

	var predResp PredictionResponse
	if err := json.Unmarshal(body, &predResp); err != nil {
		log.Errorf("Failed to decode prediction response: %v\nResponse body: %s", err, string(body))
		return -1
	}

	return predResp.Prediction
}

func (c *PowerManager) makeDecision() ([]string, []string, []string, []string) {
	currentTime := time.Now()

	predictedActiveNodeCount := c.getPredictedActiveNodeCount()

	totalNodes := 0
	c.nodesInfo.Range(func(key, value interface{}) bool {
		totalNodes++
		return true
	})

	activeNodes := c.GetNodesByState(Active)
	idleNodes := c.GetNodesByState(Idle)
	sleepNodes := c.GetNodesByState(Sleep)
	poweredOffNodes := c.GetNodesByState(PoweredOff)

	log.Debugf("Current total node count: %d", totalNodes)
	log.Debugf("Predicted active node count: %d", predictedActiveNodeCount)
	log.Debugf("Current active node count: %d", len(activeNodes))
	log.Debugf("Current idle node count: %d", len(idleNodes))
	log.Debugf("Current sleep node count: %d", len(sleepNodes))
	log.Debugf("Current powered off node count: %d", len(poweredOffNodes))

	if predictedActiveNodeCount == -1 {
		log.Warnf("Failed to get predicted active node count, skip power control")
		return nil, nil, nil, nil
	}

	currentIdleNodeCount := len(idleNodes)
	currentActiveNodeCount := len(activeNodes)

	var nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff []string
	nodesToWake, nodesToPowerOn = c.getNodesForWakeUpOrPowerOn(
		currentTime,
		predictedActiveNodeCount,
		currentActiveNodeCount,
		currentIdleNodeCount,
		sleepNodes,
		poweredOffNodes,
	)

	if len(nodesToWake) == 0 && len(nodesToPowerOn) == 0 {
		nodesToSleep, nodesToPowerOff = c.getNodesForSleepOrPowerOff(
			currentTime,
			predictedActiveNodeCount,
			currentActiveNodeCount,
			idleNodes,
			sleepNodes,
		)
	}

	c.recordClusterState(
		currentTime,
		predictedActiveNodeCount,
		currentActiveNodeCount,
		currentIdleNodeCount,
		len(sleepNodes),
		len(poweredOffNodes),
		len(nodesToWake),
		len(nodesToPowerOn),
		len(nodesToSleep),
		len(nodesToPowerOff),
	)

	// Log decision summary
	log.Infof("Power decision: predicted=%d nodes, current: active=%d idle=%d sleep=%d off=%d | actions: wake=%d on=%d sleep=%d off=%d",
		predictedActiveNodeCount,
		len(activeNodes), len(idleNodes), len(sleepNodes), len(poweredOffNodes),
		len(nodesToWake), len(nodesToPowerOn), len(nodesToSleep), len(nodesToPowerOff))

	return nodesToWake, nodesToPowerOn, nodesToSleep, nodesToPowerOff
}

func (c *PowerManager) getNodesForWakeUpOrPowerOn(
	currentTime time.Time,
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	currentIdleNodeCount int,
	sleepingNodes []string,
	poweredOffNodes []string,
) ([]string, []string) {
	totalNodes := 0
	c.nodesInfo.Range(func(key, value interface{}) bool {
		totalNodes++
		return true
	})

	requiredIdleNodeCount := int(math.Ceil(float64(totalNodes) * c.config.PowerControl.IdleReserveRatio))
	totalAvailableNodeCount := currentActiveNodeCount + currentIdleNodeCount
	requiredTotalNodeCount := predictedActiveNodeCount + requiredIdleNodeCount

	neededNodeCount := requiredTotalNodeCount - totalAvailableNodeCount
	if neededNodeCount <= 0 {
		return nil, nil
	}

	var nodesToWake []string
	var nodesToPowerOn []string

	if len(sleepingNodes) > 0 {
		sortedNodes := make([]string, len(sleepingNodes))
		copy(sortedNodes, sleepingNodes)
		sort.Slice(sortedNodes, func(i, j int) bool {
			valueI, _ := c.nodesInfo.Load(sortedNodes[i])
			infoI := valueI.(*NodeInfo)

			valueJ, _ := c.nodesInfo.Load(sortedNodes[j])
			infoJ := valueJ.(*NodeInfo)

			return infoI.LastStateChangeTime.Before(infoJ.LastStateChangeTime)
		})

		if len(sortedNodes) >= neededNodeCount {
			nodesToWake = sortedNodes[:neededNodeCount]
			neededNodeCount = 0
		} else {
			nodesToWake = sortedNodes
			neededNodeCount -= len(sortedNodes)
		}
	}

	if neededNodeCount > 0 && len(poweredOffNodes) > 0 {
		sortedNodes := make([]string, len(poweredOffNodes))
		copy(sortedNodes, poweredOffNodes)
		sort.Slice(sortedNodes, func(i, j int) bool {
			valueI, _ := c.nodesInfo.Load(sortedNodes[i])
			infoI := valueI.(*NodeInfo)

			valueJ, _ := c.nodesInfo.Load(sortedNodes[j])
			infoJ := valueJ.(*NodeInfo)

			return infoI.LastStateChangeTime.Before(infoJ.LastStateChangeTime)
		})

		if len(sortedNodes) >= neededNodeCount {
			nodesToPowerOn = sortedNodes[:neededNodeCount]
			neededNodeCount = 0
		} else {
			nodesToPowerOn = sortedNodes
			neededNodeCount -= len(sortedNodes)
		}
	}

	log.Debugf("[Time %v] Wake-up decision:\n"+
		"  Current active: %d\n"+
		"  Predicted active: %d\n"+
		"  Current idle: %d\n"+
		"  Required idle: %d\n"+
		"  Total available: %d\n"+
		"  Required total: %d\n"+
		"  Nodes needed: %d\n"+
		"  Nodes to wake up: %d\n"+
		"  Nodes to power on: %d\n",
		currentTime,
		currentActiveNodeCount,
		predictedActiveNodeCount,
		currentIdleNodeCount,
		requiredIdleNodeCount,
		totalAvailableNodeCount,
		requiredTotalNodeCount,
		neededNodeCount,
		len(nodesToWake),
		len(nodesToPowerOn))

	return nodesToWake, nodesToPowerOn
}

func (c *PowerManager) getNodesForSleepOrPowerOff(
	currentTime time.Time,
	predictedActiveNodeCount int,
	currentActiveNodeCount int,
	idleNodes []string,
	sleepingNodes []string,
) ([]string, []string) {
	var nodesToSleep []string
	var nodesToPowerOff []string

	if len(idleNodes) > 0 {
		totalNodes := 0
		c.nodesInfo.Range(func(key, value interface{}) bool {
			totalNodes++
			return true
		})

		requiredIdleCount := int(math.Ceil(float64(totalNodes) * c.config.PowerControl.IdleReserveRatio))
		currentIdleNodeCount := len(idleNodes)
		log.Debugf("Idle reserve ratio: %f", c.config.PowerControl.IdleReserveRatio)
		log.Debugf("Total nodes: %d, Current idle node count: %d, required idle count: %d", totalNodes, currentIdleNodeCount, requiredIdleCount)

		nodesCanSleepCount := currentIdleNodeCount - requiredIdleCount
		if nodesCanSleepCount > 0 {
			sortedIdleNodes := make([]string, len(idleNodes))
			copy(sortedIdleNodes, idleNodes)

			sort.Slice(sortedIdleNodes, func(i, j int) bool {
				valueI, _ := c.nodesInfo.Load(sortedIdleNodes[i])
				infoI := valueI.(*NodeInfo)

				valueJ, _ := c.nodesInfo.Load(sortedIdleNodes[j])
				infoJ := valueJ.(*NodeInfo)

				return infoI.LastStateChangeTime.Before(infoJ.LastStateChangeTime)
			})

			if c.config.PowerControl.EnableSleep {
				nodesToSleep = sortedIdleNodes[:nodesCanSleepCount]
			} else {
				nodesToPowerOff = sortedIdleNodes[:nodesCanSleepCount]
			}
		}
	}

	for _, node := range sleepingNodes {
		value, ok := c.nodesInfo.Load(node)
		if !ok {
			log.Errorf("Node %s not found in nodesInfo", node)
			continue
		}
		info := value.(*NodeInfo)

		log.Debugf("node %s last state change time: %s", node, info.LastStateChangeTime)
		sleepTime := currentTime.Sub(info.LastStateChangeTime)
		log.Debugf("node %s sleep time: %s", node, sleepTime)

		if sleepTime >= time.Duration(c.config.PowerControl.SleepTimeThresholdSeconds)*time.Second {
			nodesToPowerOff = append(nodesToPowerOff, node)
		}
	}

	log.Debugf("[Time %v] Sleep/PowerOff decision:\n"+
		"  Current active: %d\n"+
		"  Predicted active: %d\n"+
		"  Current idle: %d\n"+
		"  Nodes to sleep: %d\n"+
		"  Nodes to shutdown: %d\n"+
		"  Remaining idle after sleep and power off: %d\n",
		currentTime,
		currentActiveNodeCount,
		predictedActiveNodeCount,
		len(idleNodes),
		len(nodesToSleep),
		len(nodesToPowerOff),
		len(idleNodes)-len(nodesToSleep)-len(nodesToPowerOff))

	return nodesToSleep, nodesToPowerOff
}

func (c *PowerManager) startPowerStateMonitor() {
	go func() {
		ticker := time.NewTicker(time.Duration(c.config.PowerControl.NodeStateCheckIntervalSeconds) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				c.checkPowerState()
			case <-c.stopChan:
				return
			}
		}
	}()
}

func (c *PowerManager) checkPowerState() {
	c.nodesInfo.Range(func(key, value interface{}) bool {
		nodeID := key.(string)
		info := value.(*NodeInfo)

		switch info.State {
		case PoweringOn, Wakingup:
			poweredOn, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				return true
			}

			if !poweredOn {
				return true
			}

			alive := c.powerTool.CheckNodeAlive(nodeID)
			if alive {
				c.updateNodeState(nodeID, Idle)
				log.Infof("Node %s is now available", nodeID)
			}

		case PoweringOff:
			poweredOn, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				return true
			}

			if !poweredOn {
				c.updateNodeState(nodeID, PoweredOff)
				log.Infof("Node %s is now powered off", nodeID)
			}

		case ToSleeping:
			alive := c.powerTool.CheckNodeAlive(nodeID)
			if !alive {
				c.updateNodeState(nodeID, Sleep)
				log.Infof("Node %s is now sleeping", nodeID)
			}

		case Idle, Active:
			alive := c.powerTool.CheckNodeAlive(nodeID)
			if !alive {
				powered, err := c.powerTool.GetPowerState(nodeID)
				if err != nil {
					log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
					return true
				}
				if !powered {
					c.updateNodeState(nodeID, PoweredOff)
					log.Warnf("Node %s was idle but found powered off", nodeID)
				} else {
					c.updateNodeState(nodeID, Sleep)
					log.Warnf("Node %s was idle but found sleeping", nodeID)
				}
			}

		case Sleep:
			powered, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				return true
			}
			if !powered {
				c.updateNodeState(nodeID, PoweredOff)
				log.Warnf("Node %s was sleeping but found powered off", nodeID)
			} else {
				alive := c.powerTool.CheckNodeAlive(nodeID)
				if alive {
					c.updateNodeState(nodeID, Idle)
					log.Warnf("Node %s was sleeping but found active", nodeID)
				}
			}

		case PoweredOff:
			powered, err := c.powerTool.GetPowerState(nodeID)
			if err != nil {
				log.Errorf("Failed to check power status for node %s: %v", nodeID, err)
				return true
			}
			if powered {
				alive := c.powerTool.CheckNodeAlive(nodeID)
				if alive {
					c.updateNodeState(nodeID, Idle)
					log.Warnf("Node %s was powered off but found active", nodeID)
				} else {
					c.updateNodeState(nodeID, Sleep)
					log.Warnf("Node %s was powered off but found sleeping", nodeID)
				}
			}
		}
		return true
	})
}

func (c *PowerManager) recordClusterState(
	currentTime time.Time,
	prediction int,
	activeCount int,
	idleCount int,
	sleepCount int,
	powerOffCount int,
	toWakeCount int,
	toPowerOnCount int,
	toSleepCount int,
	toPowerOffCount int,
) {
	dir := filepath.Dir(c.config.PowerControl.ClusterStateFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Errorf("Failed to create directory %s: %v", dir, err)
		return
	}

	if _, err := os.Stat(c.config.PowerControl.ClusterStateFile); os.IsNotExist(err) {
		file, err := os.Create(c.config.PowerControl.ClusterStateFile)
		if err != nil {
			log.Errorf("Failed to create cluster state file: %v", err)
			return
		}
		writer := csv.NewWriter(file)
		headers := []string{
			"Timestamp",
			"PredictedActive",
			"CurrentActive",
			"CurrentIdle",
			"CurrentSleep",
			"CurrentPowerOff",
			"PoweringOn",
			"WakingUp",
			"ToSleeping",
			"PoweringOff",
			"ToWake",
			"ToPowerOn",
			"ToSleep",
			"ToPowerOff",
		}
		if err := writer.Write(headers); err != nil {
			log.Warnf("Failed to write headers: %v", err)
			file.Close()
			return
		}
		writer.Flush()
		file.Close()
	}

	file, err := os.OpenFile(c.config.PowerControl.ClusterStateFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Warnf("Failed to open cluster state file: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	poweringOnCount := 0
	wakingUpCount := 0
	toSleepingCount := 0
	poweringOffCount := 0

	c.nodesInfo.Range(func(key, value interface{}) bool {
		info := value.(*NodeInfo)
		switch info.State {
		case PoweringOn:
			poweringOnCount++
		case Wakingup:
			wakingUpCount++
		case ToSleeping:
			toSleepingCount++
		case PoweringOff:
			poweringOffCount++
		}
		return true
	})

	dateTime := currentTime.Format("2006-01-02 15:04:05")
	record := []string{
		dateTime,
		fmt.Sprintf("%d", prediction),
		fmt.Sprintf("%d", activeCount),
		fmt.Sprintf("%d", idleCount),
		fmt.Sprintf("%d", sleepCount),
		fmt.Sprintf("%d", powerOffCount),
		fmt.Sprintf("%d", poweringOnCount),
		fmt.Sprintf("%d", wakingUpCount),
		fmt.Sprintf("%d", toSleepingCount),
		fmt.Sprintf("%d", poweringOffCount),
		fmt.Sprintf("%d", toWakeCount),
		fmt.Sprintf("%d", toPowerOnCount),
		fmt.Sprintf("%d", toSleepCount),
		fmt.Sprintf("%d", toPowerOffCount),
	}

	if err := writer.Write(record); err != nil {
		log.Warnf("Failed to write cluster state record: %v", err)
	}
}

func (c *PowerManager) recordStateChange(time time.Time, nodeID string, oldState, newState NodeState) {
	// If NodeStateChangeFile is not configured, skip recording
	if c.config.PowerControl.NodeStateChangeFile == "" {
		return
	}

	dir := filepath.Dir(c.config.PowerControl.NodeStateChangeFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Errorf("Failed to create directory %s: %v", dir, err)
		return
	}

	if _, err := os.Stat(c.config.PowerControl.NodeStateChangeFile); os.IsNotExist(err) {
		file, err := os.Create(c.config.PowerControl.NodeStateChangeFile)
		if err != nil {
			log.Errorf("Failed to create state change log: %v", err)
			return
		}
		defer file.Close()

		writer := csv.NewWriter(file)
		if err := writer.Write([]string{"time", "node_id", "old_state", "new_state"}); err != nil {
			log.Errorf("Failed to write header: %v", err)
			return
		}
		writer.Flush()
	}

	file, err := os.OpenFile(c.config.PowerControl.NodeStateChangeFile, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Failed to open state change log: %v", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	oldStateStr := string(oldState)
	if oldState == "" {
		oldStateStr = "INIT"
	}
	err = writer.Write([]string{
		time.Format("2006-01-02 15:04:05"),
		nodeID,
		oldStateStr,
		string(newState),
	})
	if err != nil {
		log.Errorf("Failed to write state change: %v", err)
	}
	writer.Flush()
}
