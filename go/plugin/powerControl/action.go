package main

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

func (c *PowerManager) filterExcludedNodes(nodeIDs []string) []string {
	var allowedNodes []string
	for _, nodeID := range nodeIDs {
		value, exists := c.nodesInfo.Load(nodeID)
		if !exists {
			log.Warnf("Node %s not found in nodesInfo, skipping", nodeID)
			continue
		}

		info := value.(*NodeInfo)
		if !info.Exclude {
			allowedNodes = append(allowedNodes, nodeID)
		} else {
			log.Infof("Node %s is excluded from power management", nodeID)
		}
	}
	return allowedNodes
}

func (c *PowerManager) wakeupNodes(nodeIDs []string) error {
	for _, nodeID := range nodeIDs {
		err := c.wakeUpNode(nodeID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *PowerManager) sleepNodes(nodeIDs []string) error {
	allowedNodes := c.filterExcludedNodes(nodeIDs)
	for _, nodeID := range allowedNodes {
		if err := c.sleepNode(nodeID); err != nil {
			return err
		}
	}
	return nil
}

func (c *PowerManager) processBatchNodes(nodes []string, operation string, nodeFunc func(string) error,
	maxNodesPerBatch int, batchIntervalSeconds int) error {

	if len(nodes) == 0 {
		return nil
	}

	batchInterval := time.Duration(batchIntervalSeconds) * time.Second

	go func() {
		for i := 0; i < len(nodes); i += maxNodesPerBatch {
			end := i + maxNodesPerBatch
			if end > len(nodes) {
				end = len(nodes)
			}

			currentBatch := nodes[i:end]
			log.Infof("Processing %s batch %d-%d of %d nodes", operation, i, end-1, len(nodes))

			for _, nodeID := range currentBatch {
				if err := nodeFunc(nodeID); err != nil {
					log.Errorf("Failed to %s node %s: %v", operation, nodeID, err)
				} else {
					log.Infof("Successfully %s node %s", operation, nodeID)
				}
			}

			if end < len(nodes) {
				log.Infof("Waiting %d seconds before processing next %s batch...",
					batchIntervalSeconds, operation)
				time.Sleep(batchInterval)
			}
		}
	}()

	return nil
}

func (c *PowerManager) powerOnNodes(nodeIDs []string) error {
	allowedNodes := c.filterExcludedNodes(nodeIDs)

	maxNodesPerBatch := c.config.IPMI.MaxNodesPerBatch
	batchIntervalSeconds := c.config.IPMI.BatchIntervalSeconds
	if batchIntervalSeconds > 10 {
		batchIntervalSeconds = 10
	}

	return c.processBatchNodes(allowedNodes, "power on", c.powerOnNode,
		maxNodesPerBatch, batchIntervalSeconds)
}

func (c *PowerManager) powerOffNodes(nodeIDs []string) error {
	allowedNodes := c.filterExcludedNodes(nodeIDs)

	maxNodesPerBatch := c.config.IPMI.MaxNodesPerBatch
	batchIntervalSeconds := c.config.IPMI.BatchIntervalSeconds

	return c.processBatchNodes(allowedNodes, "power off", c.powerOffNode,
		maxNodesPerBatch, batchIntervalSeconds)
}

func (c *PowerManager) wakeUpNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != Sleep {
		return fmt.Errorf("node %s is not in sleeping state", nodeID)
	}

	c.updateNodeState(nodeID, Wakingup)

	err := c.powerTool.WakeUp(nodeID)
	if err != nil {
		log.Errorf("Failed to wake up node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, Sleep)
		return err
	}

	return nil
}

func (c *PowerManager) powerOnNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != PoweredOff {
		return fmt.Errorf("node %s is not in powered off state", nodeID)
	}

	c.updateNodeState(nodeID, PoweringOn)

	err := c.powerTool.PowerOn(nodeID)
	if err != nil {
		log.Errorf("Failed to power on node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, PoweredOff)
		return err
	}

	return nil
}

func (c *PowerManager) sleepNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	if info.State != Idle {
		return fmt.Errorf("node %s is not in idle state", nodeID)
	}

	c.updateNodeState(nodeID, ToSleeping)

	err := c.powerTool.Sleep(nodeID)
	if err != nil {
		log.Errorf("Failed to put node %s to sleep: %v", nodeID, err)
		c.updateNodeState(nodeID, Idle)
		return err
	}

	return nil
}

func (c *PowerManager) powerOffNode(nodeID string) error {
	value, exists := c.nodesInfo.Load(nodeID)
	if !exists {
		return fmt.Errorf("node %s not found", nodeID)
	}

	info := value.(*NodeInfo)
	oldState := info.State
	if oldState != Sleep && oldState != Idle {
		return fmt.Errorf("node %s is not in sleep or idle state", nodeID)
	}

	c.updateNodeState(nodeID, PoweringOff)

	err := c.powerTool.PowerOff(nodeID)
	if err != nil {
		log.Errorf("Failed to power off node %s: %v", nodeID, err)
		c.updateNodeState(nodeID, oldState)
		return err
	}

	return nil
}
