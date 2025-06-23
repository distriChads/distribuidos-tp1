package hasher

import (
	"strings"
)

type HasherContainer struct {
	buffer map[string][][]string
}

func NewHasherContainer(dictNodePosition map[string]int) *HasherContainer {
	buffer := make(map[string][][]string)

	for key, replicas := range dictNodePosition {
		replicaBuffer := make([][]string, replicas)
		for i := range replicas {
			replicaBuffer[i] = []string{}
		}
		buffer[key] = replicaBuffer
	}

	return &HasherContainer{
		buffer: buffer,
	}
}

func (hc *HasherContainer) AddMessage(movieId int, message string) {
	for nodeType := range hc.buffer {
		replicaIndex := movieId % len(hc.buffer[nodeType])
		hc.buffer[nodeType][replicaIndex] = append(hc.buffer[nodeType][replicaIndex], message)
	}
}

func (hc *HasherContainer) GetMessages(nodeType string) map[int]string {
	if messages, exists := hc.buffer[nodeType]; exists {
		results := make(map[int]string)
		for i, msg := range messages {
			if len(msg) > 0 {
				results[i] = strings.Join(msg, "\n")
			}
		}
		hc.clearMessages(nodeType)
		return results
	}
	return nil
}

func (hc *HasherContainer) clearMessages(nodeType string) {
	if _, exists := hc.buffer[nodeType]; exists {
		for i := range hc.buffer[nodeType] {
			hc.buffer[nodeType][i] = []string{}
		}
	}
}
