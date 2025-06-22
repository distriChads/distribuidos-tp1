package hasher

import (
	"distribuidos-tp1/common/worker/worker"
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

func (hc *HasherContainer) GetMessages(nodeType string) [][]string {
	if messages, exists := hc.buffer[nodeType]; exists {
		hc.clearMessages(nodeType)
		results := make([]string, len(messages))
		for i, msg := range messages {
			results[i] = strings.Join(msg, worker.MESSAGE_SEPARATOR)
		}
		return messages
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
