package hasher

import (
	"strings"
)

type HasherContainer struct {
	buffer map[string][][]string
}

// NewHasherContainer creates and initializes a new HasherContainer instance.
// It takes a map dictNodePosition, where each key is a string representing a node,
// and the value is an integer representing the number of replicas for that node.
// For each node, it allocates a slice of string slices (one for each replica) in the buffer.
// Returns a pointer to the newly created HasherContainer.
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

// AddMessage adds a message associated with a given movieId to the buffer of each node type.
// The message is appended to the replica determined by movieId modulo the number of replicas for that node type.
// Parameters:
//   - movieId: an integer representing the ID of the movie, used to determine the replica index.
//   - message: the message string to be added to the buffer.
func (hc *HasherContainer) AddMessage(movieId int, message string) {
	for nodeType := range hc.buffer {
		replicaIndex := movieId % len(hc.buffer[nodeType])
		hc.buffer[nodeType][replicaIndex] = append(hc.buffer[nodeType][replicaIndex], message)
	}
}

// GetMessages retrieves and returns all buffered messages for the specified nodeType.
// The returned map has the message index as the key and the concatenated message string as the value.
// After retrieval, the messages for the given nodeType are cleared from the buffer.
// If there are no messages for the nodeType, it returns nil.
//
// Parameters:
//   - nodeType: The type of node whose messages are to be retrieved.
//
// Returns:
//   - map[int]string: A map where each key is the message index and the value is the concatenated message string.
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
