package vitarit

import (
	"hash/crc32"
	"sort"
	"sync"
)

// hashRing is a consistent hash ring that uses the CRC32 algorithm
type hashRing struct {
	nodes        []*cacheNode          // List of nodes participating in the ring
	sortedHashes []uint32              // Sorted list of hashes
	nodeMap      map[uint32]*cacheNode // Maps hash to node
	mtx          sync.Mutex            // Lock to protect the ring
}

// -----------------------------------------------------------------------

// NewHashRing allocates a new hash ring
func NewHashRing() *hashRing {
	logMessage(LOG_DEBUG, "creating new hash ring")

	return &hashRing{
		nodeMap: make(map[uint32]*cacheNode),
	}
}

// -----------------------------------------------------------------------

// addNode adds a new node to the hash ring
func (ring *hashRing) addNode(node nodeInfo) {
	hash := crc32.ChecksumIEEE([]byte(node.ID))

	ring.mtx.Lock()
	defer ring.mtx.Unlock()

	_, found := ring.nodeMap[hash]
	if found {
		// This is just a heartbeat from an existing node
		// No need to update any data structures here just mark that node is alive
		logMessage(LOG_DEBUG, "HB received from "+node.ID)
		return
	}

	// New node found so lets create it and add it to our consistent hash ring
	cnode := newCacheNode(node)

	logMessage(LOG_DEBUG, "hashring adding a new node "+cnode.ID)
	ring.nodes = append(ring.nodes, cnode)
	ring.sortedHashes = append(ring.sortedHashes, hash)
	ring.nodeMap[hash] = cnode

	sort.Slice(ring.sortedHashes, func(i, j int) bool {
		return ring.sortedHashes[i] < ring.sortedHashes[j]
	})
}

// removeNode removes a node from the hash ring
func (ring *hashRing) removeNode(nodeID string) {
	hash := crc32.ChecksumIEEE([]byte(nodeID))

	ring.mtx.Lock()
	defer ring.mtx.Unlock()

	_, found := ring.nodeMap[hash]
	if !found {
		// This node is not found in the ring
		logMessage(LOG_DEBUG, "hashring not able to remove "+nodeID)
		return
	}

	logMessage(LOG_DEBUG, "hashring removing "+nodeID)
	delete(ring.nodeMap, hash)

	for i, h := range ring.sortedHashes {
		if h == hash {
			ring.sortedHashes = append(ring.sortedHashes[:i], ring.sortedHashes[i+1:]...)
			break
		}
	}

	for i, node := range ring.nodes {
		if node.ID == nodeID {
			ring.nodes = append(ring.nodes[:i], ring.nodes[i+1:]...)
			break
		}
	}
}

// -----------------------------------------------------------------------

// getNodeByID returns the node with the given ID
func (ring *hashRing) getNodeByID(id string) *cacheNode {
	hash := crc32.ChecksumIEEE([]byte(id))

	ring.mtx.Lock()
	defer ring.mtx.Unlock()

	logMessage(LOG_DEBUG, "hashring searching node for id "+id)

	return ring.nodeMap[hash]
}

// getNode returns the node that a key belongs to
func (ring *hashRing) getNode(key string) *cacheNode {
	hash := crc32.ChecksumIEEE([]byte(key))

	ring.mtx.Lock()
	defer ring.mtx.Unlock()

	logMessage(LOG_DEBUG, "hashring searching node for key "+key)

	idx := sort.Search(len(ring.sortedHashes), func(i int) bool {
		return ring.sortedHashes[i] >= hash
	})

	if idx == len(ring.sortedHashes) {
		idx = 0
	}

	return ring.nodeMap[ring.sortedHashes[idx]]
}

// -----------------------------------------------------------------------

// getNode returns the node that a key belongs to
func (ring *hashRing) getNodes(key string, redundancy int) []*cacheNode {
	nodes := []*cacheNode{}
	hash := crc32.ChecksumIEEE([]byte(key))

	ring.mtx.Lock()
	defer ring.mtx.Unlock()

	logMessage(LOG_DEBUG, "hashring searching node for key "+key)

	idx := sort.Search(len(ring.sortedHashes), func(i int) bool {
		return ring.sortedHashes[i] >= hash
	})

	// Normalise index of the node where the key belongs
	if idx == len(ring.sortedHashes) {
		idx = 0
	}

	masterIdx := idx
	nodes = append(nodes, ring.nodeMap[ring.sortedHashes[idx]])

	for redundancy > 0 {
		idx++
		if idx >= len(ring.sortedHashes) {
			idx = 0
		}

		if idx == masterIdx {
			break
		}

		nodes = append(nodes, ring.nodeMap[ring.sortedHashes[idx]])
		redundancy--
	}

	return nodes
}
