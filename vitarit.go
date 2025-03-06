package vitarit

// Vitarit struct
type Vitarit struct {
	node  nodeInfo          // Embedding nodeInfo struct to Vitarit struct
	cache *distributedCache // Embedding distributedCache struct to Vitarit struct
}

// NewVitarit function to create a new Vitarit struct
func NewVitarit(nodeId string, ip string, port string, groupID string) *Vitarit {
	node := nodeInfo{
		ID:      nodeId,
		IP:      ip,
		Port:    port,
		GroupID: groupID,
	}

	return &Vitarit{
		node:  node,
		cache: nil,
	}
}

// SetLogger function to set the logger function
func (v *Vitarit) SetLogger(f func(int, string)) {
	logFunc = f
}

// Start this node and join the ring
func (v *Vitarit) Start(redundancy int) {
	// Create a new distribute cache object to add this node to the ring
	v.cache = newDistributedCache(redundancy)
	v.cache.addNode(v.node)

	// Start peer discovery using heartbeats
	v.cache.start(v.node)
}

// Stop the peer discovery and server of this node
func (v *Vitarit) Stop() {
	v.cache.stop()
}

// Get the value of key from the ring
func (v *Vitarit) Get(key string) ([]byte, bool) {
	return v.cache.get(key)
}

// Set value of given key in the ring
func (v *Vitarit) Set(key string, value []byte) {
	v.cache.set(key, value)
}

// Remove this key from the ring
func (v *Vitarit) Remove(key string) {
	v.cache.remove(key)
}
