package vitarit

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// distributedCache is a distributed cache that uses consistent hashing
type distributedCache struct {
	*hashRing      // Consistent hash ring
	*peerDiscovery // Peer discovery module

	redundancy int // mentions how many copies of data should be stored

	nodeHB map[string]time.Time // Map of nodeID to heartbeat status
	mtx    sync.RWMutex         // Lock to protect the nodeHB
}

// -----------------------------------------------------------------------

// newDistributedCache allocates a new distributed cache
func newDistributedCache(redundancy int) *distributedCache {
	cache := &distributedCache{
		hashRing: NewHashRing(),
		peerDiscovery: &peerDiscovery{
			client:   nil,
			sendConn: nil,
			recvConn: nil,
		},

		redundancy: redundancy,
		nodeHB:     make(map[string]time.Time),
	}

	cache.client = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	logMessage(LOG_DEBUG, "creating new distributed cache, starting HB monitor")
	return cache
}

// -----------------------------------------------------------------------

func (cache *distributedCache) start(node nodeInfo) {
	cnode := cache.getNodeByID(node.ID)
	if cnode == nil {
		logMessage(LOG_ERROR, "failed to get node by ID")
		return
	}

	cnode.start()
	cache.startDiscovery(cnode.nodeInfo)
}

func (cache *distributedCache) stop() {
	cache.stopDiscovery()
}

// -----------------------------------------------------------------------

// addNode adds a new node to the distributed cache
func (cache *distributedCache) addNode(node nodeInfo) {
	cache.mtx.Lock()
	defer cache.mtx.Unlock()

	if _, found := cache.nodeHB[node.ID]; !found {
		logMessage(LOG_DEBUG, "adding node "+node.ID+" to the cache")
		cache.hashRing.addNode(node)
	}

	cache.nodeHB[node.ID] = time.Now()

}

// removeNode removes a node from the distributed cache
func (cache *distributedCache) removeNode_unlocked(nodeID string) {
	delete(cache.nodeHB, nodeID)
	cache.hashRing.removeNode(nodeID)
}

// -----------------------------------------------------------------------

// createURL creates a URL for a key on a node
func createURL(cnode *cacheNode, key string) string {
	return fmt.Sprintf("https://%s:%s?id=%s&key=%s", cnode.IP, cnode.Port, cnode.ID, key)
}

// createURLForRedundancy creates a URL to store a key on the node with the redundancy factor
func createURLForSet(cnode *cacheNode, key string, copy int) string {
	return fmt.Sprintf("https://%s:%s?id=%s&copy=%d", cnode.IP, cnode.Port, cnode.ID, copy)
}

// Get retrieves the value of a key from the distributed cache
func (cache *distributedCache) get(key string) ([]byte, bool) {
	nodes := cache.hashRing.getNodes(key, cache.redundancy)

	for idx, node := range nodes {
		logMessage(LOG_DEBUG, "sending get for key "+key+" to "+node.ID+" try "+fmt.Sprintf("%d", idx))
		data, err := cache.getFromNode(node, key)
		if err != nil {
			logMessage(LOG_ERROR, "failed to get key: "+key+" from "+node.ID)
			continue
		}

		return data, true
	}

	return []byte{}, false
}

// get key from a node which might own this cache key

func (cache *distributedCache) getFromNode(cnode *cacheNode, key string) ([]byte, error) {

	url := createURL(cnode, key)
	resp, err := cache.client.Get(url)

	if err != nil || resp.StatusCode != http.StatusOK {
		logMessage(LOG_ERROR, "failed to get key: "+key+" from "+cnode.ID)
		return []byte{}, fmt.Errorf("failed to get key: %s from %s", key, cnode.ID)
	}

	defer resp.Body.Close()
	value, err := io.ReadAll(resp.Body)

	if err != nil {
		logMessage(LOG_ERROR, "failed to read response body: "+err.Error())
		return []byte{}, fmt.Errorf("failed to read response body: %s", err.Error())
	}

	return value, nil
}

// set sets the value of a key in the distributed cache
func (cache *distributedCache) set(key string, value []byte) error {

	var err error = nil

	nodes := cache.hashRing.getNodes(key, cache.redundancy)
	for idx, node := range nodes {
		logMessage(LOG_DEBUG, "sending set for key "+key+" to "+node.ID+" with copy factor "+fmt.Sprintf("%d", idx-1))
		err = cache.setToNode(node, (idx - 1), key, value)
		if err == nil {
			break
		}
		logMessage(LOG_ERROR, "failed to set key: "+key+" to "+node.ID)
	}

	return err
}

// set sets the value of a key in the distributed cache
func (cache *distributedCache) setToNode(cnode *cacheNode, copy int, key string, value []byte) error {
	url := createURLForSet(cnode, key, copy)

	kv := map[string][]byte{key: value}
	data, _ := json.Marshal(kv)

	_, err := cache.client.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		logMessage(LOG_ERROR, "failed to set key: "+key+" to "+cnode.ID)
		return err
	}

	return nil
}

// remvoe deletes the entry fromt he hashring
func (cache *distributedCache) remove(key string) bool {
	node := cache.hashRing.getNode(key)
	url := createURL(node, key)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		logMessage(LOG_ERROR, "failed to create request: "+err.Error())
		return false
	}

	logMessage(LOG_DEBUG, "sending remove for key "+key+" to "+node.ID)

	// Send the DELETE request
	resp, err := cache.client.Do(req)
	if err != nil {
		logMessage(LOG_ERROR, "failed to send request: "+err.Error())
		return false
	}

	defer resp.Body.Close()

	// Read and print the response body
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		logMessage(LOG_ERROR, "failed to read response body: "+err.Error())
		return false
	}

	return true
}
