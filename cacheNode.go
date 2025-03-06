package vitarit

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log"
	"net/http"
	"strconv"
	"sync"
)

// nodeInfo contains information about a node in the cache cluster.
type nodeInfo struct {
	ID      string `json:"node_id"`
	IP      string `json:"node_ip"`
	Port    string `json:"node_port"`
	GroupID string `json:"group_id"`
}

// data cached per key
type cacheData struct {
	bytes []byte // Actual data recived for a given key
	copy  int    // Copy factor of the data, 0 means you are master, > 0 means its a redundant copy
	crc   uint32 // CRC32 checksum of the data
}

// cacheNode is a participating node in the cache cluster.
type cacheNode struct {
	nodeInfo // Information about the node

	data map[string]cacheData // Stores the key-value pairs
	mtx  sync.RWMutex         // Lock to protect the data

	server *http.Server // HTTP server for the node to serve REST calls
}

// -----------------------------------------------------------------------

// newCacheNode allocates a new node in the cluster
func newCacheNode(node nodeInfo) *cacheNode {
	logMessage(LOG_DEBUG, "creating new cache node with id: "+node.ID)

	return &cacheNode{
		nodeInfo: node,
		data:     make(map[string]cacheData),
		server:   nil,
	}
}

// -----------------------------------------------------------------------

// get retrieves the value of a key from the node
func (cnode *cacheNode) get(key string) ([]byte, bool) {
	cnode.mtx.RLock()
	defer cnode.mtx.RUnlock()

	value, exists := cnode.data[key]
	logMessage(LOG_DEBUG, cnode.ID+" get key: "+key+" Results"+fmt.Sprintf("%v", exists))

	return value.bytes, exists
}

// set sets the value of a key in the node
func (cnode *cacheNode) set(key string, copy int, value []byte) {
	cnode.mtx.Lock()
	defer cnode.mtx.Unlock()

	cnode.data[key] = cacheData{
		bytes: value,
		copy:  copy,
		crc:   crc32.ChecksumIEEE(value),
	}

	logMessage(LOG_DEBUG, cnode.ID+" set key: "+key)
}

// remove deletes a key from the node
func (cnode *cacheNode) remove(key string) {
	cnode.mtx.Lock()
	defer cnode.mtx.Unlock()

	delete(cnode.data, key)
	logMessage(LOG_DEBUG, cnode.ID+" remove key: "+key)
}

// -----------------------------------------------------------------------

// start starts the server for this node
func (cnode *cacheNode) start() {
	go cnode.startServer()
}

// stop stops the server for this node
func (cnode *cacheNode) stop() {
	cnode.server.Shutdown(context.TODO())
}

// -----------------------------------------------------------------------

// startServer starts the server for this node and listens for incoming requests
func (cnode *cacheNode) startServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", cnode.ServeHTTP)

	cnode.server = &http.Server{
		Addr:    cnode.IP + ":" + cnode.Port,
		Handler: mux,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	logMessage(LOG_DEBUG, cnode.ID+" starting https server")

	err := cnode.server.ListenAndServeTLS("cert.pem", "key.pem")
	if err != nil {
		log.Fatalf("server failed to start: %v", err)
	}
}

// -----------------------------------------------------------------------
// serveHTTP handles the HTTP requests coming to this particular node
func (cnode *cacheNode) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	switch r.Method {

	case http.MethodGet:
		// Retreive a key from the node
		key := r.URL.Query().Get("key")
		id := r.URL.Query().Get("id")

		logMessage(LOG_DEBUG, cnode.ID+" received get key: "+key+" from "+id)

		value, exists := cnode.get(key)
		if exists {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(value))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}

	case http.MethodPost:
		id := r.URL.Query().Get("id")
		copy, err := strconv.Atoi(r.URL.Query().Get("copy"))

		if err != nil {
			logMessage(LOG_DEBUG, cnode.ID+" received invalid set from "+id+" with copy factor "+fmt.Sprintf("%d", copy))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Set value corrosponding to a key in the node
		var kv map[string][]byte
		if err := json.NewDecoder(r.Body).Decode(&kv); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		for key, value := range kv {
			logMessage(LOG_DEBUG, cnode.ID+" received set key: "+key+" from "+id+" with copy factor "+fmt.Sprintf("%d", copy))
			cnode.set(key, copy, value)
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodDelete:
		// Remove a key from the node
		key := r.URL.Query().Get("key")
		logMessage(LOG_DEBUG, cnode.ID+" received remove key: "+key)
		cnode.remove(key)

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
