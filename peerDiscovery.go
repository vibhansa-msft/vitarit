package vitarit

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/ipv4"
)

/*
224.0.0.1: 		All hosts on the local network.
224.0.0.2: 		All routers on the local network.
224.0.0.9: 		Routing Information Protocol (RIP).
224.0.0.251: 	Multicast DNS (mDNS).
224.0.1.1: 		Network Time Protocol (NTP).
224.0.2.1: 		Ad-hoc block 1.
224.3.0.1: 		Ad-hoc block 2.
233.252.0.1: 		Ad-hoc block 3.
239.0.0.1: 		Administratively scoped.
*/

const (
	multicastAddress  = "224.0.0.1:8454"
	heartbeatInterval = 2 * time.Second
	monitorInterval   = 10 * time.Second
)

type peerDiscovery struct {
	client *http.Client // HTTP client

	sendConn *net.UDPConn // UDP peerdection for sending heartbeats
	recvConn *net.UDPConn // UDP peerdection for receiving heartbeats

	ctx    context.Context    // Context for the Vitarit struct
	cancel context.CancelFunc // Cancel function for the Vitarit struct
}

// start listens for nodes on the network and adds them to the cache
func (cache *distributedCache) startDiscovery(node nodeInfo) {
	logMessage(LOG_DEBUG, "start node discovery")

	err := cache.setupMulticastUDP(multicastAddress)
	if err != nil {
		fmt.Println("Error setting up multicast UDP:", err)
		return
	}

	// Create context to stop the peer discovery
	cache.ctx, cache.cancel = context.WithCancel(context.Background())

	go cache.monitorHeartbeats(node.ID)
	go cache.sendHeartbeats(node)
	go cache.receiveHeartbeats(node.ID, node.GroupID)
}

// stop peer discovery and close the peerdections
func (cache *distributedCache) stopDiscovery() error {
	// Stop all threads
	cache.cancel()

	// Close UDP connections used for heartbeat
	err1 := cache.sendConn.Close()
	err2 := cache.recvConn.Close()
	if err1 != nil {
		return err1
	}

	return err2
}

// -----------------------------------------------------------------------

func (cache *distributedCache) write(b []byte) (int, error) {
	return cache.sendConn.Write(b)
}

func (cache *distributedCache) readFromUDP(b []byte) (int, *net.UDPAddr, error) {
	return cache.recvConn.ReadFromUDP(b)
}

// -----------------------------------------------------------------------

// setupMulticastUDP sets up a multicast UDP peerdection
func (cache *distributedCache) setupMulticastUDP(address string) error {
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return err
	}

	// Use DialUDP for sending, and ListenMulticastUDP for receiving.
	// This is the CRUCIAL change.
	cache.sendConn, err = net.DialUDP("udp", nil, addr) // For sending
	if err != nil {
		return err
	}

	// For receiving, we need to bind to the multicast address.
	// For sending, we can use any available port (0 for automatic assignment).
	cache.recvConn, err = net.ListenMulticastUDP("udp", nil, addr)
	if err != nil {
		return err
	}

	pc := ipv4.NewPacketConn(cache.recvConn)
	err = pc.SetMulticastLoopback(true)
	if err != nil {
		logMessage(LOG_ERROR, "failed to set loopback on heartbeat receival pipe: "+err.Error())
		return err
	}

	return nil
}

// -----------------------------------------------------------------------

// sendHeartbeats sends heartbeats to the network
func (cache *distributedCache) sendHeartbeats(node nodeInfo) {
	logMessage(LOG_DEBUG, "start heartbest transmission")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	data, err := json.Marshal(node)
	if err != nil {
		logMessage(LOG_ERROR, "failed to marshal heartbeat message: "+err.Error())
		return
	}

	for {
		select {
		case <-cache.ctx.Done():
			return
		case <-ticker.C:
			_, err = cache.write(data)
			if err != nil {
				logMessage(LOG_ERROR, "failed to send heartbeat message from "+node.ID+": "+err.Error())
			}

			//logMessage(LOG_DEBUG, "sent heartbeat from "+node.ID)
		}
	}
}

// receiveHeartbeats listens for heartbeats from the network
func (cache *distributedCache) receiveHeartbeats(myNodeID string, myGroupID string) {
	var node nodeInfo
	buf := make([]byte, 1024)

	for {
		select {
		case <-cache.ctx.Done():
			return
		default:
			n, src, err := cache.readFromUDP(buf)
			if err != nil {
				logMessage(LOG_ERROR, "error reading from udp: "+err.Error())
				continue
			}

			err = json.Unmarshal(buf[:n], &node)
			if err != nil {
				logMessage(LOG_ERROR, "fail to parse heartbeat: "+err.Error())
				continue
			}

			if node.ID == myNodeID {
				// As loopback is enabled we might receive our own heartbeats as well
				continue
			}

			if node.GroupID == myGroupID {
				// This node does not belong to your group so ignore the HB
				continue
			}

			logMessage(LOG_DEBUG, "received heartbeat from "+node.ID+" IP: "+src.IP.String()+"Port: "+fmt.Sprint(src.Port))

			cache.addNode(node)
		}
	}

}

// -----------------------------------------------------------------------

// monitorHeartbeats monitors the heartbeats of the nodes in the cache
func (cache *distributedCache) monitorHeartbeats(myNodeID string) {
	logMessage(LOG_DEBUG, "start heartbest monitor")

	ticker := time.NewTicker(monitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cache.ctx.Done():
			return

		case <-ticker.C:
			now := time.Now()

			cache.mtx.Lock()
			for nodeID, lastSeen := range cache.nodeHB {
				if nodeID != myNodeID {
					// If heartbeat is not received from a node for 10 seconds declare it out of ring
					if now.Sub(lastSeen) > monitorInterval {
						logMessage(LOG_DEBUG, "**********   no HB from node, removing "+nodeID)
						cache.removeNode_unlocked(nodeID)
					}
				}
			}
			cache.mtx.Unlock()
		}
	}
}
