package vitarit

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	id    = flag.String("id", "node1", "ID of this node")
	ip    = flag.String("ip", "127.0.0.1", "IP address of the server")
	port  = flag.String("port", "8081", "Port of the server")
	group = flag.String("group", "X", "Group id of the cache group")
)

func TestMain(m *testing.M) {
	// Parse the flags
	flag.Parse()
	// Run the tests
	os.Exit(m.Run())
}

func TestVitarit(t *testing.T) {
	flag.Parse()

	t.Logf("Selected IP : %s, Port %s, ID: %s", *ip, *port, *id)

	vitarit := NewVitarit(*id, *ip, *port, *group)
	defer vitarit.Stop()

	vitarit.SetLogger(func(level int, message string) {
		t.Logf("Level: %d, Message: %s", level, message)
	})

	vitarit.Start(0)

	// Example operations
	vitarit.Set("key1", []byte{0, 1, 2, 3, 4})
	vitarit.Set("key2", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	vitarit.Set("key3", []byte{0, 9})

	value, exists := vitarit.Get("key1")
	if exists {
		t.Logf("key1: %v", value)
	} else {
		t.Logf("key1 not found")
	}

	value, exists = vitarit.Get("key2")
	if exists {
		t.Logf("key2: %v", value)
	} else {
		t.Logf("key2 not found")
	}

	value, exists = vitarit.Get("key3")
	if exists {
		t.Logf("key3: %v", value)
	} else {
		t.Logf("key3 not found")
	}
}

func TestPeerDiscovery(t *testing.T) {
	flag.Parse()

	t.Logf("Selected IP : %s, Port %s, ID: %s", *ip, *port, *id)

	vitarit := NewVitarit(*id, *ip, *port, *group)
	vitarit.SetLogger(func(level int, message string) {
		t.Logf("Level: %d, Message: %s", level, message)
	})

	vitarit.Start(0)
	defer vitarit.Stop()

	// Wait for http server of the node to start
	time.Sleep(20 * time.Minute)
}

func TestKeyDistribution(t *testing.T) {
	flag.Parse()

	t.Logf("Selected IP : %s, Port %s, ID: %s", *ip, *port, *id)

	loglocal := func(level int, message string) {
		t.Logf("Level: %d, Message: %s", level, message)
	}

	vitarit1 := NewVitarit("node1", "127.0.0.1", "8081", *group)
	vitarit1.SetLogger(loglocal)

	vitarit2 := NewVitarit("node2", "127.0.0.1", "8082", *group)
	vitarit2.SetLogger(loglocal)

	vitarit3 := NewVitarit("node3", "127.0.0.1", "8083", *group)
	vitarit3.SetLogger(loglocal)

	vitarit1.Start(1)
	vitarit2.Start(1)
	vitarit3.Start(1)

	defer vitarit1.Stop()
	defer vitarit2.Stop()
	defer vitarit3.Stop()

	// Wait for http server of the node to start
	time.Sleep(20 * time.Second)

	// Example operations
	vitarit1.Set("key1", []byte{0, 1, 2, 3, 4})
	vitarit1.Set("key2", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	vitarit1.Set("key3", []byte{0, 9})

	value, exists := vitarit1.Get("key1")
	if exists {
		t.Logf("key1: %v", value)
	} else {
		t.Logf("key1 not found")
	}

	value, exists = vitarit1.Get("key2")
	if exists {
		t.Logf("key2: %v", value)
	} else {
		t.Logf("key2 not found")
	}

	value, exists = vitarit1.Get("key3")
	if exists {
		t.Logf("key3: %v", value)
	} else {
		t.Logf("key3 not found")
	}
}

func TestPeerGrouping(t *testing.T) {
	flag.Parse()

	t.Logf("Selected IP : %s, Port %s, ID: %s", *ip, *port, *id)

	loglocal := func(level int, message string) {
		t.Logf("Level: %d, Message: %s", level, message)
	}

	vitarit1 := NewVitarit("node0", "127.0.0.1", "8081", "A")
	vitarit1.SetLogger(loglocal)
	vitarit1.Start(1)

	for i := 1; i <= 3; i++ {
		vitaritx := NewVitarit(fmt.Sprintf("node%d", i), "127.0.0.1", fmt.Sprintf("%d", 8082+i), "A")
		vitaritx.SetLogger(loglocal)
		vitaritx.Start(1)
	}

	vitarit2 := NewVitarit("node10", "127.0.0.1", "8091", "B")
	vitarit2.SetLogger(loglocal)
	vitarit2.Start(1)

	for i := 1; i <= 5; i++ {
		vitaritx := NewVitarit(fmt.Sprintf("node%d", 10+i), "127.0.0.1", fmt.Sprintf("%d", 8092+i), "B")
		vitaritx.SetLogger(loglocal)
		vitaritx.Start(1)
	}

	// Wait for http server of the node to start
	time.Sleep(30 * time.Second)

	peers := vitarit1.GetPeers()
	t.Logf("Peers of vitarit1: %v", peers)

	peers = vitarit2.GetPeers()
	t.Logf("Peers of vitarit3: %v", peers)
}

func TestRedundancy(t *testing.T) {
	flag.Parse()

	t.Logf("Selected IP : %s, Port %s, ID: %s", *ip, *port, *id)

	loglocal := func(level int, message string) {
		t.Logf("Level: %d, Message: %s", level, message)
	}

	vitarit1 := NewVitarit("node1", "127.0.0.1", "8081", "A")
	vitarit1.SetLogger(loglocal)
	vitarit1.Start(2)
	defer vitarit1.Stop()

	vitarit2 := NewVitarit("node2", "127.0.0.1", "8082", "A")
	vitarit2.SetLogger(loglocal)
	vitarit2.Start(2)
	defer vitarit2.Stop()

	vitarit3 := NewVitarit("node3", "127.0.0.1", "8083", "A")
	vitarit3.SetLogger(loglocal)
	vitarit3.Start(2)
	defer vitarit3.Stop()

	time.Sleep(20 * time.Second)

	value, exists := vitarit1.Get("key1")
	if exists {
		t.Logf("key1: %v", value)
	} else {
		t.Logf("key1 not found")
	}

	vitarit1.Set("key1", []byte{0, 1, 2, 3, 4})
	vitarit1.Set("key2", []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})

	value, exists = vitarit1.Get("key1")
	if exists {
		t.Logf("key1: %v", value)
	} else {
		t.Logf("key1 not found")
	}
}
