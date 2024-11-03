package main

import (
	"fmt"
	"net"
	"time"
	"raftLib/raft"
)

func main() {
	// Define server IDs and corresponding ports
	serverPorts := map[int]int{
		1: 8081,
		2: 8082,
		3: 8083,
	}

	// Initialize independent storage for each server
	storage1 := raft.NewMapStorage()
	storage2 := raft.NewMapStorage()
	storage3 := raft.NewMapStorage()

	// Create independent commit and ready channels for each server
	commitChan1 := make(chan raft.CommitEntry)
	commitChan2 := make(chan raft.CommitEntry)
	commitChan3 := make(chan raft.CommitEntry)

	readyChan1 := make(chan any)
	readyChan2 := make(chan any)
	readyChan3 := make(chan any)

	// Initialize three independent Raft servers
	server1 := raft.NewServer(1, []int{2, 3}, serverPorts, storage1, readyChan1, commitChan1, serverPorts[1])
	server2 := raft.NewServer(2, []int{1, 3}, serverPorts, storage2, readyChan2, commitChan2, serverPorts[2])
	server3 := raft.NewServer(3, []int{1, 2}, serverPorts, storage3, readyChan3, commitChan3, serverPorts[3])

	// Start serving the servers
	go server1.Serve()
	go server2.Serve()
	go server3.Serve()

	// Signal readiness
	readyChan1 <- struct{}{}
	readyChan2 <- struct{}{}
	readyChan3 <- struct{}{}

	// Connect the servers as peers dynamically using the serverPorts map
	server1.ConnectToPeer(2, &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPorts[2]})
	server1.ConnectToPeer(3, &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPorts[3]})
	server2.ConnectToPeer(1, &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPorts[1]})
	server2.ConnectToPeer(3, &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPorts[3]})
	server3.ConnectToPeer(1, &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPorts[1]})
	server3.ConnectToPeer(2, &net.TCPAddr{IP: net.ParseIP("localhost"), Port: serverPorts[2]})

	// Periodically check for the leader and send AppendEntries requests
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
    
	go func() {
		i := 0 
		for range ticker.C {
			i++
			checkAndAppendEntries(server1, i)
			checkAndAppendEntries(server2, i)
			checkAndAppendEntries(server3, i)
		}
	}()

	// Keep the main goroutine alive to allow servers to run
	select {} // Block forever
}

func checkAndAppendEntries(server *raft.Server,i int) {
	if server.IsLeader() {
		fmt.Printf("Server %d is the leader. Sending AppendEntries to peers...\n", i) 
		// Example log entry to append
        server.Submit(i)
		fmt.Println(i) 

	}
}
