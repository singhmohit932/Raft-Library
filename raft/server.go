package raft

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type Server struct {
	mu          sync.Mutex
	serverId    int
	peerIds     []int
	peerPorts   map[int]int // Store peer ports
	port        int
	rf          *Raft
	storage     Storage
	rpcServer   *rpc.Server
	listener    net.Listener
	commitChan  chan<- CommitEntry
	peerClients map[int]*rpc.Client
	ready       <-chan any
	quit        chan any
	wg          sync.WaitGroup
}

// NewServer initializes a new Server with a specified port.
func NewServer(serverId int, peerIds []int, peerPorts map[int]int, storage Storage, ready <-chan any, commitChan chan<- CommitEntry, port int) *Server {
	s := new(Server)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerPorts = peerPorts // Initialize peer ports
	s.peerClients = make(map[int]*rpc.Client)
	s.storage = storage
	s.ready = ready
	s.commitChan = commitChan
	s.quit = make(chan any)
	s.port = port
	return s
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.rf = NewRaft(s.serverId, s.peerIds, s, s.storage, s.ready, s.commitChan)

	// Create a new RPC server and register the Raft methods directly.
	s.rpcServer = rpc.NewServer()
	s.rpcServer.RegisterName("Raft", s)

	// Use the specified port for the listener.
	addr := fmt.Sprintf(":%d", s.port)
	var err error
	s.listener, err = net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[%v] listening at %s", s.serverId, s.listener.Addr())
	fmt.Println(s.peerIds)
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {

			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error:", err)
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

func (s *Server) Submit(rfd any) bool {
	return s.rf.Submit(rfd)
}

func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peerClients {
		if s.peerClients[id] != nil {
			s.peerClients[id].Close()
			s.peerClients[id] = nil
		}
	}
}

func (s *Server) Shutdown() {
	s.rf.Stop()
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// ConnectToPeer connects to a peer server and stores its port.
func (s *Server) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Store the peer port
	s.peerPorts[peerId] = addr.(*net.TCPAddr).Port

	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *Server) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		delete(s.peerPorts, peerId) // Optionally remove the port when disconnecting
		return err
	}
	return nil
}

// RequestVote is an RPC method for requesting votes from peers.
func (s *Server) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	// Directly call the Raft instance's RequestVote method
	return s.rf.RequestVote(args, reply)
}

// AppendEntries is an RPC method for appending entries to peers.
func (s *Server) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	// Directly call the Raft instance's AppendEntries method
	return s.rf.AppendEntries(args, reply)
}

// Call makes a direct RPC call to a peer server's method.
func (s *Server) Call(id int, serviceMethod string, args any, reply any) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("call client %d after it's closed", id)
	}

	switch serviceMethod {
	case "Raft.RequestVote":
		return peer.Call("Raft.RequestVote", args, reply)
	case "Raft.AppendEntries":
		return peer.Call("Raft.AppendEntries", args, reply)
	default:
		return fmt.Errorf("unknown service method: %s", serviceMethod)
	}
}

func (s *Server) IsLeader() bool {
	_, _, isLeader := s.rf.Report()
	return isLeader
}
