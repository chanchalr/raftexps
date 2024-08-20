package byor

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type RPCServer struct {
	mu          sync.Mutex
	serverId    int
	peerIds     []int
	rpcProxy    *RPCProxy
	rpcServer   *rpc.Server
	listner     net.Listener
	peerClients map[int]*rpc.Client
	ready       <-chan interface{}
	quit        chan interface{}
	wg          sync.WaitGroup
	cm          ConsensusModule
}

type RPCProxy struct {
	cm ConsensusModule
}

func NewRPCServer(serverId int, peerIds []int, ready <-chan interface{}) *RPCServer {
	s := new(RPCServer)
	s.serverId = serverId
	s.peerIds = peerIds
	s.peerClients = make(map[int]*rpc.Client)
	s.ready = ready
	s.quit = make(chan interface{})
	return s
}

func (s *RPCServer) Serve() {
	s.mu.Lock()
	s.cm = NewGPConsensusModule(s.serverId, s.peerIds, s)
	s.cm.Start(s.ready)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{cm: s.cm}
	s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)
	var err error
	s.listner, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Server %d listening on %s", s.serverId, s.listner.Addr())
	s.mu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listner.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Println(err)
					continue
				}
			}
			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				defer s.wg.Done()
			}()

		}
	}()

}

func (s *RPCServer) Shutdown() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cm.Stop()
	close(s.quit)
	s.listner.Close()
	s.wg.Wait()
}

func (s *RPCServer) Call(id int, serviceMethod string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()
	if peer == nil {
		return fmt.Errorf("call client %+v after its closed", peer)
	} else {
		return peer.Call(serviceMethod, args, reply)
	}

}

func (s *RPCServer) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, client := range s.peerClients {
		client.Close()
	}
	s.peerClients = make(map[int]*rpc.Client)
}

func (s *RPCServer) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listner.Addr()
}

func (s *RPCServer) ConnectToPeer(peerId int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] == nil {
		client, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerId] = client
	}
	return nil
}

func (s *RPCServer) DisconnectPeer(peerId int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.peerClients[peerId] != nil {
		err := s.peerClients[peerId].Close()
		s.peerClients[peerId] = nil
		return err
	}
	return nil
}

func (s *RPCServer) Report() (id int, term int, isLeader bool) {
	return s.cm.Report()
}

func (rpp *RPCProxy) RequestVote(arg RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			log.Printf("dropping Requestvote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			log.Printf("delay Requestvote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.RequestVote(arg, reply)
}

func (rpp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			log.Printf("dropping AppendEntries")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			log.Printf("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
	}
	return rpp.cm.AppendEntries(args, reply)
}
