package byor

import "net"

type Server interface {
	Serve()
	Shutdown()
	Call(id int, serviceMethod string, args interface{}, reply interface{}) error
	DisconnectAll()
	GetListenAddr() net.Addr
	ConnectToPeer(peerId int, addr net.Addr) error
	DisconnectPeer(peerId int) error
	Report() (id int, term int, isLeader bool)
}
