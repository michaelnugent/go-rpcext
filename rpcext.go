package rpcext

import (
	"errors"
	"expvar"
	"net/rpc"
	"sync"

	"github.com/samuel/go-metrics/metrics"
)

type ClientBuilder interface {
	NewClient() (*rpc.Client, error)
}

type RPCExt struct {
	name               string
	mu                 sync.RWMutex // protects clients and closed
	closed             bool
	clients            []*rpc.Client
	maxIdleConnections int
	clientBuilder      ClientBuilder

	statRequests               *metrics.Counter
	statEstablishedConnections *metrics.Counter
	statLiveConnections        *metrics.Counter
}

func NewRPCExt(name string, maxIdleConnections int, clientBuilder ClientBuilder) *RPCExt {
	r := &RPCExt{
		name:               name,
		clients:            make([]*rpc.Client, 0, maxIdleConnections),
		maxIdleConnections: maxIdleConnections,
		clientBuilder:      clientBuilder,
		closed:             false,

		statRequests:               metrics.NewCounter(),
		statEstablishedConnections: metrics.NewCounter(),
		statLiveConnections:        metrics.NewCounter(),
	}

	m := expvar.NewMap(name + "-rpc")
	m.Set("requests", r.statRequests)
	m.Set("connections.established", r.statEstablishedConnections)
	m.Set("connections.inuse", r.statLiveConnections)
	m.Set("connections.idle", expvar.Func(func() interface{} {
		r.mu.RLock()
		n := len(r.clients)
		r.mu.RUnlock()
		return n
	}))

	return r
}

// conn returns a cached or newly-opened *rpc.Client
func (r *RPCExt) conn() (*rpc.Client, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("rpcext: database is closed")
	}
	if n := len(r.clients); n > 0 {
		c := r.clients[n-1]
		r.clients = r.clients[:n-1]
		return c, nil
	}
	r.statEstablishedConnections.Inc(1)
	return r.clientBuilder.NewClient()
}

// putConn adds a connection to the free pool
func (r *RPCExt) putConn(c *rpc.Client) {
	r.mu.Lock()
	if n := len(r.clients); !r.closed && n < r.maxIdleConnections {
		r.clients = append(r.clients, c)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	c.Close()
}

func (r *RPCExt) Call(serviceMethod string, args interface{}, reply interface{}) error {
	c, err := r.conn()
	if err != nil {
		return err
	}
	r.statLiveConnections.Inc(1)
	r.statRequests.Inc(1)
	err = c.Call(serviceMethod, args, reply)
	r.statLiveConnections.Dec(1)
	if err != nil {
		c.Close()
		return err
	}
	r.putConn(c)
	return nil
}

func (r *RPCExt) Go(serviceMethod string, args interface{}, reply interface{}, done chan *rpc.Call) *rpc.Call {
	if done == nil {
		panic("rpcext: method Go requires a non-nil done chan")
	}
	c, err := r.conn()
	if err != nil {
		call := &rpc.Call{
			ServiceMethod: serviceMethod,
			Args:          args,
			Reply:         reply,
			Error:         err,
			Done:          done,
		}
		done <- call
		return call
	}
	r.statLiveConnections.Inc(1)
	d := make(chan *rpc.Call, 1)
	go func() {
		call := <-d
		call.Done = done
		r.statLiveConnections.Dec(1)
		if call.Error != nil {
			c.Close()
		} else {
			r.putConn(c)
		}
		done <- call
	}()
	r.statRequests.Inc(1)
	return c.Go(serviceMethod, args, reply, d)
}

func (r *RPCExt) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	for _, c := range r.clients {
		err1 := c.Close()
		if err1 != nil {
			err = err1
		}
	}
	r.clients = nil
	r.closed = true
	return err
}
