package fdfs

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	MaxconnsLeast = 5
)

type pConn struct {
	net.Conn
	pool *connPool
}

func (c pConn) Close() error {
	return c.pool.put(c)
}

type connPool struct {
	conns    *list.List
	addr     string
	maxConns int
	count    int
	lock     *sync.RWMutex
	finish   chan bool
}

func newConnPool(addr string, maxConns int) (*connPool, error) {
	if maxConns < MaxconnsLeast {
		return nil, fmt.Errorf("too little maxConns < %d", MaxconnsLeast)
	}
	connPool := &connPool{
		conns:    list.New(),
		addr:     addr,
		maxConns: maxConns,
		lock:     &sync.RWMutex{},
		finish:   make(chan bool),
	}
	go func() {
		timer := time.NewTimer(time.Second * 20)
		for {
			select {
			case finish := <-connPool.finish:
				if finish {
					return
				}
			case <-timer.C:
				_ = connPool.CheckConns()
				timer.Reset(time.Second * 20)
			}
		}
	}()
	connPool.lock.Lock()
	defer connPool.lock.Unlock()
	for i := 0; i < MaxconnsLeast; i++ {
		if err := connPool.makeConn(); err != nil {
			return nil, err
		}
	}
	return connPool, nil
}

func (c *connPool) Destory() {
	if c == nil {
		return
	}
	c.finish <- true
}

func (c *connPool) CheckConns() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for e, next := c.conns.Front(), new(list.Element); e != nil; e = next {
		next = e.Next()
		conn := e.Value.(pConn)
		header := &header{
			cmd: ProtoCmdActiveTest,
		}
		if err := header.SendHeader(conn.Conn); err != nil {
			c.conns.Remove(e)
			c.count--
			continue
		}
		if err := header.RecvHeader(conn.Conn); err != nil {
			c.conns.Remove(e)
			c.count--
			continue
		}
		if header.cmd != TrackerProtoCmdResp || header.status != 0 {
			c.conns.Remove(e)
			c.count--
			continue
		}
	}
	return nil
}

func (c *connPool) makeConn() error {
	conn, err := net.DialTimeout("tcp", c.addr, time.Second*10)
	if err != nil {
		return err
	}
	c.conns.PushBack(pConn{
		Conn: conn,
		pool: c,
	})
	c.count++
	return nil
}

func (c *connPool) get() (net.Conn, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for {
		e := c.conns.Front()
		if e == nil {
			if c.count >= c.maxConns {
				return nil, fmt.Errorf("reach maxConns %d", c.maxConns)
			}
			_ = c.makeConn()
			continue
		}
		c.conns.Remove(e)
		conn := e.Value.(pConn)
		return conn, nil
	}
}

func (c *connPool) put(pConn pConn) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	pConn.pool.conns.PushBack(pConn)
	return nil
}
