package xclient

import (
	"context"
	"geerpc"
	"reflect"
	"sync"
)

type XClient struct {
	d       Discovery
	mode    SelectMode
	opt     *geerpc.Option
	mu      sync.Mutex // protect following
	clients map[string]*geerpc.Client
}

func NewXClient(d Discovery, mode SelectMode, opt *geerpc.Option) *XClient {
	if opt == nil {
		opt = geerpc.DefaultOption
	}
	return &XClient{d: d, mode: mode, opt: opt, clients: make(map[string]*geerpc.Client)}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	for _, client := range xc.clients {
		if err := client.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 根据 rpcAddr 返回 rpcClient
func (xc *XClient) dial(rpcAddr string) (*geerpc.Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()

	c, ok := xc.clients[rpcAddr]
	if ok && c.IsAvailable() {
		_ = c.Close()
		c = nil
	}

	if c == nil {
		var err error
		c, err = geerpc.XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = c
	}

	return c, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	c, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}

	return c.Call(ctx, serviceMethod, args, reply)
}

func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast invokes the named function for every server registered in discovery
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}

	var lock sync.Mutex
	var wg sync.WaitGroup

	var e error
	var replyDone bool
	ctx, cancel := context.WithCancel(ctx)

	for _, s := range servers {
		wg.Add(1)
		go func(s string) {
			defer wg.Done()

			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(s, ctx, serviceMethod, args, clonedReply)

			lock.Lock()
			defer lock.Unlock()

			if err != nil && e == nil {
				e = err
				cancel()
			}

			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
		}(s)
	}
	wg.Wait()
	return e
}
