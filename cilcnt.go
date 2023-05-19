package geerpc

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Call struct {
	Seq          uint64
	ServerMethod string

	Args  interface{}
	Reply interface{}
	Error error

	Done chan *Call // 调用结束时通知
}

type clientResult struct {
	client *Client
	err    error
}

func (call *Call) done() {
	call.Done <- call
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

type Client struct {
	cc  codec.Codec
	opt *Option

	sending sync.Mutex

	mu      sync.Mutex
	seq     uint64
	pending map[uint64]*Call

	closing  bool // user has called Close
	shutdown bool // server has told us to stop
}

// 注册 RPC
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()

	seq := client.seq
	client.seq += 1
	client.pending[seq] = call

	call.Seq = seq
	return seq, nil
}

// 移除对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()

	call, ok := client.pending[seq]
	if !ok {
		return nil
	}

	delete(client.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call。
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()

	client.mu.Lock()
	defer client.mu.Unlock()

	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 接收 RPC 响应
func (client *Client) receive() {
	for {
		if client.shutdown {
			return
		}

		header := &codec.Header{}
		err := client.cc.ReadHeader(header)
		if err != nil {
			client.terminateCalls(err)
			return
		}

		call := client.removeCall(header.Seq)
		switch {
		case call == nil:
			// 通常表示操作被移除或失败
			err = client.cc.ReadBody(nil)
		case header.Error != "":
			call.Error = fmt.Errorf(header.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
}

func (client *Client) send(call *Call) {
	client.sending.Lock()
	defer client.sending.Unlock()

	seq, _ := client.registerCall(call)

	err := client.cc.Write(&codec.Header{
		ServiceMethod: call.ServerMethod,
		Seq:           seq,
	}, call.Args)

	if err != nil {
		call := client.removeCall(seq)
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}

	call := &Call{
		Seq:          client.seq,
		ServerMethod: serviceMethod,
		Args:         args,
		Reply:        reply,
		Done:         done,
	}

	client.send(call)
	return call
}

func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	//call := <-client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))

	select {
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f, ok := codec.NewCodecFuncMap[opt.CodecType]
	if !ok {
		return nil, errors.New(string("unknown codec " + opt.CodecType))
	}

	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		return nil, err
	}

	client := &Client{
		cc:       f(conn),
		opt:      opt,
		sending:  sync.Mutex{},
		mu:       sync.Mutex{},
		seq:      0,
		pending:  make(map[uint64]*Call),
		closing:  false,
		shutdown: false,
	}

	go client.receive()

	return client, nil
}

func Dial(network, address string, opt *Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opt)
}

func dialTimeout(f newClientFunc, network, address string, opt *Option) (client *Client, err error) {
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	ch := make(chan clientResult)

	go func() {
		client, err = f(conn, opt)
		ch <- clientResult{client, err}
	}()

	// 不进行超时控制
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}

	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

func NewHTTPClient(conn net.Conn, opt *Option) (client *Client, err error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err != nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

func DialHTTP(network, address string, opts *Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts)
}

func XDial(rpcAddr string, opts *Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}

	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opts)
	}
}
