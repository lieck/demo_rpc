package client

import (
	"context"
	"fmt"
	"geerpc/server"
	"net"
	"strings"
	"testing"
	"time"
)

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func TestClient_diaTimeout(t *testing.T) {
	t.Parallel()
	l, _ := net.Listen("tcp", "127.0.0.1:0")

	f := func(conn net.Conn, opt *server.Option) (client *Client, err error) {
		_ = conn.Close()
		time.Sleep(time.Second * 2)
		return nil, nil
	}

	t.Run("timeout", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &server.Option{ConnectTimeout: time.Second})
		_assert(err != nil && strings.Contains(err.Error(), "connect timeout"), "expect a timeout error")
	})
	t.Run("0", func(t *testing.T) {
		_, err := dialTimeout(f, "tcp", l.Addr().String(), &server.Option{ConnectTimeout: 0})
		_assert(err == nil, "0 means no limit")
	})
}

type Bar int

func (b *Bar) Timeout(argv int, reply *int) error {
	time.Sleep(time.Second * 5)
	return nil
}

func startServer(addr chan string) {
	var b Bar
	_ = server.Register(&b)
	// pick a free port
	l, _ := net.Listen("tcp", ":0")
	addr <- l.Addr().String()
	server.Accept(l)
}

func TestClient_Call(t *testing.T) {
	t.Parallel()
	addrCh := make(chan string)
	go startServer(addrCh)
	addr := <-addrCh
	time.Sleep(time.Second)
	t.Run("client timeout", func(t *testing.T) {
		server.DefaultOption.ConnectTimeout = time.Second
		client, _ := Dial("tcp", addr, server.DefaultOption)
		ctx, _ := context.WithTimeout(context.Background(), time.Second*1)
		var reply int
		err := client.Call(ctx, "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), ctx.Err().Error()), "expect a timeout error")
	})

	t.Run("server handle timeout", func(t *testing.T) {
		server.DefaultOption.HandleTimeout = time.Second
		server.DefaultOption.ConnectTimeout = 0
		c, _ := Dial("tcp", addr, server.DefaultOption)
		var reply int
		err := c.Call(context.Background(), "Bar.Timeout", 1, &reply)
		_assert(err != nil && strings.Contains(err.Error(), "handle timeout"), "expect a timeout error")
	})
}
