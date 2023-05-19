package geerpc

import (
	"encoding/json"
	"errors"
	"fmt"
	"geerpc/codec"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

const (
	connected        = "200 connected to Gee RPC"
	defaultRPCPath   = "/_geeprc_"
	defaultDebugPath = "/debug/geerpc"
)

type Option struct {
	MagicNumber    int
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

type Request struct {
	H          *codec.Header
	Arg, Reply reflect.Value
	mtype      *methodType
	svc        *service
}

type Server struct {
	serviceMap sync.Map
}

func (s *Server) Register(rcvr interface{}) error {
	service := newService(rcvr)
	if _, dup := s.serviceMap.LoadOrStore(service.name, service); dup {
		return errors.New("rpc: service already defined: " + service.name)
	}
	return nil
}

// Accept 监听连接处理
func (s *Server) Accept(list net.Listener) {
	for {
		conn, err := list.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

// 处理连接
func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		log.Printf("[server] conn close %s", conn.RemoteAddr().String())
		_ = conn.Close()
	}()

	opt := Option{}
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		return
	}

	if opt.MagicNumber != MagicNumber {
		return
	}

	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		return
	}

	s.serveCodec(f(conn), opt.HandleTimeout)
}

func (s *Server) serveCodec(f codec.Codec, timeout time.Duration) {
	sending := new(sync.Mutex)
	wg := new(sync.WaitGroup)

	for {
		req, err := s.readRequest(f)
		if err != nil {
			break
		}

		wg.Add(1)
		go s.handleRequest(f, req, sending, wg, timeout)
	}
	wg.Wait()
}

func (s *Server) readRequest(cc codec.Codec) (*Request, error) {
	// 读取 Header
	header := &codec.Header{}
	if err := cc.ReadHeader(header); err != nil {
		return nil, err
	}

	// 读取 request
	req := &Request{H: header}
	var err error
	req.svc, req.mtype, err = s.findService(header.ServiceMethod)
	if err != nil {
		return req, err
	}

	req.Arg = req.mtype.newArgv()
	req.Reply = req.mtype.newReply()

	args := req.Arg.Interface()
	if req.Arg.Kind() != reflect.Ptr {
		args = req.Arg.Addr().Interface()
	}
	if err := cc.ReadBody(args); err != nil {
		return req, err
	}

	return req, nil
}

func (s *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) error {
	sending.Lock()
	defer sending.Unlock()
	log.Println("Sending response")
	if err := cc.Write(h, body); err != nil {
		return err
	}

	return nil
}

func (s *Server) handleRequest(cc codec.Codec, req *Request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	log.Printf("[server] handle request seq:%v, %v\n", req.H.Seq, req.H.ServiceMethod)
	defer wg.Done()

	called := make(chan struct{})
	sent := make(chan struct{})

	go func() {
		err := req.svc.call(req.mtype, req.Arg, req.Reply)
		called <- struct{}{}
		if err != nil {
			req.H.Error = err.Error()
			_ = s.sendResponse(cc, req.H, nil, sending)
			sent <- struct{}{}
			return
		}
		_ = s.sendResponse(cc, req.H, req.Reply.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}

	select {
	case <-time.After(timeout):
		req.H.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		_ = s.sendResponse(cc, req.H, req.Reply, sending)
	case <-called:
		<-sent
	}

	_ = s.sendResponse(cc, req.H, req.Reply.Interface(), sending)
}

func (s *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot == -1 {
		return nil, nil, errors.New("rpc: invalid service method")
	}

	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := s.serviceMap.Load(serviceName)
	if !ok {
		return nil, nil, errors.New("rpc: service not found: " + serviceName)
	}

	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		return nil, nil, errors.New("rpc: method not found: " + serviceName + "." + methodName)
	}

	return svc, mtype, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	log.Printf("connection established")
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	s.handleConn(conn)
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}

func (s *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, s)
	http.Handle(defaultDebugPath, &debugHTTP{s})
	log.Println("rpc server debug path:", defaultDebugPath)
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func Accept(list net.Listener) {
	DefaultServer.Accept(list)
}

func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }
