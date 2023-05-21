package registry

import (
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultPath    = "/_geerpc_/registry"
	defaultTimeout = time.Minute * 5
)

type ServerItem struct {
	Addr  string
	start time.Time // 上次访问的时间
}

type GeeRegistry struct {
	timeout time.Duration
	mu      sync.Mutex // protect following
	servers map[string]*ServerItem
}

// NewGeeRegistry returns a new GeeRegistry
func NewGeeRegistry(timeout time.Duration) *GeeRegistry {
	return &GeeRegistry{
		timeout: timeout,
		mu:      sync.Mutex{},
		servers: make(map[string]*ServerItem),
	}
}

// 添加服务实例，如果服务已经存在，则更新 start
func (r *GeeRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	server, ok := r.servers[addr]
	if ok {
		server.start = time.Now()
	} else {
		r.servers[addr] = &ServerItem{
			Addr:  addr,
			start: time.Now(),
		}
	}
}

// 返回可用的服务列表，如果存在超时的服务，则删除
func (r *GeeRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	var aliveServers []string
	nowTime := time.Now()
	for _, server := range r.servers {
		if nowTime.Sub(server.start) >= r.timeout {
			delete(r.servers, server.Addr)
		} else {
			aliveServers = append(aliveServers, server.Addr)
		}
	}
	return aliveServers
}

// ServeHTTP 采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中
// Get：返回所有可用的服务列表，通过自定义字段 X-Geerpc-Servers 承载
// Post：添加服务实例或发送心跳，通过自定义字段 X-Geerpc-Server 承载
func (r *GeeRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("X-Geerpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		addr := req.Header.Get("X-Geerpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *GeeRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("[GeeRegistry.HandleHTTP] starting")
}

// Heartbeat 向服务中心发送心跳
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		duration = 1 * time.Minute
	}

	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// 发送心跳
func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}

var DefaultGeeRegister = NewGeeRegistry(defaultTimeout)

func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}
