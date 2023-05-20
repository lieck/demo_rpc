package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	RandomSelect     SelectMode = iota // select randomly
	RoundRobinSelect                   // select using Robbin algorithm
)

type Discovery interface {
	Refresh() error // refresh from remote registry
	Update(servers []string) error
	Get(mode SelectMode) (string, error)
	GetAll() ([]string, error)
}

type MultiServersDiscovery struct {
	r       *rand.Rand   // generate random number
	mu      sync.RWMutex // protect following
	servers []string
	index   int // record the selected position for robin algorithm
}

func NewMultiServersDiscovery(servers []string) *MultiServersDiscovery {
	m := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	m.index = m.r.Intn(math.MaxInt32 - 1)
	return m
}

func (m *MultiServersDiscovery) Refresh() error {
	//TODO implement me
	return nil
}

func (m *MultiServersDiscovery) Update(servers []string) error {
	m.mu.Lock()
	defer m.mu.RLock()
	m.servers = servers
	return nil
}

func (m *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	switch mode {
	case RandomSelect:
		return m.servers[m.r.Intn(len(m.servers))], nil
	case RoundRobinSelect:
		m.index = (m.index + 1) % len(m.servers)
		return m.servers[m.index], nil
	}

	return "", errors.New("invalid server")
}

func (m *MultiServersDiscovery) GetAll() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ret := make([]string, len(m.servers), len(m.servers))
	copy(ret, m.servers)
	return ret, nil
}
