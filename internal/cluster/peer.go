package cluster

import (
	"sync"
	"time"
)

const (
	StateAlive   = "alive"
	StateSuspect = "suspect"
	StateDead    = "dead"
)

type Peer struct {
	NodeID         string        `json:"node_id"`
	Address        string        `json:"address"`
	State          string        `json:"state"`
	LastSeen       time.Time     `json:"last_seen"`
	RTT            time.Duration `json:"rtt_ms"` // in milliseconds for JSON representation
	ReplicationLag int64         `json:"replication_lag"`
	SuspiciousAt   time.Time     `json:"-"` // Time when marked suspect
}

type PeerManager struct {
	mu    sync.RWMutex
	peers map[string]*Peer
}

func NewPeerManager() *PeerManager {
	return &PeerManager{
		peers: make(map[string]*Peer),
	}
}

func (pm *PeerManager) AddOrUpdate(nodeID, address, state string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	p, exists := pm.peers[nodeID]
	if !exists {
		p = &Peer{
			NodeID:  nodeID,
			Address: address,
		}
		pm.peers[nodeID] = p
	}

	p.State = state
	p.LastSeen = time.Now()
	if state == StateSuspect {
		p.SuspiciousAt = time.Now()
	}
}

func (pm *PeerManager) Get(nodeID string) (Peer, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	p, exists := pm.peers[nodeID]
	if !exists {
		return Peer{}, false
	}
	return *p, true
}

func (pm *PeerManager) GetAll() []Peer {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	list := make([]Peer, 0, len(pm.peers))
	for _, p := range pm.peers {
		list = append(list, *p)
	}
	return list
}

func (pm *PeerManager) UpdateRTT(nodeID string, rtt time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if p, exists := pm.peers[nodeID]; exists {
		p.RTT = rtt
		p.LastSeen = time.Now()
	}
}

func (pm *PeerManager) UpdateLag(nodeID string, lag int64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if p, exists := pm.peers[nodeID]; exists {
		p.ReplicationLag = lag
	}
}
