package cluster

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type GossipManager struct {
	nodeID          string
	address         string
	seedAddr        string
	gossipInterval  time.Duration
	pingTimeout     time.Duration
	suspicionWindow time.Duration
	pm              *PeerManager
	client          *http.Client
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

func NewGossipManager(nodeID, address, seedAddr string, gossipInterval, pingTimeout, suspicionWindow time.Duration, pm *PeerManager) *GossipManager {
	return &GossipManager{
		nodeID:          nodeID,
		address:         address,
		seedAddr:        seedAddr,
		gossipInterval:  gossipInterval,
		pingTimeout:     pingTimeout,
		suspicionWindow: suspicionWindow,
		pm:              pm,
		client: &http.Client{
			Timeout: pingTimeout,
		},
		stopChan: make(chan struct{}),
	}
}

func (gm *GossipManager) Start() {
	gm.wg.Add(2)
	go gm.gossipLoop()
	go gm.suspicionCheckLoop()

	if gm.seedAddr != "" {
		// Asynchronously bootstrap by pinging the seed node
		go gm.bootstrap()
	}
}

func (gm *GossipManager) Stop() {
	close(gm.stopChan)
	gm.wg.Wait()
}

func (gm *GossipManager) bootstrap() {
	// Add seed node as a temporary peer to bootstrap
	gm.pm.AddOrUpdate("seed", gm.seedAddr, StateAlive)
	gm.pingPeer(Peer{NodeID: "seed", Address: gm.seedAddr})
}

func (gm *GossipManager) gossipLoop() {
	defer gm.wg.Done()
	ticker := time.NewTicker(gm.gossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gm.tick()
		case <-gm.stopChan:
			return
		}
	}
}

func (gm *GossipManager) tick() {
	peers := gm.pm.GetAll()
	var candidates []Peer

	for _, p := range peers {
		// Do not ping ourselves, and do not ping dead nodes
		if p.NodeID == gm.nodeID || p.State == StateDead {
			continue
		}
		candidates = append(candidates, p)
	}

	if len(candidates) == 0 {
		return
	}

	// Select a random peer
	// Simple pseudo-random selection to avoid dependencies
	idx := int(time.Now().UnixNano()) % len(candidates)
	target := candidates[idx]

	go gm.pingPeer(target)
}

func (gm *GossipManager) pingPeer(p Peer) {
	start := time.Now()
	// Construct GET ping URL
	u := fmt.Sprintf("http://%s/internal/ping?sender_id=%s&sender_addr=%s",
		p.Address, url.QueryEscape(gm.nodeID), url.QueryEscape(gm.address))

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return
	}

	resp, err := gm.client.Do(req)
	rtt := time.Since(start)

	if err != nil {
		// Ping failed: mark suspect if currently alive
		if p.State == StateAlive || p.NodeID == "seed" {
			gm.pm.AddOrUpdate(p.NodeID, p.Address, StateSuspect)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		if p.State == StateAlive || p.NodeID == "seed" {
			gm.pm.AddOrUpdate(p.NodeID, p.Address, StateSuspect)
		}
		return
	}

	// Read peer list from response
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var remotePeers []Peer
	if err := json.Unmarshal(bodyBytes, &remotePeers); err != nil {
		return
	}

	// Determine actual NodeID of seed if we pinged it
	actualNodeID := p.NodeID
	if p.NodeID == "seed" {
		// Find the peer in the remote response list that matches the seed's own address
		for _, rp := range remotePeers {
			if rp.Address == p.Address {
				actualNodeID = rp.NodeID
				// Remove temporary seed key and add actual node
				gm.pm.mu.Lock()
				delete(gm.pm.peers, "seed")
				gm.pm.mu.Unlock()
				break
			}
		}
	}

	// Update the pinged peer's status
	gm.pm.AddOrUpdate(actualNodeID, p.Address, StateAlive)
	gm.pm.UpdateRTT(actualNodeID, rtt)

	// Merge remote peer list
	gm.MergePeers(remotePeers)
}

func (gm *GossipManager) MergePeers(remotePeers []Peer) {
	for _, rp := range remotePeers {
		if rp.NodeID == gm.nodeID {
			continue // Don't merge ourselves
		}

		localPeer, exists := gm.pm.Get(rp.NodeID)
		if !exists {
			// Add new peer
			if rp.State != StateDead {
				gm.pm.AddOrUpdate(rp.NodeID, rp.Address, rp.State)
			}
		} else {
			// Resolve state conflicts
			// State priority: dead > suspect > alive
			if localPeer.State == StateDead {
				// Dead is final, do nothing
				continue
			}

			if rp.State == StateDead {
				gm.pm.AddOrUpdate(rp.NodeID, rp.Address, StateDead)
			} else if rp.State == StateSuspect && localPeer.State == StateAlive {
				gm.pm.AddOrUpdate(rp.NodeID, rp.Address, StateSuspect)
			} else if rp.State == StateAlive && localPeer.State == StateSuspect {
				// Only let direct success or self-refutation clear suspect.
				// However, if the last seen from the remote is significantly newer, we can accept it.
				if rp.LastSeen.After(localPeer.LastSeen.Add(gm.suspicionWindow)) {
					gm.pm.AddOrUpdate(rp.NodeID, rp.Address, StateAlive)
				}
			}
		}
	}
}

func (gm *GossipManager) suspicionCheckLoop() {
	defer gm.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			gm.checkSuspicious()
		case <-gm.stopChan:
			return
		}
	}
}

func (gm *GossipManager) checkSuspicious() {
	peers := gm.pm.GetAll()
	now := time.Now()

	for _, p := range peers {
		if p.State == StateSuspect {
			if now.Sub(p.SuspiciousAt) >= gm.suspicionWindow {
				// Suspicion window expired, mark node dead
				gm.pm.AddOrUpdate(p.NodeID, p.Address, StateDead)
			}
		}
	}
}
