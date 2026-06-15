package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type ReplicateRequest struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Version   uint64 `json:"version"`
	ExpiresAt int64  `json:"expires_at"` // UnixNano
	Action    string `json:"action"`     // "SET" or "DELETE"
}

type Replicator struct {
	nodeID     string
	pm         *PeerManager
	client     *http.Client
	timeout    time.Duration
	lastServed uint64 // Tracks highest local version written
	mu         sync.Mutex
}

func NewReplicator(nodeID string, pm *PeerManager, timeout time.Duration) *Replicator {
	return &Replicator{
		nodeID: nodeID,
		pm:     pm,
		client: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}
}

// UpdateLastServed records the highest local version written.
func (r *Replicator) UpdateLastServed(version uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if version > r.lastServed {
		r.lastServed = version
	}
}

// Replicate propagates a SET/DELETE write to all active peers asynchronously.
func (r *Replicator) Replicate(key string, value []byte, version uint64, expiresAt time.Time, action string) {
	r.UpdateLastServed(version)

	peers := r.pm.GetAll()
	for _, p := range peers {
		if p.NodeID == r.nodeID || p.State == StateDead {
			continue
		}

		// Run replication in goroutines to make it async and non-blocking
		go r.replicateToPeer(p, key, value, version, expiresAt, action)
	}
}

func (r *Replicator) replicateToPeer(p Peer, key string, value []byte, version uint64, expiresAt time.Time, action string) {
	var expNano int64
	if !expiresAt.IsZero() {
		expNano = expiresAt.UnixNano()
	}

	reqPayload := ReplicateRequest{
		Key:       key,
		Value:     value,
		Version:   version,
		ExpiresAt: expNano,
		Action:    action,
	}

	bodyBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return
	}

	u := fmt.Sprintf("http://%s/internal/replicate?sender_id=%s", p.Address, r.nodeID)
	req, err := http.NewRequest("POST", u, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		// If replication fails, we update lag based on the difference
		r.mu.Lock()
		diff := int64(r.lastServed) - int64(version)
		r.mu.Unlock()
		r.pm.UpdateLag(p.NodeID, diff)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// Peer successfully processed it, update their lag.
		r.mu.Lock()
		lag := int64(r.lastServed) - int64(version)
		r.mu.Unlock()
		if lag < 0 {
			lag = 0
		}
		r.pm.UpdateLag(p.NodeID, lag)
	} else {
		// Non-OK status, estimate lag
		r.mu.Lock()
		diff := int64(r.lastServed) - int64(version)
		r.mu.Unlock()
		r.pm.UpdateLag(p.NodeID, diff)
	}
}
