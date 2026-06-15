package main

import (
	"context"
	"errors"
	"fmt"
	"high-performance-kv-store/internal/api"
	"high-performance-kv-store/internal/cluster"
	"high-performance-kv-store/internal/config"
	"high-performance-kv-store/internal/metrics"
	"high-performance-kv-store/internal/store"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("Starting Antigravity KV Server...")

	// 1. Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	log.Printf("Config loaded. NodeID: %s, Port: %d, DataDir: %s, SeedAddr: %s",
		cfg.NodeID, cfg.Port, cfg.DataDir, cfg.SeedAddress)

	// 2. Initialize In-memory Store (WAL initially nil for replay safety)
	s := store.NewStore(nil)

	// 3. Load Snapshot if it exists
	snapPath := filepath.Join(cfg.DataDir, "snapshot.bin")
	loadedSnap, err := s.LoadSnapshot(snapPath)
	if err != nil {
		log.Fatalf("Failed to load snapshot: %v", err)
	}
	if loadedSnap {
		log.Printf("Loaded snapshot from %s", snapPath)
	}

	// 4. Initialize WAL and replay records
	wal, err := store.NewWAL(cfg.DataDir, "wal.log")
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %v", err)
	}

	log.Println("Replaying WAL...")
	err = wal.Replay(func(recType byte, key string, value []byte, version uint64, expiresAt time.Time) error {
		if recType == store.RecordTypeSet {
			var ttl time.Duration
			if !expiresAt.IsZero() {
				ttl = time.Until(expiresAt)
				if ttl <= 0 {
					return nil // Skip expired records
				}
			}
			_, err := s.Set(key, value, ttl, version)
			return err
		} else if recType == store.RecordTypeDelete {
			_, _, err := s.Delete(key, version)
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to replay WAL: %v", err)
	}
	log.Printf("WAL replay completed. Store size: %d keys", s.Size())

	// 5. Link the WAL to the store for future writes
	s.SetWAL(wal)

	// 6. Start background TTL reaper
	s.StartReaper(cfg.ReapInterval)
	log.Printf("TTL Reaper started with interval %v", cfg.ReapInterval)

	// 7. Setup clustering, membership (Gossip) and replication
	pm := cluster.NewPeerManager()
	gossip := cluster.NewGossipManager(cfg.NodeID, cfg.AdvertiseAddr, cfg.SeedAddress, cfg.GossipInterval, cfg.PingTimeout, cfg.SuspicionWindow, pm)
	replicator := cluster.NewReplicator(cfg.NodeID, pm, cfg.ReplicationTimeout)

	gossip.Start()
	log.Println("Gossip clustering service started")

	// 8. Hand-rolled metrics setup
	mc := metrics.NewMetricsCollector(
		s.Size,
		wal.Size,
		func() map[string]int {
			counts := map[string]int{
				cluster.StateAlive:   0,
				cluster.StateSuspect: 0,
				cluster.StateDead:    0,
			}
			for _, p := range pm.GetAll() {
				counts[p.State]++
			}
			return counts
		},
	)

	// 9. Periodic Snapshotting & Compaction goroutine (e.g. every 2 minutes for demo/safety)
	stopCompactor := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Println("Triggering periodic snapshot & log compaction...")
				if err := s.CreateSnapshot(); err != nil {
					log.Printf("ERROR creating periodic snapshot: %v", err)
				} else {
					log.Printf("Periodic snapshot created successfully. New WAL size: %d", wal.Size())
				}
			case <-stopCompactor:
				return
			}
		}
	}()

	// 10. Start HTTP API
	apiInstance := api.NewAPI(s, pm, gossip, replicator, mc, cfg)
	handler := api.NewHandler(apiInstance)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Port),
		Handler: handler,
	}

	go func() {
		log.Printf("HTTP Server listening on :%d", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP Server failed: %v", err)
		}
	}()

	// 11. Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server gracefully...")

	// Stop clustering & compactor
	close(stopCompactor)
	gossip.Stop()

	// Shutdown HTTP Server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP Server forced to shutdown: %v", err)
	}

	// Final snapshot to preserve state on clean shutdown
	log.Println("Creating final shutdown snapshot...")
	if err := s.CreateSnapshot(); err != nil {
		log.Printf("ERROR creating final shutdown snapshot: %v", err)
	}

	// Close store and WAL files
	if err := s.Close(); err != nil {
		log.Printf("Error closing store: %v", err)
	}

	log.Println("Server stopped cleanly.")
}
