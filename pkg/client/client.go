package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrBadRequest  = errors.New("bad request")
	ErrInternal    = errors.New("internal server error")
)

type Client struct {
	addr       string
	httpClient *http.Client
}

type ClusterStatus struct {
	NodeID       string  `json:"node_id"`
	Role         string  `json:"role"`
	UptimeSec    float64 `json:"uptime_seconds"`
	StoreSize    int64   `json:"store_size"`
	WALSizeBytes int64   `json:"wal_size_bytes"`
	PeersCount   int     `json:"peers_count"`
}

type PutRequest struct {
	Value      string `json:"value"`
	TTLSeconds *int   `json:"ttl_seconds,omitempty"`
}

type PutResponse struct {
	Version uint64 `json:"version"`
}

type GetResponse struct {
	Value   string `json:"value"`
	Version uint64 `json:"version"`
}

type DeleteResponse struct {
	Version uint64 `json:"version"`
	Deleted bool   `json:"deleted"`
}

type KeysResponse struct {
	Keys       []string `json:"keys"`
	NextCursor string   `json:"next_cursor"`
}

func NewClient(addr string) *Client {
	return &Client{
		addr:       addr,
		httpClient: http.DefaultClient,
	}
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, uint64, error) {
	u := fmt.Sprintf("%s/v1/kv/%s", c.addr, url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, 0, ErrKeyNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("unexpected status: %s (code %d)", resp.Status, resp.StatusCode)
	}

	var res GetResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, 0, err
	}

	return []byte(res.Value), res.Version, nil
}

func (c *Client) Set(ctx context.Context, key string, value []byte, ttlSeconds int) (uint64, error) {
	u := fmt.Sprintf("%s/v1/kv/%s", c.addr, url.PathEscape(key))

	reqBody := PutRequest{
		Value: string(value),
	}
	if ttlSeconds > 0 {
		reqBody.TTLSeconds = &ttlSeconds
	}

	jsonBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", u, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusBadRequest {
		return 0, ErrBadRequest
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status: %s (code %d)", resp.Status, resp.StatusCode)
	}

	var res PutResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, err
	}

	return res.Version, nil
}

func (c *Client) Delete(ctx context.Context, key string) (uint64, bool, error) {
	u := fmt.Sprintf("%s/v1/kv/%s", c.addr, url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "DELETE", u, nil)
	if err != nil {
		return 0, false, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("unexpected status: %s (code %d)", resp.Status, resp.StatusCode)
	}

	var res DeleteResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, false, err
	}

	return res.Version, res.Deleted, nil
}

func (c *Client) ListKeys(ctx context.Context, prefix string, limit int, cursor string) ([]string, string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/v1/keys", c.addr))
	if err != nil {
		return nil, "", err
	}

	q := u.Query()
	if prefix != "" {
		q.Set("prefix", prefix)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("unexpected status: %s (code %d)", resp.Status, resp.StatusCode)
	}

	var res KeysResponse
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, "", err
	}

	return res.Keys, res.NextCursor, nil
}

func (c *Client) ClusterStatus(ctx context.Context) (*ClusterStatus, error) {
	u := fmt.Sprintf("%s/v1/cluster/status", c.addr)
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s (code %d)", resp.Status, resp.StatusCode)
	}

	var status ClusterStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}

	return &status, nil
}
