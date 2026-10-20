package wiring

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"time"

	"github.com/xiarsx/egress/internal/core/view"
	"github.com/xiarsx/egress/internal/ports"
)

// fetchTimeout bounds one subscription/GeoIP download (the syncer applies
// its own context timeout as well; this is the transport-level backstop).
const fetchTimeout = 30 * time.Second

// maxFetchBytes caps a single download in memory (subscriptions and GeoIP
// DBs; design §5.1: callers bound size via ctx — this is the backstop).
const maxFetchBytes = 64 << 20

// Compile-time check: Fetcher is the ports contract the source syncer and
// the GeoIP refresher consume.
var _ ports.Fetcher = (*Fetcher)(nil)

// Fetcher is the production ports.Fetcher (design §5.1):
// Direct ⊕ ViaDefaultPlatform — on direct failure the download is retried
// through the Default platform's view, so a gateway behind a restrictive
// network can still refresh its own subscriptions. Fallback is nil until
// the assembly root finishes wiring the data plane.
type Fetcher struct {
	Client *http.Client
	// Fallback retries failed direct downloads through the Default
	// platform (nil = direct-only).
	Fallback *http.Client
}

// NewFetcher builds a Fetcher with a 30s client timeout. A custom client
// may be injected via the Client field.
func NewFetcher() *Fetcher {
	return &Fetcher{
		Client: &http.Client{Timeout: fetchTimeout},
	}
}

// Fetch downloads url completely into memory.
func (f *Fetcher) Fetch(ctx context.Context, url string) ([]byte, error) {
	client := f.Client
	if client == nil {
		client = &http.Client{Timeout: fetchTimeout}
	}
	body, err := fetchWith(ctx, client, url)
	if err == nil || f.Fallback == nil {
		return body, err
	}
	fbBody, fbErr := fetchWith(ctx, f.Fallback, url)
	if fbErr == nil {
		return fbBody, nil
	}
	return nil, fmt.Errorf("fetch: %s: direct: %v; via default platform: %v", url, err, fbErr)
}

func fetchWith(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch: build request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch: GET %s: status %s", url, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFetchBytes+1))
	if err != nil {
		return nil, fmt.Errorf("fetch: read %s: %w", url, err)
	}
	if int64(len(body)) > maxFetchBytes {
		return nil, fmt.Errorf("fetch: %s exceeds %d bytes", url, maxFetchBytes)
	}
	return body, nil
}

// DialViaPlatform returns a DialContext routing through a randomly picked
// node of the platform view resolved per call (no lease semantics —
// fetches are cold-path one-shots). It fails when the view is empty,
// which simply means the fallback is unavailable.
func DialViaPlatform(viewOf func() *view.View, dialer ports.NodeDialer) func(ctx context.Context, network, address string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		v := viewOf()
		if v == nil || v.Size() == 0 {
			return nil, errors.New("fetch: default platform has no routable nodes")
		}
		rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
		h, ok := v.RandomPick(rng)
		if !ok {
			return nil, errors.New("fetch: default platform view pick failed")
		}
		return dialer.DialContext(ctx, h, network, address)
	}
}
