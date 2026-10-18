package wiring

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

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

// Fetcher is the production ports.Fetcher: plain direct HTTPS downloads
// with a 30s timeout.
//
// Composition (design §5.1): the full Fetcher is
// Direct ⊕ ViaDefaultPlatform — on direct failure the download is retried
// routed through the Default platform's view, so a gateway behind a
// restrictive network can still refresh its own subscriptions. That
// fallback needs the pipeline and the Default view, which only exist
// after the assembly root finishes wiring; it is a TODO here and the
// current implementation is direct-only.
type Fetcher struct {
	Client *http.Client
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
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch: build request: %w", err)
	}
	// TODO(design §5.1): ViaDefaultPlatform fallback — retry through the
	// Default platform's view when the direct dial fails. Needs the
	// pipeline + Default view handle; wire it in cmd/egress once the
	// pipeline exists at fetch time.
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
