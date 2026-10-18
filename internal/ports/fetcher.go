package ports

import "context"

// Fetcher downloads a URL completely into memory: subscriptions, GeoIP
// databases, recipes. Callers bound size and time via ctx; the returned
// bytes are the raw response body.
//
// Composition (design §5.1): the production Fetcher is
// Direct ⊕ ViaDefaultPlatform — it first attempts a direct dial and, on
// failure, retries the download routed through the Default platform's
// view. The fallback exists so a gateway behind a restrictive network can
// still refresh its own subscriptions and GeoIP data. Composition lives in
// the adapter layer; this interface stays a single pure fetch so tests can
// stub it with canned bytes.
type Fetcher interface {
	Fetch(ctx context.Context, url string) ([]byte, error)
}
