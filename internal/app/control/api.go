package control

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// RuntimeBackend: runtime-owned reads/actions behind the control API
// ---------------------------------------------------------------------------

// ProbeKind selects the probe type for POST /nodes/{hash}/actions/probe-*.
type ProbeKind string

const (
	ProbeKindEgress  ProbeKind = "egress"
	ProbeKindLatency ProbeKind = "latency"
)

// RuntimeBackend supplies everything the control API does not own itself:
// nodes, leases, views, metrics, request logs and GeoIP. It is implemented
// by the runtime group (internal/app/runtime). While no backend is wired
// (see WithRuntimeBackend) those routes answer 501 NOT_IMPLEMENTED in the
// standard error envelope.
//
// The runtime backend surface is segregated by domain (ISP): handlers
// depend only on their own slice, and tests stub only what they exercise.
// Backend methods should return an error wrapping ports.ErrNotFound when the
// addressed object does not exist so the API can map it to 404.

// NodeBackend serves node reads, probes and filter previews.
type NodeBackend interface {
	ListNodes(ctx context.Context, limit, offset int, health, platform string) ([]Node, int, error)
	GetNode(ctx context.Context, hash string) (Node, error)
	ProbeNode(ctx context.Context, hash string, kind ProbeKind) error
	PreviewFilter(ctx context.Context, f PlatformFilter) (matched, total int, err error)
}

// LeaseBackend serves lease reads and releases.
type LeaseBackend interface {
	ListLeases(ctx context.Context, platformID string, limit, offset int) ([]Lease, int, error)
	GetLease(ctx context.Context, platformID, account string) (Lease, error)
	ReleaseLease(ctx context.Context, platformID, account string) error
	ReleaseAllLeases(ctx context.Context, platformID string) error
	IPLoad(ctx context.Context, platformID string) (map[string]int64, error)
}

// ViewBackend rebuilds derived routing views.
type ViewBackend interface {
	RebuildRoutableView(ctx context.Context, platformID string) error
}

// SourceBackend triggers source refreshes.
type SourceBackend interface {
	RefreshSource(ctx context.Context, name string) error
}

// MetricsBackend serves the metrics reads. name is the path tail (e.g.
// "throughput"); Realtime gets the raw query values: from/to (RFC3339
// window bounds) and platform_id (leases series only).
type MetricsBackend interface {
	MetricRealtime(ctx context.Context, name string, query url.Values) (any, error)
	MetricHistory(ctx context.Context, name string) (any, error)
	MetricSnapshot(ctx context.Context, name string) (any, error)
}

// ReqLogBackend serves request-log reads.
type ReqLogBackend interface {
	ListRequestLogs(ctx context.Context, limit, offset int, platform, account, egressErr string) ([]RequestLogEntry, int, error)
	GetRequestLog(ctx context.Context, id string) (RequestLogEntry, error)
	GetRequestLogPayload(ctx context.Context, id string) (any, error)
}

// GeoBackend serves GeoIP status, lookups and updates.
type GeoBackend interface {
	GeoIPStatus(ctx context.Context) (any, error)
	GeoIPLookup(ctx context.Context, ips []string) (any, error)
	GeoIPUpdateNow(ctx context.Context) error
}

// RuntimeBackend is the union the assembly root wires once; the service
// splits it into the domain slices above.
type RuntimeBackend interface {
	NodeBackend
	LeaseBackend
	ViewBackend
	SourceBackend
	MetricsBackend
	ReqLogBackend
	GeoBackend
}

// ---------------------------------------------------------------------------
// Route registration (spec/openapi.yaml, 45 paths)
// ---------------------------------------------------------------------------

// RegisterRoutes registers every control-plane route from spec/openapi.yaml
// on mux. All routes except GET /healthz require
// `Authorization: Bearer <AdminToken>` (see WithAdminToken/SetAdminToken);
// failures answer 401 {"error":{"code":"UNAUTHORIZED"}}.
//
// Routes the control plane does not own (nodes, leases, metrics,
// request-logs, geoip, probes, view rebuild, source refresh) are served
// through RuntimeBackend and answer 501 NOT_IMPLEMENTED until the runtime
// group wires it.
func (s *Service) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", s.handleHealthz)

	auth := func(h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !s.checkAuth(r) {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing or invalid admin token")
				return
			}
			h(w, r)
		}
	}
	reg := func(pattern string, h http.HandlerFunc) { mux.HandleFunc(pattern, auth(h)) }

	// System.
	reg("GET /api/v1/system/info", s.handleSystemInfo)
	reg("GET /api/v1/system/config", s.handleGetSystemConfig)
	reg("PATCH /api/v1/system/config", s.handlePatchSystemConfig)
	reg("GET /api/v1/system/config/default", s.handleSystemConfigDefault)
	reg("GET /api/v1/system/config/env", s.handleSystemConfigEnv)

	// Platforms.
	reg("GET /api/v1/platforms", s.handleListPlatforms)
	reg("POST /api/v1/platforms", s.handleCreatePlatform)
	reg("POST /api/v1/platforms/preview-filter", s.handlePreviewFilter)
	reg("GET /api/v1/platforms/{id}", s.handleGetPlatform)
	reg("PATCH /api/v1/platforms/{id}", s.handlePatchPlatform)
	reg("DELETE /api/v1/platforms/{id}", s.handleDeletePlatform)
	reg("POST /api/v1/platforms/{id}/actions/reset-to-default", s.handleResetPlatform)
	reg("POST /api/v1/platforms/{id}/actions/rebuild-routable-view", s.handleRebuildView)
	reg("GET /api/v1/platforms/{id}/leases", s.handleListLeases)
	reg("DELETE /api/v1/platforms/{id}/leases", s.handleDeleteLeases)
	reg("GET /api/v1/platforms/{id}/leases/{account}", s.handleGetLease)
	reg("DELETE /api/v1/platforms/{id}/leases/{account}", s.handleReleaseLease)
	reg("GET /api/v1/platforms/{id}/ip-load", s.handleIPLoad)

	// Identity policies.
	reg("GET /api/v1/identity-policies", s.handleListIdentityPolicies)
	reg("POST /api/v1/identity-policies", s.handleCreateIdentityPolicy)
	reg("GET /api/v1/identity-policies/{name}", s.handleGetIdentityPolicy)
	reg("PATCH /api/v1/identity-policies/{name}", s.handlePatchIdentityPolicy)
	reg("DELETE /api/v1/identity-policies/{name}", s.handleDeleteIdentityPolicy)

	// Sources.
	reg("GET /api/v1/sources", s.handleListSources)
	reg("POST /api/v1/sources", s.handleCreateSource)
	reg("GET /api/v1/sources/{id}", s.handleGetSource)
	reg("PATCH /api/v1/sources/{id}", s.handlePatchSource)
	reg("DELETE /api/v1/sources/{id}", s.handleDeleteSource)
	reg("POST /api/v1/sources/{id}/actions/refresh", s.handleRefreshSource)

	// Recipes.
	reg("GET /api/v1/recipes", s.handleListRecipes)
	reg("POST /api/v1/recipes", s.handleImportRecipe)
	reg("GET /api/v1/recipes/{name}", s.handleGetRecipe)
	reg("DELETE /api/v1/recipes/{name}", s.handleDeleteRecipe)
	reg("POST /api/v1/recipes/{name}/actions/apply", s.handleApplyRecipe)

	// Nodes.
	reg("GET /api/v1/nodes", s.handleListNodes)
	reg("GET /api/v1/nodes/{hash}", s.handleGetNode)
	reg("POST /api/v1/nodes/{hash}/actions/probe-egress", s.handleProbeEgress)
	reg("POST /api/v1/nodes/{hash}/actions/probe-latency", s.handleProbeLatency)

	// Endpoints.
	reg("GET /api/v1/endpoints", s.handleListEndpoints)
	reg("POST /api/v1/endpoints", s.handleCreateEndpoint)
	reg("GET /api/v1/endpoints/{id}", s.handleGetEndpoint)
	reg("PATCH /api/v1/endpoints/{id}", s.handlePatchEndpoint)
	reg("DELETE /api/v1/endpoints/{id}", s.handleDeleteEndpoint)

	// Request logs.
	reg("GET /api/v1/request-logs", s.handleListRequestLogs)
	reg("GET /api/v1/request-logs/{log_id}", s.handleGetRequestLog)
	reg("GET /api/v1/request-logs/{log_id}/payloads", s.handleGetRequestLogPayload)

	// Metrics.
	for _, n := range []string{"throughput", "connections", "leases"} {
		name := n
		reg("GET /api/v1/metrics/realtime/"+name, func(w http.ResponseWriter, r *http.Request) {
			s.handleMetric(w, r, "realtime", name)
		})
	}
	for _, n := range []string{"traffic", "requests", "access-latency", "probes", "node-pool", "lease-lifetime"} {
		name := n
		reg("GET /api/v1/metrics/history/"+name, func(w http.ResponseWriter, r *http.Request) {
			s.handleMetric(w, r, "history", name)
		})
	}
	for _, n := range []string{"node-pool", "platform-node-pool", "node-latency-distribution"} {
		name := n
		reg("GET /api/v1/metrics/snapshots/"+name, func(w http.ResponseWriter, r *http.Request) {
			s.handleMetric(w, r, "snapshots", name)
		})
	}

	// GeoIP.
	reg("GET /api/v1/geoip/status", s.handleGeoIPStatus)
	reg("GET /api/v1/geoip/lookup", s.handleGeoIPLookup)
	reg("POST /api/v1/geoip/lookup", s.handleGeoIPLookupPost)
	reg("POST /api/v1/geoip/actions/update-now", s.handleGeoIPUpdateNow)
}

// checkAuth reports whether the request carries the admin Bearer token.
// An empty configured token disables authentication (development only).
func (s *Service) checkAuth(r *http.Request) bool {
	if s.adminToken == "" {
		return true
	}
	h := r.Header.Get("Authorization")
	want := "Bearer " + s.adminToken
	if len(h) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(h), []byte(want)) == 1
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"code": code, "message": message},
	})
}

// mapError converts service/store errors to (HTTP status, code, message).
func mapError(err error) (int, string, string) {
	switch {
	case errors.Is(err, ports.ErrNotFound):
		return http.StatusNotFound, "NOT_FOUND", trimControlPrefix(err.Error())
	case errors.Is(err, ports.ErrAlreadyExists), errors.Is(err, ports.ErrConflict):
		return http.StatusConflict, "CONFLICT", trimControlPrefix(err.Error())
	case errors.Is(err, ports.ErrInvalid):
		return http.StatusBadRequest, "INVALID_ARGUMENT", trimControlPrefix(err.Error())
	case errors.Is(err, ErrUnprocessable):
		return http.StatusUnprocessableEntity, "UNPROCESSABLE", trimControlPrefix(err.Error())
	default:
		return http.StatusInternalServerError, "INTERNAL", "internal error"
	}
}

func trimControlPrefix(msg string) string {
	if strings.HasPrefix(msg, "control: ") {
		msg = strings.TrimPrefix(msg, "control: ")
	}
	// Strip the "ports: ..." sentinel text, keep the human detail.
	if i := strings.Index(msg, "ports: "); i >= 0 {
		msg = msg[:i] + msg[i+len("ports: "):]
	}
	return strings.TrimSpace(msg)
}

func writeServiceError(w http.ResponseWriter, err error) {
	status, code, msg := mapError(err)
	writeError(w, status, code, msg)
}

// pageEnvelope is the {items,total,limit,offset} envelope (openapi).
func pageEnvelope(items any, total, limit, offset int) map[string]any {
	if items == nil {
		items = []any{}
	}
	return map[string]any{"items": items, "total": total, "limit": limit, "offset": offset}
}

func parsePagination(r *http.Request) (limit, offset int, err error) {
	limit, offset = 50, 0
	q := r.URL.Query()
	if v := q.Get("limit"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 0 || n > 100000 {
			return 0, 0, fmt.Errorf("invalid limit %q (want 0-100000)", v)
		}
		limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < 0 {
			return 0, 0, fmt.Errorf("invalid offset %q (want >= 0)", v)
		}
		offset = n
	}
	return limit, offset, nil
}

// decodeJSONBody reads at most apiMaxBodyBytes and decodes JSON into v.
func (s *Service) decodeJSONBody(r *http.Request, v any) error {
	body := http.MaxBytesReader(nil, r.Body, s.apiMaxBodyBytes)
	defer r.Body.Close()
	dec := json.NewDecoder(body)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

// readPatchBody reads a raw merge-patch body.
func (s *Service) readPatchBody(r *http.Request) ([]byte, error) {
	body := http.MaxBytesReader(nil, r.Body, s.apiMaxBodyBytes)
	defer r.Body.Close()
	b, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("empty patch body")
	}
	return b, nil
}

// backendOr501 returns the domain backend or writes 501. The any()==nil
// check is safe here: backends are only ever assigned a concrete
// implementation or left unset (never a typed nil).
func backendOr501[T any](w http.ResponseWriter, what string, b T) (T, bool) {
	if any(b) == nil {
		writeError(w, http.StatusNotImplemented, "NOT_IMPLEMENTED",
			fmt.Sprintf("%s is served by the runtime backend, which is not wired", what))
		var zero T
		return zero, false
	}
	return b, true
}

// ---------------------------------------------------------------------------
// System
// ---------------------------------------------------------------------------

func (s *Service) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Service) handleSystemInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"version":   s.version,
		"uptime_ns": time.Since(s.startTime).Nanoseconds(),
	})
}

func (s *Service) handleGetSystemConfig(w http.ResponseWriter, r *http.Request) {
	c, err := s.GetSystemConfig(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Service) handlePatchSystemConfig(w http.ResponseWriter, r *http.Request) {
	patch, err := s.readPatchBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	c, err := s.PatchSystemConfig(r.Context(), patch)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Service) handleSystemConfigDefault(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.bootConfig)
}

// envVarsForEndpoint lists the startup variables reported by
// GET /api/v1/system/config/env. Token values are masked.
var envVarsForEndpoint = []struct {
	env    string
	secret bool
}{
	{"EGRESS_LISTEN_ADDRESS", false},
	{"EGRESS_PORT", false},
	{"EGRESS_ADMIN_TOKEN", true},
	{"EGRESS_PROXY_TOKEN", true},
	{"EGRESS_STATE_DIR", false},
	{"EGRESS_CACHE_DIR", false},
	{"EGRESS_LOG_DIR", false},
	{"EGRESS_CONFIG", false},
}

func (s *Service) handleSystemConfigEnv(w http.ResponseWriter, r *http.Request) {
	out := make(map[string]string, len(envVarsForEndpoint))
	for _, v := range envVarsForEndpoint {
		val, _ := os.LookupEnv(v.env)
		if v.secret && val != "" {
			val = "***"
		}
		out[v.env] = val
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// Platforms
// ---------------------------------------------------------------------------

func (s *Service) handleListPlatforms(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	items, total, err := s.ListPlatforms(r.Context(), limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

func (s *Service) handleCreatePlatform(w http.ResponseWriter, r *http.Request) {
	var p Platform
	if err := s.decodeJSONBody(r, &p); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	id, err := s.ApplyPlatform(r.Context(), p)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	created, err := s.GetPlatform(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Service) handlePreviewFilter(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Filter PlatformFilter `json:"filter"`
	}
	if err := s.decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if err := validateFilter(body.Filter); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", trimControlPrefix(err.Error()))
		return
	}
	b, ok := backendOr501(w, "filter preview", s.nodeBackend)
	if !ok {
		return
	}
	matched, total, err := b.PreviewFilter(r.Context(), body.Filter)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"matched": matched, "total": total})
}

func (s *Service) handleGetPlatform(w http.ResponseWriter, r *http.Request) {
	p, err := s.GetPlatform(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Service) handlePatchPlatform(w http.ResponseWriter, r *http.Request) {
	patch, err := s.readPatchBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	p, err := s.PatchPlatform(r.Context(), r.PathValue("id"), patch)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Service) handleDeletePlatform(w http.ResponseWriter, r *http.Request) {
	if err := s.DeletePlatform(r.Context(), r.PathValue("id")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleResetPlatform(w http.ResponseWriter, r *http.Request) {
	p, err := s.ResetPlatform(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// platformForAction resolves the platform and returns its runtime ID,
// mapping a missing platform to 404 before delegating to the backend.
func (s *Service) platformForAction(w http.ResponseWriter, r *http.Request) (string, bool) {
	p, err := s.GetPlatform(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return "", false
	}
	return p.ID, true
}

func (s *Service) handleRebuildView(w http.ResponseWriter, r *http.Request) {
	id, ok := s.platformForAction(w, r)
	if !ok {
		return
	}
	b, ok := backendOr501(w, "view rebuild", s.viewBackend)
	if !ok {
		return
	}
	if err := b.RebuildRoutableView(r.Context(), id); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Service) handleListLeases(w http.ResponseWriter, r *http.Request) {
	id, ok := s.platformForAction(w, r)
	if !ok {
		return
	}
	b, ok := backendOr501(w, "leases", s.leaseBackend)
	if !ok {
		return
	}
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	items, total, err := b.ListLeases(r.Context(), id, limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if items == nil {
		items = []Lease{}
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

func (s *Service) handleDeleteLeases(w http.ResponseWriter, r *http.Request) {
	id, ok := s.platformForAction(w, r)
	if !ok {
		return
	}
	b, ok := backendOr501(w, "leases", s.leaseBackend)
	if !ok {
		return
	}
	if err := b.ReleaseAllLeases(r.Context(), id); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleGetLease(w http.ResponseWriter, r *http.Request) {
	id, ok := s.platformForAction(w, r)
	if !ok {
		return
	}
	b, ok := backendOr501(w, "leases", s.leaseBackend)
	if !ok {
		return
	}
	l, err := b.GetLease(r.Context(), id, r.PathValue("account"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, l)
}

func (s *Service) handleReleaseLease(w http.ResponseWriter, r *http.Request) {
	id, ok := s.platformForAction(w, r)
	if !ok {
		return
	}
	b, ok := backendOr501(w, "leases", s.leaseBackend)
	if !ok {
		return
	}
	if err := b.ReleaseLease(r.Context(), id, r.PathValue("account")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleIPLoad(w http.ResponseWriter, r *http.Request) {
	id, ok := s.platformForAction(w, r)
	if !ok {
		return
	}
	b, ok := backendOr501(w, "ip-load", s.leaseBackend)
	if !ok {
		return
	}
	m, err := b.IPLoad(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if m == nil {
		m = map[string]int64{}
	}
	writeJSON(w, http.StatusOK, m)
}

// ---------------------------------------------------------------------------
// Identity policies
// ---------------------------------------------------------------------------

func (s *Service) handleListIdentityPolicies(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	items, total, err := s.ListIdentityPolicies(r.Context(), limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

func (s *Service) handleCreateIdentityPolicy(w http.ResponseWriter, r *http.Request) {
	var p IdentityPolicy
	if err := s.decodeJSONBody(r, &p); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if err := s.ApplyIdentityPolicy(r.Context(), p); err != nil {
		writeServiceError(w, err)
		return
	}
	created, err := s.GetIdentityPolicy(r.Context(), p.Name)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Service) handleGetIdentityPolicy(w http.ResponseWriter, r *http.Request) {
	p, err := s.GetIdentityPolicy(r.Context(), r.PathValue("name"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Service) handlePatchIdentityPolicy(w http.ResponseWriter, r *http.Request) {
	patch, err := s.readPatchBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	p, err := s.PatchIdentityPolicy(r.Context(), r.PathValue("name"), patch)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Service) handleDeleteIdentityPolicy(w http.ResponseWriter, r *http.Request) {
	if err := s.DeleteIdentityPolicy(r.Context(), r.PathValue("name")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

func (s *Service) handleListSources(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	items, total, err := s.ListSources(r.Context(), limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

func (s *Service) handleCreateSource(w http.ResponseWriter, r *http.Request) {
	var src Source
	if err := s.decodeJSONBody(r, &src); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if err := s.ApplySource(r.Context(), src); err != nil {
		writeServiceError(w, err)
		return
	}
	created, err := s.GetSource(r.Context(), src.Name)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Service) handleGetSource(w http.ResponseWriter, r *http.Request) {
	src, err := s.GetSource(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, src)
}

func (s *Service) handlePatchSource(w http.ResponseWriter, r *http.Request) {
	patch, err := s.readPatchBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	src, err := s.PatchSource(r.Context(), r.PathValue("id"), patch)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, src)
}

func (s *Service) handleDeleteSource(w http.ResponseWriter, r *http.Request) {
	if err := s.DeleteSource(r.Context(), r.PathValue("id")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleRefreshSource(w http.ResponseWriter, r *http.Request) {
	src, err := s.GetSource(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	b, ok := backendOr501(w, "source refresh", s.sourceBackend)
	if !ok {
		return
	}
	if err := b.RefreshSource(r.Context(), src.Name); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// ---------------------------------------------------------------------------
// Recipes
// ---------------------------------------------------------------------------

func (s *Service) handleListRecipes(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	items, total, err := s.ListRecipes(r.Context(), limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

// handleImportRecipe accepts application/json (default) and
// application/yaml, creating or updating the recipe.
func (s *Service) handleImportRecipe(w http.ResponseWriter, r *http.Request) {
	var rec Recipe
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "yaml") {
		body := http.MaxBytesReader(nil, r.Body, s.apiMaxBodyBytes)
		defer r.Body.Close()
		b, err := io.ReadAll(body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
		var fileDoc struct {
			APIVersion string `yaml:"apiVersion"`
			Kind       string `yaml:"kind"`
			Metadata   struct {
				Name     string   `yaml:"name"`
				Version  string   `yaml:"version"`
				Requires []string `yaml:"requires"`
			} `yaml:"metadata"`
			Spec yaml.Node `yaml:"spec"`
		}
		if err := yaml.Unmarshal(b, &fileDoc); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", fmt.Sprintf("invalid YAML: %v", err))
			return
		}
		rec.Name = fileDoc.Metadata.Name
		rec.Version = fileDoc.Metadata.Version
		rec.Requires = fileDoc.Metadata.Requires
		var spec struct {
			IdentityPolicy   *IdentityPolicySpec   `yaml:"identityPolicy"`
			PlatformTemplate *PlatformTemplateSpec `yaml:"platformTemplate"`
		}
		if err := fileDoc.Spec.Decode(&spec); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", fmt.Sprintf("invalid recipe spec: %v", err))
			return
		}
		rec.IdentityPolicy = spec.IdentityPolicy
		rec.PlatformTemplate = spec.PlatformTemplate
	} else {
		if err := s.decodeJSONBody(r, &rec); err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
			return
		}
	}
	if err := s.ImportRecipe(r.Context(), rec); err != nil {
		writeServiceError(w, err)
		return
	}
	created, err := s.GetRecipe(r.Context(), rec.Name)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Service) handleGetRecipe(w http.ResponseWriter, r *http.Request) {
	rec, err := s.GetRecipe(r.Context(), r.PathValue("name"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

func (s *Service) handleDeleteRecipe(w http.ResponseWriter, r *http.Request) {
	if err := s.DeleteRecipe(r.Context(), r.PathValue("name")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleApplyRecipe(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Platform string `json:"platform"`
	}
	if err := s.decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if strings.TrimSpace(body.Platform) == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "platform is required")
		return
	}
	res, err := s.ApplyRecipe(r.Context(), r.PathValue("name"), strings.TrimSpace(body.Platform))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	out := map[string]any{"platform": res.Platform}
	if res.IdentityPolicy != nil {
		out["identityPolicy"] = res.IdentityPolicy
	}
	writeJSON(w, http.StatusOK, out)
}

// ---------------------------------------------------------------------------
// Nodes (backend)
// ---------------------------------------------------------------------------

func (s *Service) handleListNodes(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "nodes", s.nodeBackend)
	if !ok {
		return
	}
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	q := r.URL.Query()
	items, total, err := b.ListNodes(r.Context(), limit, offset, q.Get("health"), q.Get("platform"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if items == nil {
		items = []Node{}
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

func (s *Service) handleGetNode(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "nodes", s.nodeBackend)
	if !ok {
		return
	}
	n, err := b.GetNode(r.Context(), r.PathValue("hash"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *Service) handleProbeEgress(w http.ResponseWriter, r *http.Request) {
	s.handleProbe(w, r, ProbeKindEgress)
}

func (s *Service) handleProbeLatency(w http.ResponseWriter, r *http.Request) {
	s.handleProbe(w, r, ProbeKindLatency)
}

func (s *Service) handleProbe(w http.ResponseWriter, r *http.Request, kind ProbeKind) {
	b, ok := backendOr501(w, "probes", s.nodeBackend)
	if !ok {
		return
	}
	if err := b.ProbeNode(r.Context(), r.PathValue("hash"), kind); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

func (s *Service) handleListEndpoints(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	items, total, err := s.ListEndpoints(r.Context(), limit, offset)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

func (s *Service) handleCreateEndpoint(w http.ResponseWriter, r *http.Request) {
	var e Endpoint
	if err := s.decodeJSONBody(r, &e); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	if err := s.ApplyEndpoint(r.Context(), e); err != nil {
		writeServiceError(w, err)
		return
	}
	created, err := s.GetEndpoint(r.Context(), e.Name)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Service) handleGetEndpoint(w http.ResponseWriter, r *http.Request) {
	e, err := s.GetEndpoint(r.Context(), r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func (s *Service) handlePatchEndpoint(w http.ResponseWriter, r *http.Request) {
	patch, err := s.readPatchBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	e, err := s.PatchEndpoint(r.Context(), r.PathValue("id"), patch)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func (s *Service) handleDeleteEndpoint(w http.ResponseWriter, r *http.Request) {
	if err := s.DeleteEndpoint(r.Context(), r.PathValue("id")); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Request logs (backend)
// ---------------------------------------------------------------------------

func (s *Service) handleListRequestLogs(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "request logs", s.reqLogBackend)
	if !ok {
		return
	}
	limit, offset, err := parsePagination(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	q := r.URL.Query()
	items, total, err := b.ListRequestLogs(r.Context(), limit, offset, q.Get("platform"), q.Get("account"), q.Get("egress_error"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if items == nil {
		items = []RequestLogEntry{}
	}
	writeJSON(w, http.StatusOK, pageEnvelope(items, total, limit, offset))
}

func (s *Service) handleGetRequestLog(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "request logs", s.reqLogBackend)
	if !ok {
		return
	}
	e, err := b.GetRequestLog(r.Context(), r.PathValue("log_id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func (s *Service) handleGetRequestLogPayload(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "request logs", s.reqLogBackend)
	if !ok {
		return
	}
	p, err := b.GetRequestLogPayload(r.Context(), r.PathValue("log_id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// ---------------------------------------------------------------------------
// Metrics (backend)
// ---------------------------------------------------------------------------

func (s *Service) handleMetric(w http.ResponseWriter, r *http.Request, group, name string) {
	b, ok := backendOr501(w, "metrics", s.metricsBackend)
	if !ok {
		return
	}
	var (
		v   any
		err error
	)
	switch group {
	case "realtime":
		v, err = b.MetricRealtime(r.Context(), name, r.URL.Query())
	case "history":
		v, err = b.MetricHistory(r.Context(), name)
	default:
		v, err = b.MetricSnapshot(r.Context(), name)
	}
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if v == nil {
		v = map[string]any{}
	}
	writeJSON(w, http.StatusOK, v)
}

// ---------------------------------------------------------------------------
// GeoIP (backend)
// ---------------------------------------------------------------------------

func (s *Service) handleGeoIPStatus(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "geoip", s.geoBackend)
	if !ok {
		return
	}
	v, err := b.GeoIPStatus(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, orEmptyObject(v))
}

func (s *Service) handleGeoIPLookup(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "geoip", s.geoBackend)
	if !ok {
		return
	}
	ip := r.URL.Query().Get("ip")
	if ip == "" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "query parameter ip is required")
		return
	}
	v, err := b.GeoIPLookup(r.Context(), []string{ip})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, orEmptyObject(v))
}

func (s *Service) handleGeoIPLookupPost(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "geoip", s.geoBackend)
	if !ok {
		return
	}
	var body struct {
		IPs []string `json:"ips"`
	}
	if err := s.decodeJSONBody(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}
	v, err := b.GeoIPLookup(r.Context(), body.IPs)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, orEmptyObject(v))
}

func (s *Service) handleGeoIPUpdateNow(w http.ResponseWriter, r *http.Request) {
	b, ok := backendOr501(w, "geoip", s.geoBackend)
	if !ok {
		return
	}
	if err := b.GeoIPUpdateNow(r.Context()); err != nil {
		writeServiceError(w, err)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func orEmptyObject(v any) any {
	if v == nil {
		return map[string]any{}
	}
	return v
}
