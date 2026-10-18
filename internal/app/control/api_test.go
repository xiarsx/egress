package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testMux() (*Service, *http.ServeMux) {
	s := NewService(newMemStore(), &recordHooks{},
		WithAdminToken("secret"),
		WithVersion("0.1.0-test"),
		WithSystemConfig(SystemConfig{ListenAddress: "127.0.0.1", Port: 8080, StateDir: "/s", CacheDir: "/c", LogDir: "/l"}),
	)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	return s, mux
}

func do(t *testing.T, mux *http.ServeMux, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rdr)
	if body != "" && !strings.HasPrefix(body, "{") && !strings.HasPrefix(body, "[") {
		req.Header.Set("Content-Type", "application/merge-patch+json")
	} else if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func authed(t *testing.T, mux *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, mux, method, path, body, "secret")
}

func decodeBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var v map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("bad JSON response: %v\n%s", err, rec.Body.String())
	}
	return v
}

func TestAuth(t *testing.T) {
	_, mux := testMux()
	// /healthz is public.
	if rec := do(t, mux, "GET", "/healthz", "", ""); rec.Code != 200 {
		t.Fatalf("healthz = %d", rec.Code)
	}
	// Missing token → 401 envelope.
	rec := do(t, mux, "GET", "/api/v1/platforms", "", "")
	if rec.Code != 401 {
		t.Fatalf("code = %d", rec.Code)
	}
	v := decodeBody(t, rec)
	errObj := v["error"].(map[string]any)
	if errObj["code"] != "UNAUTHORIZED" {
		t.Fatalf("envelope = %v", v)
	}
	// Wrong token → 401.
	if rec := do(t, mux, "GET", "/api/v1/platforms", "", "nope"); rec.Code != 401 {
		t.Fatalf("code = %d", rec.Code)
	}
	// Right token → 200.
	if rec := authed(t, mux, "GET", "/api/v1/platforms", ""); rec.Code != 200 {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestPlatformAPI(t *testing.T) {
	_, mux := testMux()

	rec := authed(t, mux, "POST", "/api/v1/platforms",
		`{"name":"video","allocation":"BALANCED","sticky_ttl":"168h","filter":{"tags":["^sub-a/"]}}`)
	if rec.Code != 201 {
		t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
	}
	created := decodeBody(t, rec)
	id, _ := created["id"].(string)
	if len(id) != 32 || created["name"] != "video" {
		t.Fatalf("created = %v", created)
	}

	// List envelope.
	rec = authed(t, mux, "GET", "/api/v1/platforms?limit=10&offset=0", "")
	v := decodeBody(t, rec)
	if v["total"] != float64(1) || v["limit"] != float64(10) || v["offset"] != float64(0) {
		t.Fatalf("page = %v", v)
	}
	if items, ok := v["items"].([]any); !ok || len(items) != 1 {
		t.Fatalf("items = %v", v["items"])
	}

	// Get by ID.
	rec = authed(t, mux, "GET", "/api/v1/platforms/"+id, "")
	if rec.Code != 200 {
		t.Fatalf("get = %d", rec.Code)
	}

	// Merge-patch.
	rec = authed(t, mux, "PATCH", "/api/v1/platforms/"+id, `{"sticky_ttl":"24h"}`)
	if rec.Code != 200 {
		t.Fatalf("patch = %d %s", rec.Code, rec.Body.String())
	}
	if decodeBody(t, rec)["sticky_ttl"] != "24h0m0s" {
		t.Fatalf("patched = %s", rec.Body.String())
	}

	// Invalid body → 400 envelope.
	rec = authed(t, mux, "POST", "/api/v1/platforms", `{"name":"x","allocation":"NOPE"}`)
	if rec.Code != 400 {
		t.Fatalf("code = %d", rec.Code)
	}
	if decodeBody(t, rec)["error"].(map[string]any)["code"] != "INVALID_ARGUMENT" {
		t.Fatalf("envelope = %s", rec.Body.String())
	}

	// Delete → 204, then 404 envelope.
	if rec := authed(t, mux, "DELETE", "/api/v1/platforms/"+id, ""); rec.Code != 204 {
		t.Fatalf("delete = %d", rec.Code)
	}
	rec = authed(t, mux, "GET", "/api/v1/platforms/"+id, "")
	if rec.Code != 404 {
		t.Fatalf("code = %d", rec.Code)
	}
	if decodeBody(t, rec)["error"].(map[string]any)["code"] != "NOT_FOUND" {
		t.Fatalf("envelope = %s", rec.Body.String())
	}
}

func TestBackendNil501(t *testing.T) {
	_, mux := testMux()
	for _, p := range []string{
		"GET /api/v1/nodes",
		"GET /api/v1/request-logs",
		"GET /api/v1/metrics/realtime/throughput",
		"GET /api/v1/geoip/status",
	} {
		parts := strings.SplitN(p, " ", 2)
		rec := authed(t, mux, parts[0], parts[1], "")
		if rec.Code != 501 {
			t.Fatalf("%s = %d", p, rec.Code)
		}
		if decodeBody(t, rec)["error"].(map[string]any)["code"] != "NOT_IMPLEMENTED" {
			t.Fatalf("envelope = %s", rec.Body.String())
		}
	}
}

func TestIdentityPolicyAPI(t *testing.T) {
	_, mux := testMux()
	body := `{"name":"strict","chain":[{"header":"X-Account"}],"on_miss":"reject"}`
	if rec := authed(t, mux, "POST", "/api/v1/identity-policies", body); rec.Code != 201 {
		t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
	}
	// Reference it, then delete → 409.
	if rec := authed(t, mux, "POST", "/api/v1/platforms", `{"name":"p","identity_policy":"strict"}`); rec.Code != 201 {
		t.Fatalf("create platform = %d", rec.Code)
	}
	rec := authed(t, mux, "DELETE", "/api/v1/identity-policies/strict", "")
	if rec.Code != 409 {
		t.Fatalf("delete = %d %s", rec.Code, rec.Body.String())
	}
	if decodeBody(t, rec)["error"].(map[string]any)["code"] != "CONFLICT" {
		t.Fatalf("envelope = %s", rec.Body.String())
	}
}

func TestSourceAPIAndRefresh501(t *testing.T) {
	_, mux := testMux()
	rec := authed(t, mux, "POST", "/api/v1/sources",
		`{"name":"main","type":"remote","url":"https://example.com/sub.txt"}`)
	if rec.Code != 201 {
		t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
	}
	// Refresh on existing source → 501 (no backend), proving the 404/501 order.
	rec = authed(t, mux, "POST", "/api/v1/sources/main/actions/refresh", "")
	if rec.Code != 501 {
		t.Fatalf("refresh = %d", rec.Code)
	}
	// Refresh on missing source → 404.
	rec = authed(t, mux, "POST", "/api/v1/sources/nope/actions/refresh", "")
	if rec.Code != 404 {
		t.Fatalf("refresh = %d", rec.Code)
	}
}

func TestRecipeAPI(t *testing.T) {
	_, mux := testMux()
	rec := authed(t, mux, "POST", "/api/v1/recipes",
		`{"name":"r1","version":"1.0.0","requires":["egress>=99.0.0"]}`)
	if rec.Code != 201 {
		t.Fatalf("import = %d %s", rec.Code, rec.Body.String())
	}
	// Apply with unsatisfied requires → 422.
	rec = authed(t, mux, "POST", "/api/v1/recipes/r1/actions/apply", `{"platform":"p1"}`)
	if rec.Code != 422 {
		t.Fatalf("apply = %d %s", rec.Code, rec.Body.String())
	}
	if decodeBody(t, rec)["error"].(map[string]any)["code"] != "UNPROCESSABLE" {
		t.Fatalf("envelope = %s", rec.Body.String())
	}
	// Apply missing recipe → 404.
	rec = authed(t, mux, "POST", "/api/v1/recipes/nope/actions/apply", `{"platform":"p1"}`)
	if rec.Code != 404 {
		t.Fatalf("apply = %d", rec.Code)
	}
}

func TestSystemAPI(t *testing.T) {
	_, mux := testMux()
	rec := authed(t, mux, "GET", "/api/v1/system/info", "")
	if rec.Code != 200 {
		t.Fatalf("info = %d", rec.Code)
	}
	if decodeBody(t, rec)["version"] != "0.1.0-test" {
		t.Fatalf("info = %s", rec.Body.String())
	}
	rec = authed(t, mux, "GET", "/api/v1/system/config", "")
	if rec.Code != 200 || decodeBody(t, rec)["port"] != float64(8080) {
		t.Fatalf("config = %d %s", rec.Code, rec.Body.String())
	}
	rec = authed(t, mux, "PATCH", "/api/v1/system/config", `{"port":9090}`)
	if rec.Code != 200 || decodeBody(t, rec)["port"] != float64(9090) {
		t.Fatalf("patch = %d %s", rec.Code, rec.Body.String())
	}
	rec = authed(t, mux, "GET", "/api/v1/system/config/default", "")
	if rec.Code != 200 || decodeBody(t, rec)["port"] != float64(8080) {
		t.Fatalf("default = %d %s", rec.Code, rec.Body.String())
	}
	rec = authed(t, mux, "GET", "/api/v1/system/config/env", "")
	if rec.Code != 200 {
		t.Fatalf("env = %d", rec.Code)
	}
}

func TestEndpointAPI(t *testing.T) {
	_, mux := testMux()
	rec := authed(t, mux, "POST", "/api/v1/endpoints",
		`{"name":"socks","port":1080,"allow_socks5":true}`)
	if rec.Code != 201 {
		t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
	}
	v := decodeBody(t, rec)
	if v["id"] != "socks" || v["port"] != float64(1080) || v["enabled"] != true {
		t.Fatalf("created = %v", v)
	}
	rec = authed(t, mux, "GET", "/api/v1/endpoints/socks", "")
	if rec.Code != 200 {
		t.Fatalf("get = %d", rec.Code)
	}
	if rec := authed(t, mux, "DELETE", "/api/v1/endpoints/socks", ""); rec.Code != 204 {
		t.Fatalf("delete = %d", rec.Code)
	}
}

func TestPaginationBadInput(t *testing.T) {
	_, mux := testMux()
	rec := authed(t, mux, "GET", "/api/v1/platforms?limit=999999999", "")
	if rec.Code != 400 {
		t.Fatalf("code = %d", rec.Code)
	}
}
