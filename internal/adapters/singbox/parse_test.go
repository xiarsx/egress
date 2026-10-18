package singbox

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func mustParse(t *testing.T, content string) []Outbound {
	t.Helper()
	obs, err := ParseSubscription([]byte(content))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	return obs
}

func typesOf(obs []Outbound) []string {
	types := make([]string, 0, len(obs))
	for _, ob := range obs {
		types = append(types, ob.Type())
	}
	return types
}

func tagsOf(obs []Outbound) []string {
	tags := make([]string, 0, len(obs))
	for _, ob := range obs {
		if tg, ok := ob.(Taggable); ok {
			tags = append(tags, tg.Tag())
		}
	}
	return tags
}

func TestParseSubscription_SingboxJSON(t *testing.T) {
	content := `{"outbounds":[
		{"type":"socks","tag":"s1","server":"127.0.0.1","server_port":1080},
		{"type":"vmess","tag":"v1","server":"example.com","server_port":443,"uuid":"26a1d547-b031-4139-9fc5-6671e1d0408a"}
	]}`
	obs := mustParse(t, content)
	if len(obs) != 2 {
		t.Fatalf("got %d outbounds, want 2", len(obs))
	}
	if got := typesOf(obs); got[0] != "socks" || got[1] != "vmess" {
		t.Fatalf("types = %v", got)
	}
	if got := tagsOf(obs); got[0] != "s1" || got[1] != "v1" {
		t.Fatalf("tags = %v", got)
	}
	// RawOptions must not contain the tag (hash input).
	var m map[string]any
	if err := json.Unmarshal(obs[0].RawOptions(), &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["tag"]; ok {
		t.Fatalf("RawOptions contains tag: %s", obs[0].RawOptions())
	}
	if m["server"] != "127.0.0.1" {
		t.Fatalf("RawOptions lost fields: %s", obs[0].RawOptions())
	}
}

func TestParseSubscription_BareArray(t *testing.T) {
	content := `[{"type":"http","tag":"h1","server":"10.0.0.1","server_port":8080}]`
	obs := mustParse(t, content)
	if len(obs) != 1 || obs[0].Type() != "http" {
		t.Fatalf("got %v", typesOf(obs))
	}
}

func TestParseSubscription_SingboxJSONSkipsUnsupported(t *testing.T) {
	content := `{"outbounds":[
		{"type":"direct","tag":"d1"},
		{"type":"socks","tag":"s1","server":"127.0.0.1","server_port":1080}
	]}`
	obs := mustParse(t, content)
	if len(obs) != 1 || obs[0].Type() != "socks" {
		t.Fatalf("got %v", typesOf(obs))
	}
}

func TestParseSubscription_ClashJSON(t *testing.T) {
	content := `{"proxies":[
		{"name":"clash-ss","type":"ss","server":"example.com","port":8388,"cipher":"aes-256-gcm","password":"secret"},
		{"name":"clash-vmess","type":"vmess","server":"example.com","port":443,"uuid":"26a1d547-b031-4139-9fc5-6671e1d0408a"}
	]}`
	obs := mustParse(t, content)
	if len(obs) != 2 {
		t.Fatalf("got %d outbounds, want 2", len(obs))
	}
	if got := typesOf(obs); got[0] != "shadowsocks" || got[1] != "vmess" {
		t.Fatalf("types = %v", got)
	}
	if got := tagsOf(obs); got[0] != "clash-ss" || got[1] != "clash-vmess" {
		t.Fatalf("tags = %v", got)
	}
}

func TestParseSubscription_ClashYAML(t *testing.T) {
	content := "proxies:\n" +
		"  - name: yaml-trojan\n" +
		"    type: trojan\n" +
		"    server: example.com\n" +
		"    port: 443\n" +
		"    password: secret\n"
	obs := mustParse(t, content)
	if len(obs) != 1 || obs[0].Type() != "trojan" {
		t.Fatalf("got %v", typesOf(obs))
	}
	if got := tagsOf(obs); got[0] != "yaml-trojan" {
		t.Fatalf("tags = %v", got)
	}
}

func TestParseSubscription_URILines(t *testing.T) {
	vmessPayload := base64.StdEncoding.EncodeToString([]byte(
		`{"add":"example.com","port":"443","id":"26a1d547-b031-4139-9fc5-6671e1d0408a","aid":"0","net":"tcp","type":"none","tls":"tls","ps":"VMESS"}`))
	ssCred := base64.StdEncoding.EncodeToString([]byte("aes-256-gcm:secret"))
	content := strings.Join([]string{
		"vmess://" + vmessPayload,
		"vless://26a1d547-b031-4139-9fc5-6671e1d0408a@example.com:443?type=tcp&security=tls&sni=example.com#VLESS%20Node",
		"trojan://password@example.com:443?allowInsecure=1&sni=example.com#Trojan%20Node",
		"ss://" + ssCred + "@example.com:8388#SS%20Node",
		"hysteria2://password@example.com:443?sni=example.com#HY2%20Node",
		"http://user:pass@example.com:8080#HTTP%20Node",
		"https://user:pass@example.com:8443?sni=example.com#HTTPS%20Node",
		"socks5://user:pass@example.com:1080#S5%20Node",
		"socks5h://user:pass@example.com:1080#S5H%20Node",
	}, "\n")
	obs := mustParse(t, content)
	want := []string{"vmess", "vless", "trojan", "shadowsocks", "hysteria2", "http", "http", "socks", "socks"}
	if got := typesOf(obs); !equalStrings(got, want) {
		t.Fatalf("types = %v, want %v", got, want)
	}
	// https:// URIs produce type http with TLS enabled.
	var m map[string]any
	if err := json.Unmarshal(obs[6].RawOptions(), &m); err != nil {
		t.Fatal(err)
	}
	tlsMap, _ := m["tls"].(map[string]any)
	if tlsMap["enabled"] != true {
		t.Fatalf("https URI outbound missing tls.enabled: %s", obs[6].RawOptions())
	}
}

func TestParseSubscription_PlainLines(t *testing.T) {
	content := "203.0.113.7:8080\n203.0.113.8:3128:alice:s3cret\n"
	obs := mustParse(t, content)
	if len(obs) != 2 {
		t.Fatalf("got %d outbounds, want 2", len(obs))
	}
	for _, ob := range obs {
		if ob.Type() != "http" {
			t.Fatalf("plain line type = %q, want http", ob.Type())
		}
	}
	var m map[string]any
	if err := json.Unmarshal(obs[1].RawOptions(), &m); err != nil {
		t.Fatal(err)
	}
	if m["username"] != "alice" || m["password"] != "s3cret" {
		t.Fatalf("plain line credentials lost: %s", obs[1].RawOptions())
	}
}

func TestParseSubscription_Base64Wrapped(t *testing.T) {
	plain := "trojan://password@example.com:443#Trojan%20Node\n" +
		"vless://26a1d547-b031-4139-9fc5-6671e1d0408a@example.com:443#VLESS%20Node\n"
	wrapped := base64.StdEncoding.EncodeToString([]byte(plain))
	obs := mustParse(t, wrapped)
	if len(obs) != 2 {
		t.Fatalf("got %d outbounds, want 2", len(obs))
	}
	if got := typesOf(obs); got[0] != "trojan" || got[1] != "vless" {
		t.Fatalf("types = %v", got)
	}
}

func TestParseSubscription_BadLinesSkippedAndCounted(t *testing.T) {
	content := strings.Join([]string{
		"# a comment",
		"",
		"trojan://password@example.com:443#Good",
		"vmess://%%%-not-base64-%%%",
		"vless://also-broken",
		"trojan://password2@example.com:8443#Good2",
	}, "\n")
	obs, skipped, err := ParseSubscriptionWithStats([]byte(content))
	if err != nil {
		t.Fatalf("ParseSubscriptionWithStats: %v", err)
	}
	if len(obs) != 2 {
		t.Fatalf("got %d outbounds, want 2", len(obs))
	}
	if skipped != 2 {
		t.Fatalf("skipped = %d, want 2", skipped)
	}
}

func TestParseSubscription_UnsupportedFormat(t *testing.T) {
	for _, content := range []string{
		"",
		"   \n  ",
		"just some random text without any scheme",
		`{"not_a_subscription": true}`,
	} {
		if _, err := ParseSubscription([]byte(content)); err == nil {
			t.Fatalf("expected error for %q", content)
		}
	}
}

func TestRawOptionsCanonical(t *testing.T) {
	// Same outbound, different key order and different tag → identical
	// RawOptions (the node-hash input).
	a := `{"type":"socks","tag":"one","server":"127.0.0.1","server_port":1080}`
	b := `{"server_port":1080,"server":"127.0.0.1","tag":"two","type":"socks"}`
	obsA := mustParse(t, `{"outbounds":[`+a+`]}`)
	obsB := mustParse(t, `{"outbounds":[`+b+`]}`)
	if string(obsA[0].RawOptions()) != string(obsB[0].RawOptions()) {
		t.Fatalf("RawOptions differ:\n%s\n%s", obsA[0].RawOptions(), obsB[0].RawOptions())
	}
	// Keys are sorted (canonical form).
	raw := string(obsA[0].RawOptions())
	if !strings.HasPrefix(raw, `{"server":`) {
		t.Fatalf("RawOptions not canonical: %s", raw)
	}
}

func TestToNodeSnapshots(t *testing.T) {
	obs := mustParse(t, `{"outbounds":[{"type":"http","tag":"h1","server":"10.0.0.1","server_port":8080}]}`)
	nodes := ToNodeSnapshots(obs)
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes", len(nodes))
	}
	n := nodes[0]
	if !n.HasOutbound {
		t.Fatal("HasOutbound = false")
	}
	if len(n.RawOptions) == 0 {
		t.Fatal("empty RawOptions")
	}
	if len(n.Tags) != 1 || n.Tags[0] != "h1" {
		t.Fatalf("tags = %v (raw, unscoped)", n.Tags)
	}
	if n.Attrs == nil {
		t.Fatal("Attrs nil, want empty map")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
