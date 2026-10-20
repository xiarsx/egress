//go:build e2e

package e2e

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestEndpointCRUDOpensAndClosesListeners proves Endpoint resources drive
// real sockets: create → the port serves the declared capabilities;
// delete → the port refuses. Also: the synthesized default endpoint is
// read-only over the API.
func TestEndpointCRUDOpensAndClosesListeners(t *testing.T) {
	g := startGateway(t)
	port := freePort(t)

	g.admin(t, http.MethodPost, "/api/v1/endpoints", map[string]any{
		"name":             "e2e-ep",
		"port":             port,
		"enabled":          true,
		"allow_proxy":      true,
		"allow_socks5":     true,
		"allow_management": false,
	}, http.StatusCreated)

	// The new listener answers a SOCKS5 greeting on its own port.
	waitFor(t, "endpoint listener to come up", 5*time.Second, func() bool {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
		if err != nil {
			return false
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := conn.Write([]byte{0x05, 0x01, 0x02}); err != nil {
			return false
		}
		sel := make([]byte, 2)
		_, err = io.ReadFull(conn, sel)
		return err == nil && sel[0] == 0x05 && sel[1] == 0x02
	})

	// Management capability is off: the admin API must not answer there.
	epClient := &http.Client{Timeout: 2 * time.Second}
	resp, err := epClient.Get(fmt.Sprintf("http://127.0.0.1:%d/api/v1/platforms", port))
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusForbidden {
			t.Fatalf("management on proxy-only endpoint = %d, want 403/404", resp.StatusCode)
		}
	}

	// Delete → listener closes, port refuses.
	g.admin(t, http.MethodDelete, "/api/v1/endpoints/e2e-ep", nil, http.StatusNoContent)
	waitFor(t, "endpoint listener to close", 5*time.Second, func() bool {
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 500*time.Millisecond)
		if err != nil {
			return true
		}
		conn.Close()
		return false
	})

	// The synthesized default endpoint rejects mutation.
	if st, raw := g.adminStatus(t, http.MethodPatch, "/api/v1/endpoints/default", map[string]any{"addr": "0.0.0.0:9999"}); st == http.StatusOK {
		t.Fatalf("PATCH default endpoint succeeded, want rejection: %s", raw)
	}
}
