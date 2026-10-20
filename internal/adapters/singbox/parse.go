// Package singbox parses proxy subscriptions into outbounds and dials
// through them.
//
// Parsing pipeline: base64-wrapped content is unwrapped first, then sing-box
// JSON ({"outbounds":[...]} or a bare array), Clash JSON/YAML
// ({"proxies":[...]} / proxies:), Surge ([Proxy] sections), URI lines and
// finally plain IP:PORT[:USER:PASS] text lines. Each line is parsed
// independently; unparseable lines are skipped and counted.
//
// An Outbound's RawOptions is the canonicalized sing-box outbound JSON
// with the "tag" field removed — it is the stable input for node hashing
// (pkg/node documents xxh3-128 over canonicalized raw options).
package singbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"

	pknode "github.com/xiarsx/egress/pkg/node"
)

// Outbound is one parsed proxy outbound: a dialable proxy chain endpoint.
//
// DialContext dials network/address through the proxy via the embedded
// sing-box outbound engine (engine.go): vmess, vless (incl. REALITY),
// trojan, shadowsocks, socks, http, direct, …; QUIC protocols need
// `-tags with_quic`, REALITY needs `-tags with_utls`. Types without a
// constructor fail closed with ErrUnsupportedOutbound (see dial.go).
type Outbound interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
	// RawOptions returns the canonicalized sing-box outbound JSON without
	// the "tag" field. It is the node-hash input.
	RawOptions() []byte
	// Type returns the sing-box outbound type, e.g. "vmess", "socks", "http".
	Type() string
}

// Taggable is implemented by outbounds that carry their original tag.
type Taggable interface {
	Outbound
	// Tag returns the original tag from the subscription ("" if absent).
	Tag() string
}

// outbound is the concrete Outbound.
type outbound struct {
	outType string
	tag     string
	raw     []byte // canonical JSON, tag removed
}

var _ Taggable = (*outbound)(nil)

func (o *outbound) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ob, err := buildDialOutbound(o.raw)
	if err != nil {
		return nil, err
	}
	return ob.DialContext(ctx, network, address)
}

func (o *outbound) RawOptions() []byte { return o.raw }
func (o *outbound) Type() string       { return o.outType }
func (o *outbound) Tag() string        { return o.tag }

var errEmptySubscription = errors.New("singbox: empty subscription")

// ParseSubscription parses subscription content into outbounds.
//
// Recognition order: base64-wrapped content is decoded first, then
// sing-box JSON, Clash JSON/YAML, Surge, URI lines and plain
// IP:PORT[:USER:PASS] lines. Lines that carry a known scheme but fail to
// parse are skipped silently. An error is returned only when nothing is
// recognized at all.
func ParseSubscription(content []byte) ([]Outbound, error) {
	obs, _, err := ParseSubscriptionWithStats(content)
	return obs, err
}

// ParseSubscriptionWithStats is ParseSubscription plus the number of
// skipped (unparseable) lines.
func ParseSubscriptionWithStats(content []byte) ([]Outbound, int, error) {
	normalized := normalizeInput(content)
	if len(normalized) == 0 {
		return nil, 0, errEmptySubscription
	}

	// Task order: unwrap base64 first. (The guard in tryDecodeBase64ToText
	// rejects anything that is not plausibly base64, so plain JSON / YAML /
	// URI content never takes this branch.)
	if decodedText, ok := tryDecodeBase64ToText(normalized); ok {
		if attempt, err := parseSubscriptionContent([]byte(decodedText)); err != nil {
			return nil, 0, err
		} else if attempt.recognized {
			return convertNodes(attempt.nodes, attempt.skipped)
		}
	}

	attempt, err := parseSubscriptionContent(normalized)
	if err != nil {
		return nil, 0, err
	}
	if !attempt.recognized {
		return nil, 0, errors.New("singbox: unsupported format or no supported nodes found")
	}
	return convertNodes(attempt.nodes, attempt.skipped)
}

// convertNodes turns reference ParsedNodes into Outbounds: the type is
// read from the outbound JSON, the tag is split off, and RawOptions is
// canonicalized without the tag.
func convertNodes(nodes []ParsedNode, skipped int) ([]Outbound, int, error) {
	obs := make([]Outbound, 0, len(nodes))
	for _, n := range nodes {
		ob, ok := nodeToOutbound(n)
		if !ok {
			skipped++
			continue
		}
		obs = append(obs, ob)
	}
	return obs, skipped, nil
}

func nodeToOutbound(n ParsedNode) (*outbound, bool) {
	var header outboundHeader
	if err := json.Unmarshal(n.RawOptions, &header); err != nil {
		return nil, false
	}
	typ := strings.ToLower(strings.TrimSpace(header.Type))
	// Exact boundary of the reference parser: raw sing-box outbounds are
	// filtered by supportedOutboundTypes without case folding, and all
	// converted types are already lowercase.
	if !supportedOutboundTypes[header.Type] || !supportedOutboundTypes[typ] {
		return nil, false
	}
	raw, err := canonicalizeRawOptions(n.RawOptions)
	if err != nil {
		return nil, false
	}
	tag := n.Tag
	if tag == "" {
		tag = header.Tag
	}
	return &outbound{outType: typ, tag: tag, raw: raw}, true
}

// canonicalizeRawOptions re-marshals the outbound JSON with the "tag"
// field removed. encoding/json sorts map keys, so the output is a stable
// canonical form suitable as node-hash input.
func canonicalizeRawOptions(raw json.RawMessage) ([]byte, error) {
	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("singbox: decode outbound options: %w", err)
	}
	delete(m, "tag")
	out, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("singbox: encode outbound options: %w", err)
	}
	return out, nil
}

// ToNodeSnapshots converts parsed outbounds into node snapshots with raw
// (unscoped) tags. The caller (source.Syncer) prefixes tags with the
// source name and fills Hash via source.HashOutbound before publishing.
func ToNodeSnapshots(obs []Outbound) []pknode.NodeSnapshot {
	nodes := make([]pknode.NodeSnapshot, 0, len(obs))
	for _, ob := range obs {
		tag := ""
		if t, ok := ob.(Taggable); ok {
			tag = t.Tag()
		}
		nodes = append(nodes, pknode.NodeSnapshot{
			RawOptions:  ob.RawOptions(),
			HasOutbound: true,
			Tags:        []pknode.ScopedTag{pknode.ScopedTag(tag)},
			Attrs:       make(map[pknode.AttrKey]pknode.AttrValue),
		})
	}
	return nodes
}
