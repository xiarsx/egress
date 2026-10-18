package wiring

import (
	"strings"
	"unique"

	"github.com/xiarsx/egress/internal/adapters/singbox"
	"github.com/xiarsx/egress/internal/adapters/source"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// ParseSubscriptionToSnapshots parses subscription content into node
// snapshots: the sing-box parser, then NodeSnapshot assembly with the
// content hash filled via source.HashOutbound, tags scoped with the
// "source/" prefix (interned via the unique package, matching the
// registry's scopeTags), and an empty attribute map.
//
// It is the parse callback given to source.NewSyncer. Note the syncer
// re-applies scopeTags itself, so when source is empty no prefix is added
// here and the syncer's own scoping applies — the assembly root therefore
// passes "" and lets the syncer (which knows the source name) do the
// scoping, avoiding a doubled "name//tag" prefix.
func ParseSubscriptionToSnapshots(src string, content []byte) ([]pknode.NodeSnapshot, error) {
	obs, err := singbox.ParseSubscription(content)
	if err != nil {
		return nil, err
	}
	prefix := ""
	if src != "" {
		prefix = src + "/"
	}
	out := make([]pknode.NodeSnapshot, 0, len(obs))
	for _, ob := range obs {
		raw := ob.RawOptions()
		tag := ""
		if t, ok := ob.(singbox.Taggable); ok {
			tag = t.Tag()
		}
		if prefix != "" && !strings.HasPrefix(tag, prefix) {
			tag = prefix + tag
		}
		out = append(out, pknode.NodeSnapshot{
			Hash:        source.HashOutbound(raw),
			RawOptions:  raw,
			HasOutbound: true,
			Tags:        []pknode.ScopedTag{pknode.ScopedTag(unique.Make(tag).Value())},
			Attrs:       make(map[pknode.AttrKey]pknode.AttrValue),
		})
	}
	return out, nil
}
