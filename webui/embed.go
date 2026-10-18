// Package webui embeds the compiled egress WebUI (React + Vite + TypeScript,
// ported from the Resin project, Copyright (c) 2026 Resinat and contributors,
// MIT License, and rebranded as egress).
//
// Build the frontend before building the Go binary:
//
//	cd webui && npm ci && npm run build
package webui

import (
	"embed"
	"io/fs"
)

// distFS contains the compiled WebUI assets under webui/dist.
//
//go:embed dist
var distFS embed.FS

// DistFS returns an fs.FS rooted at the embedded dist directory.
func DistFS() (fs.FS, error) {
	return fs.Sub(distFS, "dist")
}
