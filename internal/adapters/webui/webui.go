// Package webui 提供内嵌的单页管理后台。
//
// 前端为纯手写 HTML/CSS/JS（无 npm、无构建步骤、无外部 CDN），通过 go:embed
// 打包进二进制，单二进制零外部依赖。调用方示例：
//
//	mux.Handle("/ui", webui.Handler())
//	mux.Handle("/ui/", webui.Handler())
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed static/*
var staticFS embed.FS

// Handler 返回 WebUI 的 http.Handler。
// 假设被挂载在 /ui/ 下：根路径 /ui/ 返回 index.html，其余路径返回对应静态文件。
func Handler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		// embed 内容在编译期已确定，理论上不会失败。
		panic("webui: " + err.Error())
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case p == "/ui":
			http.Redirect(w, r, "/ui/", http.StatusFound)
		case p == "/ui/" || strings.HasPrefix(p, "/ui/"):
			rest := strings.TrimPrefix(p, "/ui")
			if rest == "" {
				rest = "/"
			}
			// index.html 不缓存，保证升级后立即看到新版 UI。
			if rest == "/" || rest == "/index.html" {
				w.Header().Set("Cache-Control", "no-store")
			}
			r2 := r.Clone(r.Context())
			r2.URL.Path = rest
			fileServer.ServeHTTP(w, r2)
		default:
			http.NotFound(w, r)
		}
	})
}
