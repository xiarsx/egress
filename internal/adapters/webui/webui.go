// Package webui 提供内嵌的单页管理后台。
//
// 前端为 React + Vite + TypeScript 应用（见仓库根目录 webui/，构建产物
// webui/dist 通过 go:embed 打包进二进制），单二进制零外部依赖。调用方示例：
//
//	mux.Handle("/ui", webui.Handler())
//	mux.Handle("/ui/", webui.Handler())
package webui

import (
	"io/fs"
	"net/http"
	"strings"

	frontend "github.com/xiarsx/egress/webui"
)

// Handler 返回 WebUI 的 http.Handler。
// 假设被挂载在 /ui/ 下：根路径 /ui/ 返回 index.html，其余路径返回对应静态文件。
func Handler() http.Handler {
	sub, err := frontend.DistFS()
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
			// SPA fallback：前端使用 BrowserRouter（basename /ui），深链接
			//（如 /ui/dashboard）由前端路由处理。若路径在 dist 中没有对应
			// 的静态文件，则直接返回 index.html 内容（200，保留深链接 URL）；
			// 带扩展名的缺失资源仍返回 404。
			if _, err := fs.Stat(sub, strings.TrimPrefix(rest, "/")); err != nil && !strings.Contains(rest, ".") {
				data, err := fs.ReadFile(sub, "index.html")
				if err != nil {
					http.NotFound(w, r)
					return
				}
				// index.html 不缓存，保证升级后立即看到新版 UI。
				w.Header().Set("Cache-Control", "no-store")
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				w.Write(data)
				return
			}
			r2.URL.Path = rest
			fileServer.ServeHTTP(w, r2)
		default:
			http.NotFound(w, r)
		}
	})
}
