//go:build !with_quic

package singbox

// quicCompiled reports whether QUIC protocols (hysteria, hysteria2, tuic)
// are compiled into this binary (build with `-tags with_quic`).
const quicCompiled = false
