// Command egress is the egress gateway binary. All assembly lives in
// package boot (composition root); this file is the stock entry point.
// Custom builds with extensions: `egress build --with <module>` (see
// spec/ext-abi.md §4).
package main

import (
	"os"

	"github.com/xiarsx/egress/boot"
)

func main() {
	os.Exit(boot.Main())
}
