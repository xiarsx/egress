# stdout-sink — example egress extension

An example `pkg/ext` **Sink** module: prints every event (signals and
request logs) as one JSON line on stdout.

## Use

Import for side effects in a custom build:

```go
import _ "github.com/xiarsx/egress/examples/stdout-sink"
```

The module registers itself under the name `stdout` via `init()`:

```go
ext.RegisterSink(&stdoutSink{...})
```

Look it up any time with `ext.GetSink("stdout")`.

## Notes

- Example-grade, not production-grade: `Consume` serializes on a mutex
  around a slow writer. Per the Sink contract it still never blocks the
  data plane (the bus drops on a full subscriber buffer), but a real sink
  should shed load internally instead.
- See `pkg/ext/doc.go` for the hot-path discipline and stability levels.
