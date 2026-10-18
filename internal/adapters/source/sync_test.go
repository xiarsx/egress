package source

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xiarsx/egress/internal/ports"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// ---------------------------------------------------------------------------
// fakes
// ---------------------------------------------------------------------------

type fakeFetcher struct {
	mu      sync.Mutex
	content map[string][]byte
	err     map[string]error
	calls   []string
}

func (f *fakeFetcher) Fetch(_ context.Context, url string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, url)
	if err, ok := f.err[url]; ok {
		return nil, err
	}
	return f.content[url], nil
}

type scheduledTask struct {
	delay time.Duration
	task  ports.Task
}

type fakeScheduler struct {
	mu    sync.Mutex
	tasks []scheduledTask
}

func (s *fakeScheduler) Schedule(delay time.Duration, task ports.Task) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = append(s.tasks, scheduledTask{delay: delay, task: task})
}

func (s *fakeScheduler) ScheduleAt(t time.Time, task ports.Task) {
	s.Schedule(time.Until(t), task)
}

// runNext runs the earliest scheduled task synchronously.
func (s *fakeScheduler) runNext(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	if len(s.tasks) == 0 {
		s.mu.Unlock()
		t.Fatal("no scheduled tasks")
	}
	task := s.tasks[0].task
	s.tasks = s.tasks[1:]
	s.mu.Unlock()
	task(context.Background())
}

func (s *fakeScheduler) delays() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]time.Duration, 0, len(s.tasks))
	for _, st := range s.tasks {
		out = append(out, st.delay)
	}
	return out
}

// testParse converts subscription bytes to snapshots via the singbox
// pipeline shape: parse → snapshots with raw tags, hash filled by Syncer.
func testParse(content []byte) ([]pknode.NodeSnapshot, error) {
	text := string(content)
	if text == "bad" {
		return nil, errors.New("parse boom")
	}
	if text == "" {
		return nil, nil
	}
	return []pknode.NodeSnapshot{{
		RawOptions:  []byte(`{"type":"http","server":"10.9.0.1","server_port":8080}`),
		HasOutbound: true,
		Tags:        []pknode.ScopedTag{"web"},
	}}, nil
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", msg)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestSyncer_LocalSource(t *testing.T) {
	reg := NewRegistry()
	fetcher := &fakeFetcher{}
	sched := &fakeScheduler{}
	s := NewSyncer(reg, fetcher, sched, testParse)

	s.AddSource(ports.SourceSpec{
		Name:           "local1",
		Kind:           "local",
		Content:        "whatever",
		UpdateInterval: time.Minute,
	})

	// Immediate async refresh publishes the snapshot.
	waitFor(t, func() bool { return len(reg.All()) == 1 }, "local source snapshot")
	n := reg.All()[0]
	if len(n.Tags) != 1 || n.Tags[0] != "local1/web" {
		t.Fatalf("tags = %v, want [local1/web]", n.Tags)
	}
	if n.Health.Kind != pknode.HealthProbing {
		t.Fatalf("health = %v, want Probing", n.Health.Kind)
	}
	if n.Hash == pknode.Zero {
		t.Fatal("hash not filled by Syncer")
	}
	if s.LastError("local1") != "" {
		t.Fatalf("LastError = %q, want empty", s.LastError("local1"))
	}
	// One periodic task scheduled at the requested interval.
	if delays := sched.delays(); len(delays) != 1 || delays[0] != time.Minute {
		t.Fatalf("scheduled delays = %v, want [1m]", delays)
	}
}

func TestSyncer_RemoteFetchAndClamp(t *testing.T) {
	reg := NewRegistry()
	fetcher := &fakeFetcher{content: map[string][]byte{"https://sub.example/sub": []byte("x")}}
	sched := &fakeScheduler{}
	s := NewSyncer(reg, fetcher, sched, testParse)

	s.AddSource(ports.SourceSpec{
		Name:           "remote1",
		Kind:           "remote",
		URL:            "https://sub.example/sub",
		UpdateInterval: 5 * time.Second, // below minimum → clamped
	})
	waitFor(t, func() bool { return len(reg.All()) == 1 }, "remote source snapshot")

	if delays := sched.delays(); len(delays) != 1 || delays[0] < minRemoteInterval {
		t.Fatalf("scheduled delays = %v, want >= 30s", delays)
	}

	// A second immediate refresh inside the 30s window is rejected by the
	// provider clamp, and the old snapshot is kept.
	err := s.RefreshNow("remote1")
	if !errors.Is(err, ErrFetchTooFrequent) {
		t.Fatalf("RefreshNow = %v, want ErrFetchTooFrequent", err)
	}
	if len(reg.All()) != 1 {
		t.Fatal("snapshot should be kept on fetch failure")
	}
	if s.LastError("remote1") == "" {
		t.Fatal("LastError empty after failed refresh")
	}
}

func TestSyncer_FetchFailureKeepsSnapshot(t *testing.T) {
	reg := NewRegistry()
	fetcher := &fakeFetcher{content: map[string][]byte{"https://sub.example/sub": []byte("x")}, err: map[string]error{}}
	sched := &fakeScheduler{}
	s := NewSyncer(reg, fetcher, sched, testParse)

	s.AddSource(ports.SourceSpec{Name: "r", Kind: "remote", URL: "https://sub.example/sub", UpdateInterval: time.Hour})
	waitFor(t, func() bool { return len(reg.All()) == 1 }, "initial snapshot")

	fetcher.mu.Lock()
	fetcher.err["https://sub.example/sub"] = errors.New("network down")
	fetcher.mu.Unlock()

	if err := s.RefreshNow("r"); err == nil {
		t.Fatal("expected fetch error")
	}
	if len(reg.All()) != 1 {
		t.Fatal("old snapshot must be kept on download failure")
	}
	if s.LastError("r") == "" {
		t.Fatal("LastError empty after download failure")
	}

	// Recovery clears the error.
	fetcher.mu.Lock()
	delete(fetcher.err, "https://sub.example/sub")
	fetcher.mu.Unlock()
	// Bypass the 30s clamp for the recovery check.
	s.remotes.mu.Lock()
	delete(s.remotes.last, "r")
	s.remotes.mu.Unlock()
	if err := s.RefreshNow("r"); err != nil {
		t.Fatalf("recovery refresh: %v", err)
	}
	if s.LastError("r") != "" {
		t.Fatalf("LastError = %q after recovery", s.LastError("r"))
	}
}

func TestSyncer_ParseFailureKeepsSnapshot(t *testing.T) {
	reg := NewRegistry()
	fetcher := &fakeFetcher{}
	sched := &fakeScheduler{}
	s := NewSyncer(reg, fetcher, sched, testParse)

	s.AddSource(ports.SourceSpec{Name: "l", Kind: "local", Content: "good", UpdateInterval: time.Hour})
	waitFor(t, func() bool { return len(reg.All()) == 1 }, "initial snapshot")

	s.mu.Lock()
	s.specs["l"] = ports.SourceSpec{Name: "l", Kind: "local", Content: "bad", UpdateInterval: time.Hour}
	s.mu.Unlock()
	if err := s.RefreshNow("l"); err == nil {
		t.Fatal("expected parse error")
	}
	if len(reg.All()) != 1 {
		t.Fatal("old snapshot must be kept on parse failure")
	}
	if s.LastError("l") == "" {
		t.Fatal("LastError empty after parse failure")
	}
}

func TestSyncer_RemoveSource(t *testing.T) {
	reg := NewRegistry()
	fetcher := &fakeFetcher{}
	sched := &fakeScheduler{}
	s := NewSyncer(reg, fetcher, sched, testParse)

	s.AddSource(ports.SourceSpec{Name: "l", Kind: "local", Content: "x", UpdateInterval: time.Minute})
	waitFor(t, func() bool { return len(reg.All()) == 1 }, "initial snapshot")

	s.RemoveSource("l")
	if len(reg.All()) != 0 {
		t.Fatalf("All() = %d after RemoveSource, want 0", len(reg.All()))
	}
	if len(reg.SnapshotOf("l")) != 0 {
		t.Fatal("SnapshotOf not withdrawn")
	}
	// Stale scheduled chains must not republish.
	for range sched.delays() {
		sched.runNext(t)
	}
	if len(reg.All()) != 0 {
		t.Fatal("stale schedule chain republished after RemoveSource")
	}
}

func TestSyncer_PeriodicChainContinues(t *testing.T) {
	reg := NewRegistry()
	fetcher := &fakeFetcher{}
	sched := &fakeScheduler{}
	s := NewSyncer(reg, fetcher, sched, testParse)

	s.AddSource(ports.SourceSpec{Name: "l", Kind: "local", Content: "x", UpdateInterval: time.Minute})
	waitFor(t, func() bool { return len(reg.All()) == 1 }, "initial snapshot")

	// Firing the scheduled task refreshes and re-arms the next one.
	sched.runNext(t)
	waitFor(t, func() bool { return len(sched.delays()) == 1 }, "re-armed schedule")
}

func TestSyncer_UnknownKind(t *testing.T) {
	reg := NewRegistry()
	s := NewSyncer(reg, &fakeFetcher{}, &fakeScheduler{}, testParse)
	s.AddSource(ports.SourceSpec{Name: "weird", Kind: "ftp", UpdateInterval: time.Hour})
	waitFor(t, func() bool { return s.LastError("weird") != "" }, "unknown kind error")
	if len(reg.All()) != 0 {
		t.Fatal("no snapshot expected for unknown kind")
	}
}
