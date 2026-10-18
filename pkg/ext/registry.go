package ext

import (
	"fmt"
	"sync"
)

// Registry is the compile-time module registry (design §11.2): extensions
// register themselves from init(), and `egress build --with <module>`
// links them into a custom binary. The official release ships only the
// built-in modules.
//
// Registration is by name and panics on duplicates — a duplicate means two
// modules claim the same key, which must fail loudly at startup, not
// silently shadow.
var (
	mu                 sync.RWMutex
	sourceProviders    = make(map[string]SourceProvider)
	attributeProviders = make(map[string]AttributeProvider)
	predicateFactories = make(map[string]PredicateFactory)
	identityResolvers  = make(map[string]IdentityResolver)
	probers            = make(map[string]Prober)
	sinks              = make(map[string]Sink)
	selectors          = make(map[string]Selector)
)

func put[T any](m map[string]T, kind string, name string, v T) {
	mu.Lock()
	defer mu.Unlock()
	if _, dup := m[name]; dup {
		panic(fmt.Sprintf("ext: duplicate %s registration %q", kind, name))
	}
	m[name] = v
}

func get[T any](m map[string]T, name string) (T, bool) {
	mu.RLock()
	defer mu.RUnlock()
	v, ok := m[name]
	return v, ok
}

func all[T any](m map[string]T) map[string]T {
	mu.RLock()
	defer mu.RUnlock()
	out := make(map[string]T, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// RegisterSourceProvider registers a SourceProvider under its Name.
func RegisterSourceProvider(p SourceProvider) {
	put(sourceProviders, "source provider", p.Name(), p)
}

// GetSourceProvider returns the registered provider, if any.
func GetSourceProvider(name string) (SourceProvider, bool) {
	return get(sourceProviders, name)
}

// GetSourceProviders returns a snapshot of all registered providers.
func GetSourceProviders() map[string]SourceProvider { return all(sourceProviders) }

// RegisterAttributeProvider registers an AttributeProvider under its Name.
func RegisterAttributeProvider(p AttributeProvider) {
	put(attributeProviders, "attribute provider", p.Name(), p)
}

// GetAttributeProvider returns the registered provider, if any.
func GetAttributeProvider(name string) (AttributeProvider, bool) {
	return get(attributeProviders, name)
}

// GetAttributeProviders returns a snapshot of all registered providers.
func GetAttributeProviders() map[string]AttributeProvider { return all(attributeProviders) }

// RegisterPredicateFactory registers a PredicateFactory under its Name.
func RegisterPredicateFactory(f PredicateFactory) {
	put(predicateFactories, "predicate factory", f.Name(), f)
}

// GetPredicateFactory returns the registered factory, if any.
func GetPredicateFactory(name string) (PredicateFactory, bool) {
	return get(predicateFactories, name)
}

// GetPredicateFactories returns a snapshot of all registered factories.
func GetPredicateFactories() map[string]PredicateFactory { return all(predicateFactories) }

// RegisterIdentityResolver registers an IdentityResolver under its Name.
func RegisterIdentityResolver(r IdentityResolver) {
	put(identityResolvers, "identity resolver", r.Name(), r)
}

// GetIdentityResolver returns the registered resolver, if any.
func GetIdentityResolver(name string) (IdentityResolver, bool) {
	return get(identityResolvers, name)
}

// GetIdentityResolvers returns a snapshot of all registered resolvers.
func GetIdentityResolvers() map[string]IdentityResolver { return all(identityResolvers) }

// RegisterProber registers a Prober under its Name.
func RegisterProber(p Prober) {
	put(probers, "prober", p.Name(), p)
}

// GetProber returns the registered prober, if any.
func GetProber(name string) (Prober, bool) { return get(probers, name) }

// GetProbers returns a snapshot of all registered probers.
func GetProbers() map[string]Prober { return all(probers) }

// RegisterSink registers a Sink under its Name.
func RegisterSink(s Sink) { put(sinks, "sink", s.Name(), s) }

// GetSink returns the registered sink, if any.
func GetSink(name string) (Sink, bool) { return get(sinks, name) }

// GetSinks returns a snapshot of all registered sinks.
func GetSinks() map[string]Sink { return all(sinks) }

// RegisterSelector registers a Selector under its Name.
func RegisterSelector(s Selector) { put(selectors, "selector", s.Name(), s) }

// GetSelector returns the registered selector, if any.
func GetSelector(name string) (Selector, bool) { return get(selectors, name) }

// GetSelectors returns a snapshot of all registered selectors.
func GetSelectors() map[string]Selector { return all(selectors) }
