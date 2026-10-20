// Package predicate compiles platform tag/attribute filters into match
// functions (design §5.4). Compilation happens on the cold path, when a
// platform's filter changes: regexes are precompiled and attribute
// conditions become comparisons. The hot path never evaluates filters —
// it only observes View membership.
//
// Tag syntax (unchanged from the reference):
//
//	no prefix → ANY (OR), "*" prefix → MUST (AND), "!" prefix → MUST_NOT.
//
// Semantics: positive conditions (ANY/MUST) must be satisfied by one and
// the same tag; MUST_NOT inspects every tag and rejects the node on any
// match.
//
// Attribute operators: in / notIn / match / notMatch / gte / lte. A key
// that is not defined on the node counts as "not matching", except that
// notIn and notMatch pass (design §5.4).
package predicate

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/xiarsx/egress/pkg/ext"
	pknode "github.com/xiarsx/egress/pkg/node"
)

// Filter is the declarative filter (mirrors Platform spec.filter).
type Filter struct {
	Tags  []string
	Attrs map[string]AttrCondition
}

// AttrCondition is one attribute predicate; every set field must hold
// (AND). Use the GteIs/LteIs helpers for bounds so presence is explicit.
type AttrCondition struct {
	In       []string
	NotIn    []string
	Match    string // regex source; compiled at Compile time
	NotMatch string // regex source; compiled at Compile time
	Gte      string
	Lte      string
	HasGte   bool
	HasLte   bool
}

// GteIs builds a lower-bound condition.
func GteIs(bound string) AttrCondition { return AttrCondition{Gte: bound, HasGte: true} }

// LteIs builds an upper-bound condition.
func LteIs(bound string) AttrCondition { return AttrCondition{Lte: bound, HasLte: true} }

// Compile validates the filter and returns a match function.
// A bad regex or an empty tag pattern is a Compile-time error, never a
// hot-path failure.
// ExtPrefix marks an attribute key as an extension filter dimension
// (spec/ext-abi.md §2): "ext.<factory>" delegates matching to the
// PredicateFactory registered under <factory>. An unregistered factory
// is a Compile-time error (fail-closed on the cold path).
const ExtPrefix = "ext."

func Compile(f Filter) (pknode.Predicate, error) {
	tf, err := compileTags(f.Tags)
	if err != nil {
		return nil, err
	}
	af, err := compileAttrs(f.Attrs)
	if err != nil {
		return nil, err
	}
	xf, err := compileExt(f.Attrs)
	if err != nil {
		return nil, err
	}
	return func(n pknode.NodeSnapshot) bool {
		return tf.match(n.Tags) && af.match(n.Attrs) && xf.match(n.Attrs)
	}, nil
}

// compileExt compiles the ext.<factory> attribute conditions. The
// built-in operators don't apply to ext keys (the factory owns
// semantics), so they are excluded from compileAttrs.
func compileExt(attrs map[string]AttrCondition) (*extFilter, error) {
	xf := &extFilter{}
	for k, c := range attrs {
		name, ok := strings.CutPrefix(k, ExtPrefix)
		if !ok || name == "" {
			continue
		}
		factory, found := ext.GetPredicateFactory(name)
		if !found {
			return nil, fmt.Errorf("predicate: no predicate factory registered for %q", k)
		}
		fn, err := factory.Compile(c.args())
		if err != nil {
			return nil, fmt.Errorf("predicate: factory %q: %w", k, err)
		}
		xf.preds = append(xf.preds, fn)
	}
	return xf, nil
}

// args maps the declarative condition fields to factory arguments.
func (c AttrCondition) args() map[string]string {
	args := map[string]string{}
	if len(c.In) > 0 {
		args["in"] = strings.Join(c.In, ",")
	}
	if len(c.NotIn) > 0 {
		args["notIn"] = strings.Join(c.NotIn, ",")
	}
	if c.Match != "" {
		args["match"] = c.Match
	}
	if c.NotMatch != "" {
		args["notMatch"] = c.NotMatch
	}
	if c.HasGte {
		args["gte"] = c.Gte
	}
	if c.HasLte {
		args["lte"] = c.Lte
	}
	return args
}

type extFilter struct {
	preds []func(ext.Attrs) bool
}

func (xf *extFilter) match(attrs map[pknode.AttrKey]pknode.AttrValue) bool {
	for _, pred := range xf.preds {
		if !pred(attrs) {
			return false
		}
	}
	return true
}

// --- tags ---

type tagFilter struct {
	any     []*regexp.Regexp
	must    []*regexp.Regexp
	mustNot []*regexp.Regexp
}

func compileTags(rules []string) (*tagFilter, error) {
	tf := &tagFilter{}
	for _, r := range rules {
		pat := r
		var dst *[]*regexp.Regexp
		switch {
		case strings.HasPrefix(r, "*"):
			dst, pat = &tf.must, r[1:]
		case strings.HasPrefix(r, "!"):
			dst, pat = &tf.mustNot, r[1:]
		default:
			dst = &tf.any
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return nil, fmt.Errorf("predicate: bad tag pattern %q: %w", r, err)
		}
		*dst = append(*dst, re)
	}
	return tf, nil
}

// match implements the tag semantics: MUST_NOT rejects on any tag;
// positive rules (ALL must + at least one any) must hold for one and the
// same tag. With no positive rules, only MUST_NOT can reject.
func (tf *tagFilter) match(tags []pknode.ScopedTag) bool {
	hasPositive := len(tf.any) > 0 || len(tf.must) > 0
	matched := false
	for _, t := range tags {
		s := string(t)
		if matchesAny(s, tf.mustNot) {
			return false
		}
		if !matched && matchesAll(s, tf.must) &&
			(len(tf.any) == 0 || matchesAny(s, tf.any)) {
			matched = true
		}
	}
	if !hasPositive {
		return true
	}
	return matched
}

func matchesAny(s string, res []*regexp.Regexp) bool {
	for _, re := range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func matchesAll(s string, res []*regexp.Regexp) bool {
	for _, re := range res {
		if !re.MatchString(s) {
			return false
		}
	}
	return true
}

// --- attributes ---

type attrCond struct {
	in       []string
	notIn    []string
	match    *regexp.Regexp
	notMatch *regexp.Regexp
	gte      string
	lte      string
	hasGte   bool
	hasLte   bool
}

type attrFilter struct {
	conds map[pknode.AttrKey]*attrCond
}

func compileAttrs(attrs map[string]AttrCondition) (*attrFilter, error) {
	af := &attrFilter{conds: make(map[pknode.AttrKey]*attrCond, len(attrs))}
	for k, c := range attrs {
		if strings.HasPrefix(k, ExtPrefix) {
			continue // ext keys are compiled by compileExt
		}
		cc := &attrCond{
			in:     c.In,
			notIn:  c.NotIn,
			gte:    c.Gte,
			lte:    c.Lte,
			hasGte: c.HasGte,
			hasLte: c.HasLte,
		}
		if c.Match != "" {
			re, err := regexp.Compile(c.Match)
			if err != nil {
				return nil, fmt.Errorf("predicate: bad match pattern for attr %q: %w", k, err)
			}
			cc.match = re
		}
		if c.NotMatch != "" {
			re, err := regexp.Compile(c.NotMatch)
			if err != nil {
				return nil, fmt.Errorf("predicate: bad notMatch pattern for attr %q: %w", k, err)
			}
			cc.notMatch = re
		}
		af.conds[pknode.AttrKey(k)] = cc
	}
	return af, nil
}

func (af *attrFilter) match(attrs map[pknode.AttrKey]pknode.AttrValue) bool {
	for k, c := range af.conds {
		v, ok := attrs[k]
		if !ok {
			// Missing key: only pure-negative conditions pass.
			if !c.passesWhenMissing() {
				return false
			}
			continue
		}
		if !c.matchValue(v) {
			return false
		}
	}
	return true
}

// passesWhenMissing reports whether the condition holds for an undefined
// key: only notIn / notMatch pass (design §5.4).
func (c *attrCond) passesWhenMissing() bool {
	return len(c.in) == 0 && c.match == nil && !c.hasGte && !c.hasLte
}

func (c *attrCond) matchValue(v pknode.AttrValue) bool {
	if len(c.in) > 0 && !containsStr(c.in, v.Str) {
		return false
	}
	if len(c.notIn) > 0 && containsStr(c.notIn, v.Str) {
		return false
	}
	if c.match != nil && !c.match.MatchString(v.Str) {
		return false
	}
	if c.notMatch != nil && c.notMatch.MatchString(v.Str) {
		return false
	}
	if c.hasGte && compareBound(v, c.gte) < 0 {
		return false
	}
	if c.hasLte && compareBound(v, c.lte) > 0 {
		return false
	}
	return true
}

// compareBound compares an attribute value against a bound: numeric when
// both sides parse as numbers, lexicographic otherwise.
func compareBound(v pknode.AttrValue, bound string) int {
	if bv, err := strconv.ParseFloat(bound, 64); err == nil && v.IsNum {
		switch {
		case v.Num < bv:
			return -1
		case v.Num > bv:
			return 1
		default:
			return 0
		}
	}
	return strings.Compare(v.Str, bound)
}

func containsStr(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
