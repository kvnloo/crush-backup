// Command critpath finds the critical path of a Go execution trace, in the
// spirit of Brendan Gregg's off-CPU and wakeup analysis: where the time went,
// how much of it was spent asleep, and what would make the run finish sooner.
//
// Record a trace of Crush's startup with CRUSH_TRACE=startup.trace crush, then:
//
//	go run ./scripts/critpath startup.trace
//	go run ./scripts/critpath -in 42 startup.trace       # one goroutine
//	go run ./scripts/critpath -folded startup.trace      # for flamegraph.pl
//
// The trace is a graph. Each goroutine state transition is a node. Edges are
// program order within a goroutine, a wakeup from the goroutine that unblocked
// another, and a go statement from parent to child. Time spent runnable
// weighs nothing (the run on any number of CPUs). Time spent asleep weighs
// its duration only when nothing in the program woke it (a timer, the network
// poller, a syscall); otherwise the wait is charged to whoever woke it. The
// longest path to the end event is the critical path.
package main

import (
	"bufio"
	"cmp"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"golang.org/x/exp/trace"
)

// endMarkers are the log messages that end a recording, with what the
// analysis then measures.
var endMarkers = map[string]string{
	"first-frame": "crush startup, until the first frame",
	"exit":        "crush shutdown, from quitting until the process exits",
}

type state uint8

const (
	computing state = iota
	asleep
	runnable
	unattributed
)

func (s state) String() string {
	return [...]string{"computing", "asleep", "runnable", "unattributed"}[s]
}

// segment is a stretch of one goroutine's time, carried by an edge.
type segment struct {
	g          trace.GoID
	state      state
	reason     string
	stack      []trace.StackFrame
	start, end trace.Time
}

func (s *segment) dur() time.Duration { return time.Duration(s.end - s.start) }

type edge struct {
	from int
	w    time.Duration
	seg  *segment
}

type node struct {
	t  trace.Time
	in []edge
}

type gor struct {
	id     trace.GoID
	parent trace.GoID
	label  string
	last   int // index of the latest node, -1 before the first
	st     trace.GoState
	reason string
	stack  []trace.StackFrame
	// system marks runtime goroutines such as GC workers. They ready other
	// goroutines as bookkeeping, not because the program depends on them.
	system bool
}

type region struct {
	name       string
	start, end trace.Time
}

type graph struct {
	nodes   []node
	gs      map[trace.GoID]*gor
	regions []region
	start   trace.Time
	stop    trace.Time
	end     int
	title   string
}

func main() {
	in := flag.Int("in", 0, "look inside one goroutine `id`")
	folded := flag.Bool("folded", false, "print the path as folded stacks for flamegraph.pl")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: critpath [-in G] [-folded] file.trace")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	g, err := load(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, "critpath:", err)
		os.Exit(1)
	}
	_, path := g.longest(nil)
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()
	switch {
	case *folded:
		g.printFolded(w, path)
	case *in != 0:
		g.printIn(w, path, trace.GoID(*in), flag.Arg(0))
	default:
		g.printSummary(w, path, flag.Arg(0))
	}
}

func load(name string) (*graph, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r, err := trace.NewReader(bufio.NewReader(f))
	if err != nil {
		return nil, err
	}
	g := &graph{gs: map[trace.GoID]*gor{}, end: -1}
	open := map[trace.GoID][]region{}
	first := true
	for {
		ev, err := r.ReadEvent()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if first {
			g.start, first = ev.Time(), false
		}
		g.stop = ev.Time()
		switch ev.Kind() {
		case trace.EventStateTransition:
			st := ev.StateTransition()
			if st.Resource.Kind == trace.ResourceGoroutine {
				g.transition(ev, st)
			}
		case trace.EventRegionBegin:
			gid := ev.Goroutine()
			open[gid] = append(open[gid], region{name: ev.Region().Type, start: ev.Time()})
		case trace.EventRegionEnd:
			gid := ev.Goroutine()
			if s := open[gid]; len(s) > 0 {
				rg := s[len(s)-1]
				rg.end = ev.Time()
				open[gid] = s[:len(s)-1]
				g.regions = append(g.regions, rg)
			}
		case trace.EventLog:
			if title, ok := endMarkers[ev.Log().Message]; ok && g.end < 0 {
				g.title = title
				who := g.get(ev.Goroutine())
				n := g.add(ev.Time())
				if who.last >= 0 {
					g.link(who, n, nil)
				}
				g.end = n
			}
		}
	}
	if g.end < 0 {
		return nil, errors.New("no end marker; was the trace recorded with CRUSH_TRACE or CRUSH_TRACE_SHUTDOWN?")
	}
	g.stop = g.nodes[g.end].t
	slices.SortFunc(g.regions, func(a, b region) int { return cmp.Compare(a.start, b.start) })
	return g, nil
}

func (g *graph) get(id trace.GoID) *gor {
	x, ok := g.gs[id]
	if !ok {
		x = &gor{id: id, parent: trace.NoGoroutine, last: -1, st: trace.GoUndetermined}
		g.gs[id] = x
	}
	return x
}

func (g *graph) add(t trace.Time) int {
	g.nodes = append(g.nodes, node{t: t})
	return len(g.nodes) - 1
}

// link adds the edge that carries x's current state from its last node up to
// node n. A wait that another goroutine ended carries no weight here: the
// wakeup edge carries it instead. waker is the goroutine executing the
// transition at n, if any.
func (g *graph) link(x *gor, n int, waker *gor) {
	p := g.nodes[x.last]
	seg := &segment{g: x.id, reason: x.reason, stack: x.stack, start: p.t, end: g.nodes[n].t}
	e := edge{from: x.last, seg: seg}
	switch x.st {
	case trace.GoRunning:
		seg.state, e.w = computing, seg.dur()
	case trace.GoSyscall:
		seg.state, e.w = asleep, seg.dur()
		seg.reason = "syscall"
	case trace.GoRunnable:
		seg.state = runnable
	case trace.GoWaiting:
		seg.state = asleep
		if waker == nil || waker == x || waker.system {
			e.w = seg.dur()
		}
	default:
		seg.state, e.w = unattributed, seg.dur()
	}
	g.nodes[n].in = append(g.nodes[n].in, e)
}

func (g *graph) transition(ev trace.Event, st trace.StateTransition) {
	x := g.get(st.Resource.Goroutine())
	from, to := st.Goroutine()
	n := g.add(ev.Time())
	var exec *gor
	if id := ev.Goroutine(); id != trace.NoGoroutine {
		exec = g.get(id)
	}
	// A transition executed by another goroutine (a wakeup or a go
	// statement) depends on everything that goroutine did up to now.
	if exec != nil && exec != x && exec.last >= 0 && !exec.system {
		g.link(exec, n, nil)
	}
	switch {
	case x.last >= 0:
		g.link(x, n, exec)
	case from == trace.GoNotExist && exec != nil:
		x.parent = exec.id
	default:
		// Existed before the trace began, or was started by the runtime. The
		// runtime starts goroutines itself when a timer fires (time.AfterFunc,
		// context deadlines), so a new goroutine without a parent was waiting
		// on a timer.
		seg := &segment{g: x.id, state: unattributed, start: g.start, end: ev.Time()}
		if from == trace.GoNotExist {
			seg.state, seg.reason, seg.stack = asleep, "timer", frames(st.Stack)
		}
		g.nodes[n].in = append(g.nodes[n].in, edge{from: -1, w: seg.dur(), seg: seg})
	}
	if from == trace.GoNotExist || x.label == "" {
		x.label = goLabel(st.Stack)
	}
	x.last, x.st, x.reason = n, to, st.Reason
	if st.Reason == "system goroutine wait" {
		x.system = true
	}
	x.stack = frames(st.Stack)
	if len(x.stack) == 0 && exec == x {
		x.stack = frames(ev.Stack())
	}
}

// longest returns the critical path length and its weighted segments in time
// order. Edges whose segment matches zero weigh nothing, for drag.
func (g *graph) longest(zero func(*segment) bool) (time.Duration, []*segment) {
	dist := make([]time.Duration, len(g.nodes))
	via := make([]edge, len(g.nodes))
	for i := range g.nodes {
		best, ok := time.Duration(-1), false
		for _, e := range g.nodes[i].in {
			w := e.w
			if zero != nil && e.seg != nil && zero(e.seg) {
				w = 0
			}
			d := w
			if e.from >= 0 {
				d += dist[e.from]
			}
			if !ok || d > best {
				best, ok, via[i] = d, true, e
			}
		}
		if ok {
			dist[i] = best
		} else {
			via[i] = edge{from: -1}
		}
	}
	var path []*segment
	for i := g.end; i >= 0; i = via[i].from {
		if s := via[i].seg; s != nil && via[i].w > 0 {
			path = append(path, s)
		}
	}
	slices.Reverse(path)
	return dist[g.end], path
}

func (g *graph) drag(cp time.Duration, zero func(*segment) bool) time.Duration {
	d, _ := g.longest(zero)
	return cp - d
}

func total(path []*segment, keep func(*segment) bool) time.Duration {
	var d time.Duration
	for _, s := range path {
		if keep == nil || keep(s) {
			d += s.dur()
		}
	}
	return d
}

// Output.

func (g *graph) printSummary(w io.Writer, path []*segment, file string) {
	elapsed := time.Duration(g.stop - g.start)
	cp := total(path, nil)
	fmt.Fprintln(w, g.title)
	fmt.Fprintf(w, "  %8s  elapsed\n", dur(elapsed))
	fmt.Fprintf(w, "  %8s  critical path: the least it can take on any number of CPUs\n", dur(cp))
	fmt.Fprintf(w, "            %s\n", stateLine(path, cp))

	fmt.Fprintln(w, "\nThe critical path by phase")
	for _, p := range g.byPhase(path) {
		fmt.Fprintf(w, "  %8s  %-26s %s\n", dur(p.d), p.name, bar(p.d, cp))
	}

	fmt.Fprintln(w, "\nThe critical path by goroutine")
	fmt.Fprintf(w, "  %8s  %6s  %s\n", "on path", "g", "goroutine, with what it started")
	g.printTree(w, path)

	fmt.Fprintln(w, "\nWhat would shorten it")
	fmt.Fprintf(w, "  %8s  %6s  %s\n", "sooner", "g", "if this took no time")
	rows := g.dragRows(path, cp)
	for _, r := range rows[:min(len(rows), 12)] {
		fmt.Fprintf(w, "  %8s  %6s  %s\n", dur(r.drag), r.g, trunc(r.what, 90))
	}

	for _, r := range rows {
		if r.seg != nil {
			fmt.Fprintln(w, "\nWhere the top site's longest stretch was")
			printWhere(w, r.seg)
			break
		}
	}
	fmt.Fprintln(w, "\nLook closer")
	fmt.Fprintf(w, "  critpath -in G %s     one goroutine: its time on the path\n", file)
	fmt.Fprintf(w, "  critpath -folded %s   the path for flamegraph.pl or speedscope.app\n", file)
	fmt.Fprintf(w, "  go tool trace %s      every goroutine on a timeline\n", file)
}

func stateLine(path []*segment, cp time.Duration) string {
	if cp == 0 {
		return ""
	}
	by := map[state]time.Duration{}
	reasons := map[string]time.Duration{}
	for _, s := range path {
		by[s.state] += s.dur()
		if s.state == asleep {
			reasons[reasonName(s.reason)] += s.dur()
		}
	}
	var parts []string
	for _, st := range []state{asleep, computing, unattributed} {
		if by[st] == 0 {
			continue
		}
		p := fmt.Sprintf("%s %s", pct(by[st], cp), st)
		if st == asleep {
			p += " (" + topReasons(reasons, 3) + ")"
		}
		parts = append(parts, p)
	}
	return strings.Join(parts, ", ")
}

func topReasons(m map[string]time.Duration, n int) string {
	keys := slices.SortedFunc(maps.Keys(m), func(a, b string) int { return cmp.Or(cmp.Compare(m[b], m[a]), cmp.Compare(a, b)) })
	var out []string
	for _, k := range keys[:min(n, len(keys))] {
		out = append(out, fmt.Sprintf("%s %s", k, dur(m[k])))
	}
	return strings.Join(out, ", ")
}

func reasonName(r string) string {
	if r == "" {
		return "unknown"
	}
	return r
}

type phase struct {
	name string
	d    time.Duration
}

// byPhase splits the path by the startup regions recorded on the main
// goroutine, naming the gaps around them.
func (g *graph) byPhase(path []*segment) []phase {
	type span struct {
		name       string
		start, end trace.Time
	}
	var spans []span
	cur := g.start
	for i, r := range g.regions {
		if r.start > cur {
			name := "before " + r.name
			if i > 0 {
				name = "between phases"
			}
			spans = append(spans, span{name, cur, r.start})
		}
		spans = append(spans, span{r.name, r.start, r.end})
		cur = max(cur, r.end)
	}
	spans = append(spans, span{"until the end", cur, g.stop})
	acc := map[string]time.Duration{}
	var order []string
	for _, sp := range spans {
		for _, s := range path {
			lo, hi := max(s.start, sp.start), min(s.end, sp.end)
			if hi > lo {
				if _, ok := acc[sp.name]; !ok {
					order = append(order, sp.name)
				}
				acc[sp.name] += time.Duration(hi - lo)
			}
		}
	}
	var out []phase
	for _, n := range order {
		out = append(out, phase{n, acc[n]})
	}
	return out
}

func (g *graph) children() map[trace.GoID][]trace.GoID {
	kids := map[trace.GoID][]trace.GoID{}
	for id, x := range g.gs {
		if x.parent != trace.NoGoroutine {
			kids[x.parent] = append(kids[x.parent], id)
		}
	}
	return kids
}

func (g *graph) printTree(w io.Writer, path []*segment) {
	own := map[trace.GoID]time.Duration{}
	for _, s := range path {
		own[s.g] += s.dur()
	}
	kids := g.children()
	sub := map[trace.GoID]time.Duration{}
	var sum func(trace.GoID) time.Duration
	sum = func(id trace.GoID) time.Duration {
		if d, ok := sub[id]; ok {
			return d
		}
		sub[id] = 0 // Guards against cycles from reused ids.
		d := own[id]
		for _, k := range kids[id] {
			d += sum(k)
		}
		sub[id] = d
		return d
	}
	var roots []trace.GoID
	for id, x := range g.gs {
		if _, ok := g.gs[x.parent]; !ok && sum(id) > 0 {
			roots = append(roots, id)
		}
	}
	byTime := func(a, b trace.GoID) int { return cmp.Or(cmp.Compare(sub[b], sub[a]), cmp.Compare(a, b)) }
	slices.SortFunc(roots, byTime)
	var walk func(id trace.GoID, prefix string, last, root bool)
	walk = func(id trace.GoID, prefix string, last, root bool) {
		branch, next := "", ""
		if !root {
			branch, next = "├─ ", "│  "
			if last {
				branch, next = "└─ ", "   "
			}
		}
		x := g.gs[id]
		note := ""
		if own[id] > 0 {
			note = "  " + ownNote(path, id)
		}
		fmt.Fprintf(w, "  %8s  %6d  %s%s%s\n", dur(sub[id]), id, prefix+branch, trunc(x.label, 50), note)
		var ks []trace.GoID
		for _, k := range kids[id] {
			if sub[k] > 0 {
				ks = append(ks, k)
			}
		}
		slices.SortFunc(ks, byTime)
		const show = 6
		for i, k := range ks[:min(show, len(ks))] {
			walk(k, prefix+next, i == len(ks)-1, false)
		}
		if len(ks) > show {
			var rest time.Duration
			for _, k := range ks[show:] {
				rest += sub[k]
			}
			fmt.Fprintf(w, "  %8s  %6s  %s└─ %d more\n", dur(rest), "", prefix+next, len(ks)-show)
		}
	}
	for _, r := range roots {
		walk(r, "", true, true)
	}
}

// ownNote describes how one goroutine spent its own time on the path.
func ownNote(path []*segment, id trace.GoID) string {
	reasons := map[string]time.Duration{}
	var comp, all time.Duration
	for _, s := range path {
		if s.g != id {
			continue
		}
		all += s.dur()
		switch s.state {
		case asleep:
			reasons[reasonName(s.reason)] += s.dur()
		case computing:
			comp += s.dur()
		}
	}
	var parts []string
	if len(reasons) > 0 {
		parts = append(parts, "asleep in "+topReasons(reasons, 2))
	}
	if comp > all/10 {
		parts = append(parts, "computing "+dur(comp))
	}
	return strings.Join(parts, ", ")
}

type dragRow struct {
	drag time.Duration
	g    string
	what string
	seg  *segment // The longest matching stretch, for printWhere.
}

// dragRows ranks candidates by how much sooner the run would end without
// them: each site where the path slept or computed, and each goroutine with
// everything it started.
func (g *graph) dragRows(path []*segment, cp time.Duration) []dragRow {
	type cand struct {
		key   string
		g     string
		what  string
		match func(*segment) bool
		long  *segment
		on    time.Duration
		n     int
	}
	cands := map[string]*cand{}
	for _, s := range path {
		k := site(s)
		c, ok := cands[k]
		if !ok {
			c = &cand{key: k, g: fmt.Sprint(s.g), match: func(x *segment) bool { return site(x) == k }}
			cands[k] = c
		}
		c.on += s.dur()
		c.n++
		if c.long == nil || s.dur() > c.long.dur() {
			c.long = s
		}
		if c.g != fmt.Sprint(s.g) {
			c.g = ""
		}
	}
	for _, c := range cands {
		c.what = c.key
		if c.n > 1 {
			c.what = fmt.Sprintf("%d× %s", c.n, c.key)
		}
	}
	// Goroutines on the path, with everything they started.
	kids := g.children()
	onPath := map[trace.GoID]bool{}
	for _, s := range path {
		onPath[s.g] = true
	}
	for id := range onPath {
		x := g.gs[id]
		if len(kids[id]) == 0 || x.parent == trace.NoGoroutine {
			continue
		}
		set := map[trace.GoID]bool{}
		var mark func(trace.GoID)
		mark = func(i trace.GoID) {
			if set[i] {
				return
			}
			set[i] = true
			for _, k := range kids[i] {
				mark(k)
			}
		}
		mark(id)
		key := fmt.Sprintf("g%d", id)
		cands[key] = &cand{
			key: key, g: fmt.Sprint(id),
			what:  fmt.Sprintf("%s and the %d goroutines it started", trunc(x.label, 50), len(set)-1),
			match: func(s *segment) bool { return set[s.g] },
			on:    total(path, func(s *segment) bool { return set[s.g] }),
		}
	}
	list := slices.Collect(maps.Values(cands))
	slices.SortFunc(list, func(a, b *cand) int { return cmp.Or(cmp.Compare(b.on, a.on), cmp.Compare(a.key, b.key)) })
	var rows []dragRow
	for _, c := range list[:min(len(list), 25)] {
		if d := g.drag(cp, c.match); d > 0 {
			rows = append(rows, dragRow{drag: d, g: c.g, what: c.what, seg: c.long})
		}
	}
	slices.SortStableFunc(rows, func(a, b dragRow) int { return cmp.Compare(b.drag, a.drag) })
	return rows
}

// site names where a segment's time went: its state and reason, and the
// first frame of the stack that belongs to a module rather than the standard
// library.
func site(s *segment) string {
	where := "somewhere"
	if f, ok := userFrame(s.stack); ok {
		where = shortFunc(f.Func)
	} else if len(s.stack) > 0 {
		where = shortFunc(s.stack[0].Func)
	}
	switch s.state {
	case asleep:
		return fmt.Sprintf("asleep in %s at %s", reasonName(s.reason), where)
	case computing:
		return "computing until " + where
	}
	return s.state.String() + " at " + where
}

func printWhere(w io.Writer, s *segment) {
	fmt.Fprintf(w, "  goroutine %d, %s for %s\n", s.g, site(s), dur(s.dur()))
	f, ok := userFrame(s.stack)
	if !ok {
		for _, fr := range s.stack[:min(len(s.stack), 5)] {
			fmt.Fprintf(w, "    %s (%s:%d)\n", fr.Func, relFile(fr.File), fr.Line)
		}
		return
	}
	fmt.Fprintf(w, "  %s (%s:%d)\n", shortFunc(f.Func), relFile(f.File), f.Line)
	if src, err := os.ReadFile(f.File); err == nil {
		lines := strings.Split(string(src), "\n")
		for n := int(f.Line) - 2; n <= int(f.Line)+1; n++ {
			if n < 1 || n > len(lines) {
				continue
			}
			mark := " "
			if n == int(f.Line) {
				mark = ">"
			}
			fmt.Fprintf(w, "  %s %5d  %s\n", mark, n, strings.ReplaceAll(lines[n-1], "\t", "    "))
		}
	}
	seen, n := false, 0
	for _, fr := range s.stack {
		if fr == f {
			seen = true
			continue
		}
		if seen && n < 3 {
			fmt.Fprintf(w, "  called from %s (%s:%d)\n", shortFunc(fr.Func), relFile(fr.File), fr.Line)
			n++
		}
	}
}

func (g *graph) printIn(w io.Writer, path []*segment, id trace.GoID, file string) {
	x, ok := g.gs[id]
	if !ok {
		fmt.Fprintf(w, "no goroutine %d in %s\n", id, file)
		return
	}
	cp := total(path, nil)
	mine := total(path, func(s *segment) bool { return s.g == id })
	fmt.Fprintf(w, "%s (goroutine %d)\n", x.label, id)
	fmt.Fprintf(w, "  %8s  on the critical path, %s of the %s path\n", dur(mine), pct(mine, cp), dur(cp))
	var segs []*segment
	for _, s := range path {
		if s.g == id {
			segs = append(segs, s)
		}
	}
	if len(segs) == 0 {
		return
	}
	fmt.Fprintf(w, "            %s\n", stateLine(segs, mine))
	fmt.Fprintln(w, "\nIts stretches on the path, longest first")
	slices.SortFunc(segs, func(a, b *segment) int { return cmp.Compare(b.dur(), a.dur()) })
	for _, s := range segs[:min(len(segs), 10)] {
		at := time.Duration(s.start - g.start)
		fmt.Fprintf(w, "  %8s  at +%-9s %s\n", dur(s.dur()), dur(at), site(s))
	}
	fmt.Fprintln(w, "\nWhere the longest was")
	printWhere(w, segs[0])
}

// printFolded writes one line per path segment: the goroutine ancestry, the
// state and the stack, root first, for flamegraph.pl.
func (g *graph) printFolded(w io.Writer, path []*segment) {
	acc := map[string]time.Duration{}
	for _, s := range path {
		var parts []string
		for id := s.g; id != trace.NoGoroutine && len(parts) < 32; {
			x, ok := g.gs[id]
			if !ok {
				break
			}
			parts = append(parts, fmt.Sprintf("g%d %s", id, x.label))
			id = x.parent
		}
		slices.Reverse(parts)
		label := s.state.String()
		if s.state == asleep {
			label += " " + reasonName(s.reason)
		}
		parts = append(parts, label)
		for i := len(s.stack) - 1; i >= 0; i-- {
			parts = append(parts, shortFunc(s.stack[i].Func))
		}
		for i := range parts {
			parts[i] = strings.ReplaceAll(parts[i], ";", ",")
		}
		acc[strings.Join(parts, ";")] += s.dur()
	}
	for _, k := range slices.Sorted(maps.Keys(acc)) {
		fmt.Fprintf(w, "%s %d\n", k, acc[k].Microseconds())
	}
}

// Helpers.

func frames(s trace.Stack) []trace.StackFrame {
	var out []trace.StackFrame
	for f := range s.Frames() {
		out = append(out, f)
	}
	return out
}

func goLabel(s trace.Stack) string {
	fs := frames(s)
	if f, ok := userFrame(fs); ok {
		return shortFunc(f.Func)
	}
	if len(fs) > 0 {
		return shortFunc(fs[len(fs)-1].Func)
	}
	return "goroutine"
}

// userFrame returns the innermost frame from a module, skipping the standard
// library, whose package paths never contain a dot in their first element.
func userFrame(fs []trace.StackFrame) (trace.StackFrame, bool) {
	for _, f := range fs {
		first, _, nested := strings.Cut(f.Func, "/")
		if strings.HasPrefix(f.Func, "main.") || nested && strings.Contains(first, ".") {
			return f, true
		}
	}
	return trace.StackFrame{}, false
}

func shortFunc(f string) string {
	for _, p := range []string{"github.com/charmbracelet/crush/internal/", "github.com/charmbracelet/", "charm.land/"} {
		if s, ok := strings.CutPrefix(f, p); ok {
			return s
		}
	}
	return f
}

func relFile(f string) string {
	if wd, err := os.Getwd(); err == nil {
		if s, ok := strings.CutPrefix(f, wd+"/"); ok {
			return s
		}
	}
	if i := strings.Index(f, "/pkg/mod/"); i >= 0 {
		return f[i+len("/pkg/mod/"):]
	}
	return f
}

func dur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2f s", d.Seconds())
	case d >= 10*time.Millisecond:
		return fmt.Sprintf("%.0f ms", float64(d)/float64(time.Millisecond))
	case d >= time.Millisecond:
		return fmt.Sprintf("%.1f ms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%d µs", d.Microseconds())
	}
}

func pct(a, b time.Duration) string {
	if b == 0 {
		return "0%"
	}
	p := 100 * float64(a) / float64(b)
	if p > 0 && p < 1 {
		return "<1%"
	}
	return fmt.Sprintf("%.0f%%", p)
}

func bar(a, b time.Duration) string {
	if b == 0 {
		return ""
	}
	return strings.Repeat("█", max(1, int(30*float64(a)/float64(b)+0.5)))
}

func trunc(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}
