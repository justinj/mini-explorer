package main

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
)

type grp struct {
	name  string
	exprs []expr
	memo  *memo
	done  bool
}

func (g *grp) explore(m *memo) (done bool) {
	numExprs := len(g.exprs)
	// first we optimize every expression (= explore every child)
	for i := 0; i < numExprs; i++ {
		if j, ok := g.exprs[i].(*join); ok {
			for !j.l.done {
				j.l.done = j.l.explore(m)
			}
			for !j.r.done {
				j.r.done = j.r.explore(m)
			}
		}
	}
	// then we explore each group
	for i := 0; i < numExprs; i++ {
		g.exprs[i].explore(m)
	}
	// We're not done if new expressions were added.
	return numExprs == len(g.exprs)
}

func (g *grp) add(e expr) {
	g.exprs = append(g.exprs, e)
}

func (g *grp) String() string {
	var buf bytes.Buffer
	for i, e := range g.exprs {
		if i > 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprintf(&buf, "%s", e.String())
	}

	return buf.String()
}

type expr interface {
	group() *grp
	explore(m *memo)
	String() string
}

type rel struct {
	grp  *grp
	id   int
	name string
}

func (r *rel) group() *grp {
	return r.grp
}

func (r *rel) explore(m *memo) {}

func (r *rel) String() string {
	return r.name
}

type join struct {
	grp *grp
	l   *grp
	r   *grp
}

func (j *join) group() *grp {
	return j.grp
}

func (j *join) explore(m *memo) {
	// [CommuteJoin]
	commuted := join{l: j.r, r: j.l}
	newJ := m.findJoin(commuted)
	if newJ != nil && newJ != j.grp {
		fmt.Println("[CommuteJoin]", j.String(), "=>", commuted.String())
		panic(fmt.Sprintf("commute collision: %s", commuted.String()))
	}
	if newJ == nil {
		fmt.Println("[CommuteJoin]", j.String(), "=>", commuted.String())
		m.internedJoins[commuted] = j.grp
		commuted.grp = j.grp
		j.grp.add(&commuted)
		fmt.Println(m)
	}

	// [AssociateJoin]
	for _, lexpr := range j.l.exprs {
		if ljoin, ok := lexpr.(*join); ok {
			r := m.constructJoin(join{
				l: ljoin.r,
				r: j.r,
			})

			associated := join{
				l: ljoin.l,
				r: r,
			}

			newJ := m.findJoin(associated)

			if newJ != nil && newJ != j.grp {
				fmt.Println("[AssociateJoin]", j.String(), "=>", associated.String())
				panic("associate collision!")
			}
			if newJ == nil {
				fmt.Println("[AssociateJoin]", j.String(), "=>", associated.String())
				m.internedJoins[associated] = j.grp
				associated.grp = j.grp
				j.grp.add(&associated)
				fmt.Println(m)
			}
		}
	}
}

func (j *join) String() string {
	return fmt.Sprintf("(%s.%s)", j.l.name, j.r.name)
}

type memo struct {
	grps          []*grp
	internedRels  map[rel]*grp
	internedJoins map[join]*grp
}

func (m *memo) addRel(r rel) *grp {
	if interned, ok := m.internedRels[r]; ok {
		return interned
	}
	newGroup := &grp{name: r.name, memo: m}
	m.internedRels[r] = newGroup
	r.grp = newGroup
	newGroup.exprs = []expr{&r}

	m.grps = append(m.grps, newGroup)

	return newGroup
}

func (m *memo) findJoin(j join) *grp {
	if interned, ok := m.internedJoins[j]; ok {
		return interned
	}

	return nil
}

func (m *memo) constructRel(name string) *grp {
	r := rel{name: name}
	if r, ok := m.internedRels[r]; ok {
		return r
	}

	g := &grp{name: name, memo: m, exprs: []expr{&r}}
	r.grp = g
	m.internedRels[r] = g

	return g
}

func (m *memo) constructJoin(j join) *grp {
	// [SimplifyZeroCardinalityGroup]
	if j.l.name == "a" && j.r.name == "c" || j.l.name == "c" && j.r.name == "a" {
		r := rel{name: "E", id: 5}
		fmt.Println("[SimplifyZeroCardinalityGroup]", j.String(), "=>", r.name)
		fmt.Println(m)
		return m.addRel(r)
	}

	s := j.l.name + j.r.name
	ss := strings.Split(s, "")
	sort.Strings(ss)
	name := strings.Join(ss, "")

	g := m.findJoin(j)
	if g == nil {
		g = &grp{name: name, memo: m, exprs: []expr{&j}}
		m.grps = append(m.grps, g)
	}

	m.internedJoins[j] = g
	j.grp = g

	return g
}

func (m *memo) String() string {
	lines := make([]string, 0)
	for _, g := range m.grps {
		lines = append(lines, fmt.Sprintf("%4s: %s", g.name, g.String()))
	}
	sort.Strings(lines)

	var buf bytes.Buffer
	for _, l := range lines {
		buf.WriteString(l)
		buf.WriteByte('\n')
	}

	return buf.String()
}

func main() {
	m := &memo{
		internedRels:  make(map[rel]*grp),
		internedJoins: make(map[join]*grp),
	}

	a := m.addRel(rel{name: "a", id: 1})
	b := m.addRel(rel{name: "b", id: 2})
	c := m.addRel(rel{name: "c", id: 3})
	d := m.addRel(rel{name: "d", id: 4})

	ab := m.constructJoin(join{l: a, r: b})
	abc := m.constructJoin(join{l: ab, r: c})
	abcd := m.constructJoin(join{l: abc, r: d})

	fmt.Println(m.String())

	done := false
	for !done {
		done = abcd.explore(m)
	}

	fmt.Println(m.String())

	_ = abcd
}
