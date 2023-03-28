package dfa

type DFA struct {
	root *node
}

type node struct {
	children map[rune]*node
	isEnd    bool
}

func NewDFA(words []string) *DFA {
	root := &node{children: make(map[rune]*node)}
	for _, word := range words {
		cur := root
		for _, c := range word {
			if _, ok := cur.children[c]; !ok {
				cur.children[c] = &node{children: make(map[rune]*node)}
			}
			cur = cur.children[c]
		}
		cur.isEnd = true
	}
	return &DFA{root: root}
}

func (d *DFA) Match(text string) bool {
	cur := d.root
	for _, c := range text {
		if _, ok := cur.children[c]; !ok {
			return false
		}
		cur = cur.children[c]
		if cur.isEnd {
			return true
		}
	}
	return false
}
