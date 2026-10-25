package expressions

import "fmt"

// Node kinds.
type nodeKind int

const (
	nkLiteral nodeKind = iota
	nkVar              // $, $name
	nkMember           // expr.name
	nkIndex            // expr[index]
	nkCall             // funcName(args...) or expr.method(args...)
	nkUnary            // -, not
	nkBinary           // +, -, *, /, %, ==, !=, >, <, >=, <=, and, or, in
	nkIfElse           // cond ? then : else
	nkList             // [a, b, c]
	nkDict             // {k: v, k2: v2}
)

type node struct {
	kind nodeKind
	// literal
	val interface{}
	// var
	varName string
	// member / call target
	expr *node
	// member / call name
	name string
	// call args / binary right / unary op / ifelse
	args []*node
	// binary op
	op tokType
	// if-else condition, then-branch (args[0]=cond, args[1]=then, args[2]=else)
	// list / dict
	list []*node
	dict []kv
}

type kv struct {
	key *node
	val *node
}

// yaqParser is a recursive descent parser for YAQL.
type yaqParser struct {
	tokens []token
	pos    int
}

func newYaqParser(tokens []token) *yaqParser { return &yaqParser{tokens: tokens} }

func (p *yaqParser) peek() token { return p.tokens[p.pos] }
func (p *yaqParser) advance() token {
	t := p.tokens[p.pos]
	p.pos++
	return t
}
func (p *yaqParser) expect(typ tokType) (token, error) {
	t := p.advance()
	if t.typ != typ {
		return token{}, fmt.Errorf("expected %s got %s", typ, t.typ)
	}
	return t, nil
}
func (p *yaqParser) match(typs ...tokType) bool {
	cur := p.peek()
	for _, t := range typs {
		if cur.typ == t {
			return true
		}
	}
	return false
}

// expression := orExpr ('if' orExpr 'else' orExpr)?  — YAQL uses if-then-else as a postfix expression
func (p *yaqParser) parseExpr() (*node, error) {
	n, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.match(tokIf) {
		p.advance() // consume if
		cond, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		// YAQL: "a if cond else b" means (if cond then a else b)
		// But let's also support Python-style "if cond then a else b"
		// Actually YAQL's postfix is: <expr> if <cond> else <expr>
		// we have n already (expr before 'if'), cond just parsed
		var thenN *node = n
		// consume 'else'
		if !p.match(tokElse) {
			return nil, fmt.Errorf("expected else after condition")
		}
		p.advance()
		elseN, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		// reorder: thenN (which is n) is the "true" branch
		return &node{kind: nkIfElse, args: []*node{cond, thenN, elseN}}, nil
	}
	return n, nil
}

// parseOr handles left-associative 'or'.
func (p *yaqParser) parseOr() (*node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.match(tokOr) {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nkBinary, op: tokOr, expr: left, args: []*node{right}}
	}
	return left, nil
}

func (p *yaqParser) parseAnd() (*node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.match(tokAnd) {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nkBinary, op: tokAnd, expr: left, args: []*node{right}}
	}
	return left, nil
}

func (p *yaqParser) parseNot() (*node, error) {
	if p.match(tokNot) {
		p.advance()
		n, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &node{kind: nkUnary, op: tokNot, expr: n}, nil
	}
	return p.parseCmp()
}

func (p *yaqParser) parseCmp() (*node, error) {
	left, err := p.parseAdd()
	if err != nil {
		return nil, err
	}
	for p.match(tokEq, tokNeq, tokGt, tokLt, tokGte, tokLte, tokIn) {
		op := p.advance().typ
		right, err := p.parseAdd()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nkBinary, op: op, expr: left, args: []*node{right}}
	}
	return left, nil
}

func (p *yaqParser) parseAdd() (*node, error) {
	left, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for p.match(tokPlus, tokMinus) {
		op := p.advance().typ
		right, err := p.parseMul()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nkBinary, op: op, expr: left, args: []*node{right}}
	}
	return left, nil
}

func (p *yaqParser) parseMul() (*node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.match(tokStar, tokSlash, tokPercent) {
		op := p.advance().typ
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &node{kind: nkBinary, op: op, expr: left, args: []*node{right}}
	}
	return left, nil
}

func (p *yaqParser) parseUnary() (*node, error) {
	if p.match(tokMinus, tokPlus) {
		op := p.advance().typ
		n, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &node{kind: nkUnary, op: op, expr: n}, nil
	}
	return p.parsePostfix()
}

// parsePostfix handles primary followed by chains of .member, [index], (args).
func (p *yaqParser) parsePostfix() (*node, error) {
	primary, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		if p.match(tokDot) {
			p.advance()
			// member access OR method call
			if !p.match(tokIdent) {
				return nil, fmt.Errorf("expected identifier after '.'")
			}
			name := p.advance().val.(string)
			if p.match(tokLParen) {
				// method call: expr.method(args)
				args, err := p.parseCallArgs()
				if err != nil {
					return nil, err
				}
				primary = &node{kind: nkCall, expr: primary, name: name, args: args}
			} else {
				primary = &node{kind: nkMember, expr: primary, name: name}
			}
		} else if p.match(tokLBrack) {
			p.advance()
			idx, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokRBrack); err != nil {
				return nil, err
			}
			primary = &node{kind: nkIndex, expr: primary, args: []*node{idx}}
		} else {
			break
		}
	}
	return primary, nil
}

func (p *yaqParser) parsePrimary() (*node, error) {
	t := p.peek()
	switch t.typ {
	case tokInt, tokFloat, tokString, tokTrue, tokFalse, tokNull:
		p.advance()
		return &node{kind: nkLiteral, val: t.val}, nil
	case tokVar:
		p.advance()
		return &node{kind: nkVar, varName: t.val.(string)}, nil
	case tokIdent:
		p.advance()
		name := t.val.(string)
		if p.match(tokLParen) {
			args, err := p.parseCallArgs()
			if err != nil {
				return nil, err
			}
			return &node{kind: nkCall, name: name, args: args}, nil
		}
		// Bare identifier — treat as variable reference (Python/YAQL allow
		// bare identifiers like task(task1) where task1 is a variable name).
		return &node{kind: nkVar, varName: name}, nil
	case tokLParen:
		p.advance()
		n, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokRParen); err != nil {
			return nil, err
		}
		return n, nil
	case tokLBrack:
		return p.parseList()
	case tokLBrace:
		return p.parseDict()
	}
	return nil, fmt.Errorf("unexpected token %s", t.typ)
}

func (p *yaqParser) parseCallArgs() ([]*node, error) {
	if _, err := p.expect(tokLParen); err != nil {
		return nil, err
	}
	var args []*node
	if !p.match(tokRParen) {
		for {
			a, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			if !p.match(tokComma) {
				break
			}
			p.advance()
		}
	}
	if _, err := p.expect(tokRParen); err != nil {
		return nil, err
	}
	return args, nil
}

func (p *yaqParser) parseList() (*node, error) {
	p.advance() // consume [
	var items []*node
	if !p.match(tokRBrack) {
		for {
			n, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			items = append(items, n)
			if !p.match(tokComma) {
				break
			}
			p.advance()
		}
	}
	if _, err := p.expect(tokRBrack); err != nil {
		return nil, err
	}
	return &node{kind: nkList, list: items}, nil
}

func (p *yaqParser) parseDict() (*node, error) {
	p.advance() // consume {
	var pairs []kv
	if !p.match(tokRBrace) {
		for {
			k, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(tokColon); err != nil {
				return nil, err
			}
			v, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			pairs = append(pairs, kv{key: k, val: v})
			if !p.match(tokComma) {
				break
			}
			p.advance()
		}
	}
	if _, err := p.expect(tokRBrace); err != nil {
		return nil, err
	}
	return &node{kind: nkDict, dict: pairs}, nil
}

// ParseYAQL lexes + parses a YAQL expression string into an AST.
func ParseYAQL(expr string) (*node, error) {
	l := newYaqLexer(expr)
	toks, err := l.Lex()
	if err != nil {
		return nil, err
	}
	p := newYaqParser(toks)
	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().typ != tokEOF {
		return nil, fmt.Errorf("unexpected trailing token %s", p.peek().typ)
	}
	return n, nil
}
