package expressions

import (
	"fmt"
	"strings"
	"unicode"
)

// token types produced by the YAQL lexer.
type tokType int

const (
	tokEOF tokType = iota
	tokIdent
	tokVar
	tokInt
	tokFloat
	tokString
	tokTrue
	tokFalse
	tokNull
	tokPlus
	tokMinus
	tokStar
	tokSlash
	tokPercent
	tokEq
	tokNeq
	tokGt
	tokLt
	tokGte
	tokLte
	tokAnd
	tokOr
	tokNot
	tokIn
	tokIf
	tokElse
	tokDot
	tokComma
	tokColon
	tokLParen
	tokRParen
	tokLBrack
	tokRBrack
	tokLBrace
	tokRBrace
)

var tokNames = map[tokType]string{
	tokEOF:   "EOF",
	tokIdent: "ident",
	tokVar:   "var",
	tokInt:   "int",
	tokFloat: "float",
	tokString: "string",
	tokTrue:  "true",
	tokFalse: "false",
	tokNull:  "null",
	tokPlus:  "+",
	tokMinus: "-",
	tokStar:  "*",
	tokSlash: "/",
	tokPercent: "%",
	tokEq:    "==",
	tokNeq:   "!=",
	tokGt:    ">",
	tokLt:    "<",
	tokGte:   ">=",
	tokLte:   "<=",
	tokAnd:   "and",
	tokOr:    "or",
	tokNot:   "not",
	tokIn:    "in",
	tokIf:    "if",
	tokElse:  "else",
	tokDot:   ".",
	tokComma: ",",
	tokColon: ":",
	tokLParen: "(",
	tokRParen: ")",
	tokLBrack: "[",
	tokRBrack: "]",
	tokLBrace: "{",
	tokRBrace: "}",
}

func (t tokType) String() string { return tokNames[t] }

type token struct {
	typ tokType
	val interface{}
}

// yaqLexer turns a YAQL expression string into a stream of tokens.
type yaqLexer struct {
	input  string
	pos    int
	tokens []token
}

func newYaqLexer(s string) *yaqLexer { return &yaqLexer{input: s} }

func (l *yaqLexer) Lex() ([]token, error) {
	for l.pos < len(l.input) {
		l.skipWS()
		if l.pos >= len(l.input) {
			break
		}
		ch := l.input[l.pos]
		switch {
		case ch == '$':
			l.tokens = append(l.tokens, l.lexVar())
		case ch == '"' || ch == '\'':
			t, err := l.lexString(ch)
			if err != nil {
				return nil, err
			}
			l.tokens = append(l.tokens, t)
		case unicode.IsDigit(rune(ch)) || (ch == '-' && l.pos+1 < len(l.input) && unicode.IsDigit(rune(l.input[l.pos+1]))):
			l.tokens = append(l.tokens, l.lexNumber())
		case unicode.IsLetter(rune(ch)) || ch == '_':
			l.tokens = append(l.tokens, l.lexIdentOrKeyword())
		default:
			t, err := l.lexOp()
			if err != nil {
				return nil, err
			}
			l.tokens = append(l.tokens, t)
		}
	}
	l.tokens = append(l.tokens, token{typ: tokEOF})
	return l.tokens, nil
}

func (l *yaqLexer) skipWS() {
	for l.pos < len(l.input) && unicode.IsSpace(rune(l.input[l.pos])) {
		l.pos++
	}
}

func (l *yaqLexer) lexVar() token {
	l.pos++ // consume '$'
	var name strings.Builder
	for l.pos < len(l.input) && (unicode.IsLetter(rune(l.input[l.pos])) || unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '_') {
		name.WriteByte(l.input[l.pos])
		l.pos++
	}
	if name.Len() == 0 {
		return token{typ: tokVar, val: "$"} // bare $ = context root
	}
	return token{typ: tokVar, val: "$" + name.String()}
}

func (l *yaqLexer) lexString(quote byte) (token, error) {
	l.pos++ // consume opening quote
	var sb strings.Builder
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\\' && l.pos+1 < len(l.input) {
			next := l.input[l.pos+1]
			switch next {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '\\', '"', '\'':
				sb.WriteByte(next)
			default:
				sb.WriteByte(next)
			}
			l.pos += 2
			continue
		}
		if ch == quote {
			l.pos++
			return token{typ: tokString, val: sb.String()}, nil
		}
		sb.WriteByte(ch)
		l.pos++
	}
	return token{}, fmt.Errorf("unterminated string starting at pos %d", l.pos)
}

func (l *yaqLexer) lexNumber() token {
	start := l.pos
	hasDot := false
	if l.pos < len(l.input) && l.input[l.pos] == '-' {
		l.pos++
	}
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '.' && !hasDot && l.pos+1 < len(l.input) && unicode.IsDigit(rune(l.input[l.pos+1])) {
			hasDot = true
			l.pos++
			continue
		}
		if !unicode.IsDigit(rune(ch)) {
			break
		}
		l.pos++
	}
	raw := l.input[start:l.pos]
	if hasDot {
		var f float64
		fmt.Sscanf(raw, "%f", &f)
		return token{typ: tokFloat, val: f}
	}
	var i int64
	fmt.Sscanf(raw, "%d", &i)
	return token{typ: tokInt, val: i}
}

func (l *yaqLexer) lexIdentOrKeyword() token {
	start := l.pos
	for l.pos < len(l.input) && (unicode.IsLetter(rune(l.input[l.pos])) || unicode.IsDigit(rune(l.input[l.pos])) || l.input[l.pos] == '_') {
		l.pos++
	}
	word := l.input[start:l.pos]
	switch strings.ToLower(word) {
	case "true":
		return token{typ: tokTrue, val: true}
	case "false":
		return token{typ: tokFalse, val: false}
	case "null":
		return token{typ: tokNull, val: nil}
	case "and":
		return token{typ: tokAnd}
	case "or":
		return token{typ: tokOr}
	case "not":
		return token{typ: tokNot}
	case "in":
		return token{typ: tokIn}
	case "if":
		return token{typ: tokIf}
	case "else":
		return token{typ: tokElse}
	}
	return token{typ: tokIdent, val: word}
}

func (l *yaqLexer) lexOp() (token, error) {
	ch := l.input[l.pos]
	l.pos++
	switch ch {
	case '+':
		return token{typ: tokPlus}, nil
	case '-':
		return token{typ: tokMinus}, nil
	case '*':
		return token{typ: tokStar}, nil
	case '/':
		return token{typ: tokSlash}, nil
	case '%':
		return token{typ: tokPercent}, nil
	case '.':
		return token{typ: tokDot}, nil
	case ',':
		return token{typ: tokComma}, nil
	case ':':
		return token{typ: tokColon}, nil
	case '(':
		return token{typ: tokLParen}, nil
	case ')':
		return token{typ: tokRParen}, nil
	case '[':
		return token{typ: tokLBrack}, nil
	case ']':
		return token{typ: tokRBrack}, nil
	case '{':
		return token{typ: tokLBrace}, nil
	case '}':
		return token{typ: tokRBrace}, nil
	case '=':
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
		}
		return token{typ: tokEq}, nil
	case '!':
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return token{typ: tokNeq}, nil
		}
		return token{}, fmt.Errorf("unexpected '!' at pos %d", l.pos-1)
	case '>':
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return token{typ: tokGte}, nil
		}
		return token{typ: tokGt}, nil
	case '<':
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.pos++
			return token{typ: tokLte}, nil
		}
		return token{typ: tokLt}, nil
	}
	return token{}, fmt.Errorf("unexpected character %q at pos %d", ch, l.pos-1)
}
