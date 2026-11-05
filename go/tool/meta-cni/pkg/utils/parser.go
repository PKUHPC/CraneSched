package utils

import (
	"fmt"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type Expr struct {
	Delete bool   `parser:"@Minus?"`
	Key    string `parser:"@Ident"`
	Value  string `parser:"( '=' @(QuotedValue | BareValue | Ident) )?"`
}

var lexerRules = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Minus", Pattern: `-`},
	{Name: "Ident", Pattern: `[A-Za-z_][A-Za-z0-9_]*`},
	{Name: "QuotedValue", Pattern: `'(?:[^']*)'|"(?:[^"]*)"`},
	{Name: "BareValue", Pattern: `[^=\s]+`},
	{Name: "Equals", Pattern: `=`},
	{Name: "Whitespace", Pattern: `\s+`},
})

var manipulatorParser = participle.MustBuild[Expr](
	participle.Lexer(lexerRules),
	participle.Elide("Whitespace"),
)

func ParseManipulator(raw string) (*Expr, error) {
	token := strings.TrimSpace(raw)
	if token == "" {
		return nil, fmt.Errorf("empty override expression")
	}

	expr, err := manipulatorParser.ParseString("", token)
	if err != nil {
		return nil, fmt.Errorf("parse override %q: %w", raw, err)
	}

	if expr.Key == "" {
		return nil, fmt.Errorf("override %q missing key", raw)
	}
	if expr.Delete && expr.Value != "" {
		return nil, fmt.Errorf("override %q deletes and assigns simultaneously", raw)
	}
	if !expr.Delete {
		if expr.Value == "" {
			return nil, fmt.Errorf("override %q missing value", raw)
		}
		expr.Value = stripQuotes(expr.Value)
	}

	return expr, nil
}

func ParseManipulators(entries []string) ([]*Expr, error) {
	result := make([]*Expr, 0, len(entries))
	for _, entry := range entries {
		expr, err := ParseManipulator(entry)
		if err != nil {
			return nil, err
		}
		result = append(result, expr)
	}
	return result, nil
}

func stripQuotes(raw string) string {
	if len(raw) < 2 {
		return raw
	}

	if (raw[0] == '"' && raw[len(raw)-1] == '"') || (raw[0] == '\'' && raw[len(raw)-1] == '\'') {
		return raw[1 : len(raw)-1]
	}
	return raw
}
