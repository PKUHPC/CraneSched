package calloc

import (
	"errors"
	"strings"
	"testing"

	"github.com/PKUHPC/CraneSched/go/internal/util"
	"github.com/spf13/cobra"
)

func TestMainCallocRejectsInvalidSignal(t *testing.T) {
	oldSignal := FlagSignal
	t.Cleanup(func() { FlagSignal = oldSignal })

	for _, tt := range []struct {
		input string
		why   string
	}{
		{"", "empty signal"},
		{"R:USR1@65536", "invalid signal time"},
		{"B:USR1@1", "B: is only supported for batch submissions"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().StringVar(&FlagSignal, "signal", "", "")
			if err := cmd.ParseFlags([]string{"--signal=" + tt.input}); err != nil {
				t.Fatal(err)
			}
			err := MainCalloc(cmd, nil)
			argErr, ok := errors.AsType[*util.CraneError](err)
			if !ok || argErr.Code != util.ErrorCmdArg || !strings.Contains(argErr.Message, "--signal: "+tt.why) {
				t.Fatalf("got %v, want a --signal argument error containing %q", err, tt.why)
			}
		})
	}
}
