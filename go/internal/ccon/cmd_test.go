/**
 * Copyright (c) 2025 Peking University and Peking University
 * Changsha Institute for Computing and Digital Economy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ccon

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestInteractiveCommandsMentionDetachKeysInHelp(t *testing.T) {
	tests := []struct {
		name string
		cmd  *cobra.Command
	}{
		{name: "run", cmd: RunCmd},
		{name: "attach", cmd: AttachCmd},
		{name: "exec", cmd: ExecCmd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			tt.cmd.SetOut(&out)
			tt.cmd.SetErr(&out)

			if err := tt.cmd.Help(); err != nil {
				t.Fatalf("Help() error: %v", err)
			}
			if got := out.String(); !strings.Contains(got, "Ctrl-P + Ctrl-Q") {
				t.Fatalf("help output does not mention detach keys; output:\n%s", got)
			}
		})
	}
}
