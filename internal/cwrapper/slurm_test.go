// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cwrapper

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	"CraneFrontEnd/internal/cacctmgr"
)

func TestNormalizeSacctmgrArgsAcceptsNameFilters(t *testing.T) {
	tests := []struct {
		args       []string
		wantArgs   []string
		wantFilter map[string]string
	}{
		{
			args:       []string{"show", "account", "Name=acct1"},
			wantArgs:   []string{"cacctmgr", "show", "account", "name=acct1"},
			wantFilter: map[string]string{"name": "acct1"},
		},
		{
			args:       []string{"list", "user", "Accounts=acct1", "Names=user1"},
			wantArgs:   []string{"cacctmgr", "show", "user", "account=acct1", "name=user1"},
			wantFilter: map[string]string{"name": "user1", "account": "acct1"},
		},
		{
			args:       []string{"show", "qos", "Name=normal"},
			wantArgs:   []string{"cacctmgr", "show", "qos", "name=normal"},
			wantFilter: map[string]string{"name": "normal"},
		},
		{
			args:       []string{"show", "user", "where", "Name=user1", "Account=acct1"},
			wantArgs:   []string{"cacctmgr", "show", "user", "where", "name=user1", "account=acct1"},
			wantFilter: map[string]string{"name": "user1", "account": "acct1"},
		},
	}

	for _, tt := range tests {
		got := normalizeSacctmgrArgs(tt.args)
		if !slices.Equal(got, tt.wantArgs) {
			t.Errorf("normalizeSacctmgrArgs(%v) = %v, want %v", tt.args, got, tt.wantArgs)
			continue
		}

		command, err := cacctmgr.ParseCAcctMgrCommand(strings.Join(got[1:], " "))
		if err != nil {
			t.Errorf("ParseCAcctMgrCommand(%v) returned error: %v", got[1:], err)
			continue
		}
		for key, want := range tt.wantFilter {
			if value := command.GetFilterValue(key); value != want {
				t.Errorf("%v filter %s = %q, want %q", tt.args, key, value, want)
			}
		}
	}
}

func TestConvertSqueueFormats(t *testing.T) {
	got, err := convertSqueueFormat("%a %C %D %.8i %20j %k %l %M %m %N %n %o %P %p %q %r %S %T %t %u %U %V %State %JobType")
	if err != nil {
		t.Fatalf("convertSqueueFormat() error = %v", err)
	}
	const want = "%a %C %N %.8j %20n %k %l %e %M %L %r %o %P %p %q %R %S %state %statecompact %u %U %s %State %JobType"
	if got != want {
		t.Fatalf("convertSqueueFormat() = %q, want %q", got, want)
	}

	if _, err := convertSqueueFormat("%K"); err == nil || !strings.Contains(err.Error(), "%K") {
		t.Fatalf("convertSqueueFormat(%%K) error = %v, want unsupported format error", err)
	}
}

func TestSqueueHelpMatchesSlurmFormatSemantics(t *testing.T) {
	cmd := squeue()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)

	cmd.Run(cmd, []string{"--help"})
	help := output.String()
	for _, expected := range []string{
		"Supported Slurm format identifiers:",
		"%D/%NodeNum",
		"%i/%JobID",
		"%j/%Name",
		"%M/%ElapsedTime",
		"%m/%ReqMemPerNode",
		"%N/%NodeList",
		"%n/%ReqNodes",
		"%r/%Reason",
		"%t/%StateCompact",
		"%T/%State",
		"%V/%SubmitTime",
		"Supported Crane extension identifiers (long form only):",
		"%StepId",
		"%Wckey",
		"%JobType",
	} {
		if !strings.Contains(help, expected) {
			t.Errorf("squeue --help does not contain %q", expected)
		}
	}
	for _, unexpected := range []string{
		"%c/%AllocCpus",
		"%e/%ElapsedTime",
		"%h/%Held",
		"%i/%StepId",
		"%j/%JobID",
		"%K/%Wckey",
		"%L/%NodeList",
		"%M/%ReqMemPerNode",
		"%m/%AllocMemPerNode",
		"%N/%NodeNum",
		"%n/%Name",
		"%Q/%ReqCpuPerNode",
		"%R/%Reason",
		"%r/%ReqNodes",
		"%s/%SubmitTime",
		"%T/%JobType",
		"%X/%Exclusive",
		"%x/%ExcludeNodes",
	} {
		if strings.Contains(help, unexpected) {
			t.Errorf("squeue --help contains unsupported or incorrect alias %q", unexpected)
		}
	}
}
