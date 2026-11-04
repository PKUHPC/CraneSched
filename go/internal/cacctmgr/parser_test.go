// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package cacctmgr

import "testing"

func TestParseNameAsKeyValueParam(t *testing.T) {
	tests := []struct {
		input   string
		wantID  string
		wantKey string
		wantVal string
	}{
		{"show account Name=acct1", "", "Name", "acct1"},
		{"show account name=acct1,acct2", "", "name", "acct1,acct2"},
		{"show user Account=acct1 Name=user1", "", "Name", "user1"},
		{"show qos Name=normal", "", "Name", "normal"},
		{"delete account Name=acct1", "", "Name", "acct1"},
		{"delete user Name=user1 Account=acct1", "", "Name", "user1"},
		{"delete qos Name=normal", "", "Name", "normal"},
		{"add account Name=acct1,acct2 Description=x", "", "Name", "acct1,acct2"},
		{"reset Name=user1", "", "Name", "user1"},
		{"show account acct1", "acct1", "", ""},
		{"add account acct1 Description=x", "acct1", "Description", "x"},
		{"block user user1 Account=acct1", "user1", "Account", "acct1"},
	}

	for _, tt := range tests {
		command, err := ParseCAcctMgrCommand(tt.input)
		if err != nil {
			t.Errorf("ParseCAcctMgrCommand(%q) returned error: %v", tt.input, err)
			continue
		}
		if got := command.GetID(); got != tt.wantID {
			t.Errorf("ParseCAcctMgrCommand(%q) ID = %q, want %q", tt.input, got, tt.wantID)
		}
		if tt.wantKey == "" {
			continue
		}
		if got := command.GetKVMaps()[tt.wantKey]; got != tt.wantVal {
			t.Errorf("ParseCAcctMgrCommand(%q) %s = %q, want %q", tt.input, tt.wantKey, got, tt.wantVal)
		}
	}
}

func TestGetFilterValueAcceptsKeyValueAndWhere(t *testing.T) {
	tests := []struct {
		input       string
		wantName    string
		wantAccount string
	}{
		{"show user Account=acct1 Name=user1", "user1", "acct1"},
		{"show user where Name=user1 Account=acct1", "user1", "acct1"},
		{"show account where name=acct1", "acct1", ""},
		{"show qos", "", ""},
	}

	for _, tt := range tests {
		command, err := ParseCAcctMgrCommand(tt.input)
		if err != nil {
			t.Fatalf("ParseCAcctMgrCommand(%q) returned error: %v", tt.input, err)
		}
		if got := command.GetFilterValue("name"); got != tt.wantName {
			t.Errorf("%q name filter = %q, want %q", tt.input, got, tt.wantName)
		}
		if got := command.GetFilterValue("account"); got != tt.wantAccount {
			t.Errorf("%q account filter = %q, want %q", tt.input, got, tt.wantAccount)
		}
	}
}
