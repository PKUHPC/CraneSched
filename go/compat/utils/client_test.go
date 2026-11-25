// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadQueryRetryPolicy(t *testing.T) {
	unauthenticated := status.Error(codes.Unauthenticated, "expired")
	certificate := status.Error(codes.Unavailable, "certificate expired")
	unavailable := status.Error(codes.Unavailable, "connection refused")
	refreshFailure := errors.New("certificate refresh failed")
	for _, tc := range []struct {
		name                   string
		tls                    bool
		first, refresh, second error
		calls, refreshes       int
		want                   error
	}{
		{"success", true, nil, nil, nil, 1, 0, nil},
		{"no_tls", false, unauthenticated, nil, nil, 1, 0, unauthenticated},
		{"transport", true, unavailable, nil, nil, 1, 0, unavailable},
		{"renew_identity", true, unauthenticated, nil, nil, 2, 1, nil},
		{"renew_transport", true, certificate, nil, nil, 2, 1, nil},
		{"refresh_failure", true, certificate, refreshFailure, nil, 1, 1, refreshFailure},
		{"only_once", true, certificate, nil, certificate, 2, 1, certificate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, refreshes := 0, 0
			err := retryQuery(context.Background(), tc.tls, func() error {
				calls++
				if calls == 1 {
					return tc.first
				}
				return tc.second
			}, func(context.Context) error { refreshes++; return tc.refresh })
			if !errors.Is(err, tc.want) || calls != tc.calls || refreshes != tc.refreshes {
				t.Fatalf("err=%v calls=%d refreshes=%d", err, calls, refreshes)
			}
		})
	}
	for _, phase := range []string{"before_query", "after_query", "after_refresh"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls, refreshes := 0, 0
			if phase == "before_query" {
				cancel()
			}
			err := retryQuery(ctx, true, func() error {
				calls++
				if phase == "after_query" {
					cancel()
				}
				return certificate
			}, func(context.Context) error { refreshes++; cancel(); return nil })
			wantCalls, wantRefreshes := 1, 0
			if phase == "before_query" {
				wantCalls = 0
			}
			if phase == "after_refresh" {
				wantRefreshes = 1
			}
			if !errors.Is(err, context.Canceled) || calls != wantCalls || refreshes != wantRefreshes {
				t.Fatalf("err=%v calls=%d refreshes=%d", err, calls, refreshes)
			}
		})
	}
}
