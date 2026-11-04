// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package utils

import (
	"CraneFrontEnd/generated/protos"
	"CraneFrontEnd/internal/util"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

// Session owns configuration, caller identity and its connection, including
// certificate renewal. It is used sequentially and must be closed by its owner.
type Session struct {
	config *util.Config
	conn   *grpc.ClientConn
	uid    uint32
}

func OpenSession(ctx context.Context, configPath string) (*Session, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read Crane configuration: %w", err)
	}
	config := &util.Config{}
	if err = yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("parse Crane configuration: %w", err)
	}
	if config.ControlMachineConnectAddr() == "" || config.CraneCtldListenPort == "" {
		return nil, fmt.Errorf("Crane configuration must specify ControlMachine and CraneCtldListenPort")
	}
	if config.CraneBaseDir == "" {
		config.CraneBaseDir = util.DefaultCraneBaseDir
	}
	if config.TlsConfig.Enabled && config.TlsConfig.UserTlsCertPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		config.TlsConfig.UserTlsCertPath = filepath.Join(home, util.DefaultUserConfigPrefix)
	}
	s := &Session{config: config, uid: uint32(os.Getuid())}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Session) connect(ctx context.Context) error {
	var transport credentials.TransportCredentials = insecure.NewCredentials()
	if s.config.TlsConfig.Enabled {
		tlsConfig, err := util.UpdateTLSConfigContext(ctx, s.config)
		if err != nil {
			return err
		}
		transport = credentials.NewTLS(tlsConfig)
	}
	conn, err := grpc.NewClient(net.JoinHostPort(s.config.ControlMachineConnectAddr(), s.config.CraneCtldListenPort),
		grpc.WithTransportCredentials(transport), grpc.WithKeepaliveParams(util.ClientKeepAliveParams), grpc.WithConnectParams(util.ClientConnectParams))
	if err != nil {
		return err
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.conn = conn
	return nil
}

// Query executes a read-only RPC with the real caller UID. A certificate error
// may refresh the connection and retry once. Never use it for mutating RPCs:
// those need a separate, explicitly chosen retry policy.
func (s *Session) Query(ctx context.Context, call func(context.Context, protos.CraneCtldClient, uint32) error) error {
	return retryQuery(ctx, s.config.TlsConfig.Enabled, func() error {
		return call(ctx, protos.NewCraneCtldClient(s.conn), s.uid)
	}, func(ctx context.Context) error {
		if err := util.DoSignAndSaveUserCertificateContext(ctx, s.config); err != nil {
			return err
		}
		return s.connect(ctx)
	})
}

func retryQuery(ctx context.Context, tls bool, call func() error, refresh func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := call()
	if err != nil && tls && (status.Code(err) == codes.Unauthenticated || status.Code(err) == codes.Unavailable && strings.Contains(status.Convert(err).Message(), "certificate")) {
		if cancelErr := ctx.Err(); cancelErr != nil {
			return cancelErr
		}
		if err = refresh(ctx); err != nil {
			return err
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		return call()
	}
	return err
}

func (s *Session) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}
