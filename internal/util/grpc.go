/**
 * Copyright (c) 2024 Peking University and Peking University
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

package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/fs"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"

	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

var ServerKeepAliveParams = keepalive.ServerParameters{
	Time:    10 * time.Minute, // GRPC_ARG_KEEPALIVE_TIME_MS
	Timeout: 20 * time.Second, // GRPC_ARG_KEEPALIVE_TIMEOUT_MS
}

var ServerKeepAlivePolicy = keepalive.EnforcementPolicy{
	MinTime:             10 * time.Second, // GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS
	PermitWithoutStream: true,             // GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS
	//No MaxPingStrikes available default is 2
}

var ClientKeepAliveParams = keepalive.ClientParameters{
	Time:                20 * time.Second, // 20s GRPC_ARG_KEEPALIVE_TIME_MS
	Timeout:             10 * time.Second, // 10s GRPC_ARG_KEEPALIVE_TIMEOUT_MS
	PermitWithoutStream: true,             // GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS
}

var ClientConnectParams = grpc.ConnectParams{
	Backoff: backoff.Config{
		BaseDelay: 1 * time.Second,  // 1s GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS
		MaxDelay:  30 * time.Second, // 30s GRPC_ARG_MAX_RECONNECT_BACKOFF_MS
		//No min delay available GRPC_ARG_MIN_RECONNECT_BACKOFF_MS
	},
}

type UnixPeerAuthInfo struct {
	UID uint32
	GID uint32
	PID int32
}

func (a *UnixPeerAuthInfo) AuthType() string { return "unix-peer" }

type UnixPeerCredentials struct{}

func (c *UnixPeerCredentials) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {

	uconn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, nil, fmt.Errorf("not a unix socket")
	}

	file, err := uconn.File()
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	ucred, err := syscall.GetsockoptUcred(int(file.Fd()), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	if err != nil {
		return nil, nil, err
	}

	return conn, &UnixPeerAuthInfo{
		UID: ucred.Uid,
		GID: ucred.Gid,
		PID: ucred.Pid,
	}, nil
}

func (c *UnixPeerCredentials) ClientHandshake(ctx context.Context, addr string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {

	return conn, &UnixPeerAuthInfo{}, nil
}

func (c *UnixPeerCredentials) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "unix-peer"}
}
func (c *UnixPeerCredentials) Clone() credentials.TransportCredentials { return &UnixPeerCredentials{} }
func (c *UnixPeerCredentials) OverrideServerName(s string) error       { return nil }

func GetTCPSocket(bindAddr string, config *Config) (net.Listener, error) {
	if config.TlsConfig.Enabled {
		CaCertContent, err := os.ReadFile(config.TlsConfig.CaFilePath)
		if err != nil {
			return nil, err
		}

		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
			return nil, err
		}

		cert, err := tls.LoadX509KeyPair(config.TlsConfig.InternalCertFilePath, config.TlsConfig.InternalKeyFilePath)
		if err != nil {
			return nil, err
		}

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caPool,

			// NextProtos is a list of supported application level protocols, in
			// order of preference.
			// It MUST be filled. Otherwise, when c++ clients try to connect this
			// server, "Cannot check peer: missing selected ALPN property" error
			// will occur!!!!
			NextProtos: []string{"h2"},
		}
		listen, err := tls.Listen("tcp", bindAddr, tlsConfig)
		if err != nil {
			return nil, err
		}
		return listen, nil
	} else {
		listen, err := net.Listen("tcp", bindAddr)
		if err != nil {
			return nil, err
		}
		return listen, nil
	}
}

func GetUnixSocket(path string, mode fs.FileMode) (net.Listener, error) {
	dir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, err
	}

	ok := RemoveFileIfExists(path)
	if !ok {
		return nil, fmt.Errorf("error when removing existing unix socket")
	}

	socket, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}

	// 0600 -> only owner can access, no need to use TLS
	// 0666 -> everyone can access, insecure
	if err = os.Chmod(path, mode); err != nil {
		return nil, err
	}

	return socket, nil
}

// TODO: Refactor this to return ErrCodes instead of exiting.
func GetStubToCtldByConfig(config *Config) protos.CraneCtldClient {
	var serverAddr string
	var stub protos.CraneCtldClient

	if config.TlsConfig.Enabled {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachineConnectAddr(), config.CraneCtldListenPort)

		if config.TlsConfig.UserTlsCertPath == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				log.Fatal(err.Error())
			}
			config.TlsConfig.UserTlsCertPath = filepath.Join(home, DefaultUserConfigPrefix)
		}

		refreshCertificateFunc := func() error {
			return DoSignAndSaveUserCertificate(config)
		}

		updateConnFunc := func() (*grpc.ClientConn, error) {
			tlsConfig, err := UpdateTLSConfig(config)
			if err != nil {
				return nil, err
			}
			return grpc.NewClient(serverAddr, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		}

		tlsConfig, err := UpdateTLSConfig(config)
		if err != nil {
			log.Fatalf("Failed to load user certificate: %v", err)
		}

		conn, err := grpc.NewClient(serverAddr,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			grpc.WithUnaryInterceptor(RefreshCertInterceptor(refreshCertificateFunc, updateConnFunc)),
			grpc.WithKeepaliveParams(ClientKeepAliveParams),
			grpc.WithConnectParams(ClientConnectParams),
			grpc.WithIdleTimeout(time.Duration(math.MaxInt64)),
		)
		if err != nil {
			log.Errorln("Cannot connect to CraneCtld: " + err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldClient(conn)
	} else {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachineConnectAddr(), config.CraneCtldListenPort)

		conn, err := grpc.NewClient(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(ClientKeepAliveParams),
			grpc.WithConnectParams(ClientConnectParams),
			grpc.WithIdleTimeout(time.Duration(math.MaxInt64)), // GRPC_ARG_CLIENT_IDLE_TIMEOUT_MS
		)
		if err != nil {
			log.Errorf("Cannot connect to CraneCtld %s: %s", serverAddr, err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldClient(conn)
	}

	return stub
}

func GetStubToCtldForInternalByConfig(config *Config) (*grpc.ClientConn, protos.CraneCtldForInternalClient) {
	var serverAddr string
	var conn *grpc.ClientConn
	var err error
	var stub protos.CraneCtldForInternalClient

	if config.TlsConfig.Enabled {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachineConnectAddr(), config.CraneCtldForInternalListenPort)

		ServerCertContent, err := os.ReadFile(config.TlsConfig.InternalCertFilePath)
		if err != nil {
			log.Errorln("Read server certificate error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		ServerKeyContent, err := os.ReadFile(config.TlsConfig.InternalKeyFilePath)
		if err != nil {
			log.Errorln("Read server key error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		CaCertContent, err := os.ReadFile(config.TlsConfig.CaFilePath)
		if err != nil {
			log.Errorln("Read CA certifacate error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		tlsKeyPair, err := tls.X509KeyPair(ServerCertContent, ServerKeyContent)
		if err != nil {
			log.Errorln("tlsKeyPair error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		caPool := x509.NewCertPool()
		if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
			log.Errorln("AppendCertsFromPEM error: " + err.Error())
			os.Exit(ErrorGeneric)
		}

		creds := credentials.NewTLS(&tls.Config{
			Certificates:       []tls.Certificate{tlsKeyPair},
			RootCAs:            caPool,
			ServerName:         config.ControlMachine,
			InsecureSkipVerify: false,
			// NextProtos is a list of supported application level protocols, in
			// order of preference.
			NextProtos: []string{"h2"},
		})

		conn, err = grpc.NewClient(serverAddr,
			grpc.WithTransportCredentials(creds),
			grpc.WithKeepaliveParams(ClientKeepAliveParams),
			grpc.WithConnectParams(ClientConnectParams),
			grpc.WithIdleTimeout(time.Duration(math.MaxInt64)))
		if err != nil {
			log.Errorln("Cannot connect to CraneCtld: " + err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldForInternalClient(conn)
	} else {
		serverAddr = fmt.Sprintf("%s:%s", config.ControlMachineConnectAddr(), config.CraneCtldForInternalListenPort)

		conn, err = grpc.NewClient(serverAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(ClientKeepAliveParams),
			grpc.WithConnectParams(ClientConnectParams),
			grpc.WithIdleTimeout(time.Duration(math.MaxInt64)))
		if err != nil {
			log.Errorf("Cannot connect to CraneCtld %s: %s", serverAddr, err.Error())
			os.Exit(ErrorBackend)
		}

		stub = protos.NewCraneCtldForInternalClient(conn)
	}

	return conn, stub
}

func UpdateTLSConfig(config *Config) (*tls.Config, error) {
	return UpdateTLSConfigContext(context.Background(), config)
}

func UpdateTLSConfigContext(ctx context.Context, config *Config) (*tls.Config, error) {
	if err := SignAndSaveUserCertificateContext(ctx, config); err != nil {
		caCert, err := os.ReadFile(config.TlsConfig.CaFilePath)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig := &tls.Config{
			RootCAs:    caCertPool,
			ServerName: "*." + config.TlsConfig.DomainSuffix,
			MinVersion: tls.VersionTLS13,
		}

		return tlsConfig, nil
	}

	userKeyPath := fmt.Sprintf("%s/user.key", config.TlsConfig.UserTlsCertPath)
	userCertPath := fmt.Sprintf("%s/user.pem", config.TlsConfig.UserTlsCertPath)

	if !FileExists(userKeyPath) || !FileExists(userCertPath) || !FileExists(config.TlsConfig.ExternalCertFilePath) {
		return nil, fmt.Errorf("certificate files not found")
	}

	cert, err := tls.LoadX509KeyPair(userCertPath, userKeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %v", err)
	}

	CaCertContent, err := os.ReadFile(config.TlsConfig.ExternalCertFilePath)
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(CaCertContent); !ok {
		return nil, fmt.Errorf("failed to append cert Content")
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caPool,
		ServerName:   "*." + config.TlsConfig.DomainSuffix,
		MinVersion:   tls.VersionTLS13,
	}

	return tlsConfig, nil
}

func GrpcErrorPrintf(err error, format string, a ...any) {
	s := fmt.Sprintf(format, a...)
	if rpcErr, ok := grpcstatus.FromError(err); ok {
		switch rpcErr.Code() {
		case grpccodes.Unavailable:
			log.Errorf("%s: Connection to CraneCtld is broken.", s)
		case grpccodes.Unauthenticated:
			log.Errorf("%s: Access denied.", s)
		case grpccodes.DeadlineExceeded: // timeout errors
			log.Errorf("%s: Request timeout, please try again or reduce the query scope.", s)
		case grpccodes.ResourceExhausted: // resource exhaustion/oversized responses
			log.Errorf("%s: Response too large, please reduce the query scope.", s)
		default:
			log.Errorf("%s: gRPC error code %s.", s, rpcErr.String())
		}
	}
}

func RefreshCertInterceptor(refreshCertificateFunc func() error, updateConnFunc func() (*grpc.ClientConn, error)) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			return nil
		}

		rpcErr, _ := grpcstatus.FromError(err)
		if (rpcErr.Code() == grpccodes.Unavailable && strings.Contains(rpcErr.Message(), "certificate")) ||
			rpcErr.Code() == grpccodes.Unauthenticated {
			if refreshErr := refreshCertificateFunc(); refreshErr != nil {
				log.Errorf("%s", refreshErr.Error())
				return refreshErr
			}

			newConn, err := updateConnFunc()
			if err != nil {
				return err
			}
			cc.Close()
			err = invoker(ctx, method, req, reply, newConn, opts...)
			if err == nil {
				return nil
			}
		}

		return err
	}
}
