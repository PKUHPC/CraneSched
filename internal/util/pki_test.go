package util

import (
	"CraneFrontEnd/generated/protos"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type certificateController struct {
	protos.UnimplementedCraneCtldServer
	ca   *x509.Certificate
	key  *rsa.PrivateKey
	mode string
}

func (s *certificateController) SignUserCertificate(_ context.Context, req *protos.SignUserCertificateRequest) (*protos.SignUserCertificateResponse, error) {
	switch s.mode {
	case "rpc":
		return nil, status.Error(codes.Unavailable, "signer unavailable")
	case "rejected":
		return &protos.SignUserCertificateResponse{Ok: false}, nil
	case "invalid":
		return &protos.SignUserCertificateResponse{Ok: true, Certificate: "invalid certificate"}, nil
	}
	block, _ := pem.Decode([]byte(req.CsrContent))
	if block == nil {
		return nil, status.Error(codes.InvalidArgument, "missing CSR")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, err
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, err
	}
	cert := &x509.Certificate{
		SerialNumber: big.NewInt(3), Subject: csr.Subject, DNSNames: csr.DNSNames,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, s.ca, csr.PublicKey, s.key)
	if err != nil {
		return nil, err
	}
	return &protos.SignUserCertificateResponse{Ok: true, Certificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}, nil
}

func certificateFixture(t *testing.T, mode string) (*Config, []byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		DNSNames: []string{"*.example.test"},
	}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	serverCert, err := tls.X509KeyPair(caPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{serverCert}, MinVersion: tls.VersionTLS13})))
	protos.RegisterCraneCtldServer(server, &certificateController{ca: ca, key: key, mode: mode})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	config := &Config{ControlMachine: host, CraneCtldListenPort: port, TlsConfig: TLSConfig{
		CaFilePath: filepath.Join(root, "ca.pem"), DomainSuffix: "example.test", UserTlsCertPath: filepath.Join(root, "user"),
	}}
	if err := os.WriteFile(config.TlsConfig.CaFilePath, caPEM, 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(config.TlsConfig.UserTlsCertPath, 0700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{"user.key": keyPEM, "user.pem": caPEM} {
		if err := os.WriteFile(filepath.Join(config.TlsConfig.UserTlsCertPath, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return config, keyPEM, caPEM
}

func TestCertificateRenewalFailurePreservesExistingPair(t *testing.T) {
	for _, mode := range []string{"rpc", "rejected", "invalid", "canceled", "save-failed"} {
		t.Run(mode, func(t *testing.T) {
			config, oldKey, oldCert := certificateFixture(t, mode)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			if mode == "save-failed" {
				if err := os.Mkdir(filepath.Join(config.TlsConfig.UserTlsCertPath, "user.csr"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if err := DoSignAndSaveUserCertificateContext(ctx, config); err == nil {
				t.Fatal("expected renewal failure")
			}
			for name, want := range map[string][]byte{"user.key": oldKey, "user.pem": oldCert} {
				got, err := os.ReadFile(filepath.Join(config.TlsConfig.UserTlsCertPath, name))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("%s changed after failed renewal: %v", name, err)
				}
			}
			entries, err := filepath.Glob(filepath.Join(config.TlsConfig.UserTlsCertPath, ".user.*"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary credentials left behind: %v %v", entries, err)
			}
		})
	}
}

func TestCertificateProvisionAndRenewal(t *testing.T) {
	for _, mode := range []string{"initial", "pkcs1", "pkcs8"} {
		t.Run(mode, func(t *testing.T) {
			config, oldKey, oldCert := certificateFixture(t, "success")
			dir := config.TlsConfig.UserTlsCertPath
			initial := mode == "initial"
			if mode == "pkcs8" {
				block, _ := pem.Decode(oldKey)
				key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
				if err != nil {
					t.Fatal(err)
				}
				der, err := x509.MarshalPKCS8PrivateKey(key)
				if err != nil {
					t.Fatal(err)
				}
				oldKey = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
				if err := os.WriteFile(filepath.Join(dir, "user.key"), oldKey, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err := tls.LoadX509KeyPair(filepath.Join(dir, "user.pem"), filepath.Join(dir, "user.key")); err != nil {
					t.Fatalf("PKCS#8 fixture is not a usable pair: %v", err)
				}
			}
			if initial {
				for _, name := range []string{"user.key", "user.pem"} {
					if err := os.Remove(filepath.Join(dir, name)); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			if err := DoSignAndSaveUserCertificateContext(ctx, config); err != nil {
				t.Fatal(err)
			}
			pair, err := tls.LoadX509KeyPair(filepath.Join(dir, "user.pem"), filepath.Join(dir, "user.key"))
			if err != nil || pair.Leaf.SerialNumber.Int64() != 3 {
				t.Fatalf("new credentials are not a valid signed pair: %v", err)
			}
			now := time.Now()
			if now.Before(pair.Leaf.NotBefore) || !now.Before(pair.Leaf.NotAfter) {
				t.Fatalf("renewed certificate is outside its validity window: %v to %v", pair.Leaf.NotBefore, pair.Leaf.NotAfter)
			}
			key, err := os.ReadFile(filepath.Join(dir, "user.key"))
			if err != nil || !initial && !bytes.Equal(key, oldKey) {
				t.Fatalf("renewal replaced private key: %v", err)
			}
			cert, err := os.ReadFile(filepath.Join(dir, "user.pem"))
			if err != nil || bytes.Equal(cert, oldCert) {
				t.Fatalf("certificate not renewed: %v", err)
			}
			for name, mode := range map[string]os.FileMode{"user.key": 0600, "user.csr": 0600, "user.pem": 0644} {
				info, err := os.Stat(filepath.Join(dir, name))
				if err != nil || info.Mode().Perm() != mode {
					t.Fatalf("wrong permissions for %s: %v", name, err)
				}
			}
			if err := saveUserCredentials(ctx, dir, []byte("concurrent key"), nil, nil, true); !errors.Is(err, os.ErrExist) {
				// A concurrent first-time signer cannot overwrite the winner's key.
				t.Fatalf("expected key installation conflict: %v", err)
			}
			if _, err := tls.LoadX509KeyPair(filepath.Join(dir, "user.pem"), filepath.Join(dir, "user.key")); err != nil {
				t.Fatalf("concurrent installation damaged the pair: %v", err)
			}
		})
	}
}

func TestCertificateRenewalRejectsInvalidPrivateKey(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for name, keyPEM := range map[string][]byte{
		"invalid PEM":     []byte("invalid private key"),
		"invalid PKCS1":   pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("invalid")}),
		"invalid PKCS8":   pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("invalid")}),
		"non-RSA PKCS8":   pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}),
		"unsupported PEM": pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}),
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			files := map[string][]byte{"user.key": keyPEM, "user.pem": []byte("existing certificate"), "user.csr": []byte("existing CSR")}
			for name, data := range files {
				if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			config := &Config{TlsConfig: TLSConfig{UserTlsCertPath: dir}}
			if err := DoSignAndSaveUserCertificateContext(t.Context(), config); err == nil || !strings.Contains(err.Error(), "invalid user private key") {
				t.Fatalf("got %v, want private-key validation failure", err)
			}
			for name, want := range files {
				got, err := os.ReadFile(filepath.Join(dir, name))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("%s changed after failed renewal: %v", name, err)
				}
			}
		})
	}
}
