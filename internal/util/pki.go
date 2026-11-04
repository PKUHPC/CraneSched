package util

import (
	"CraneFrontEnd/generated/protos"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func SignAndSaveUserCertificate(config *Config) error {
	return SignAndSaveUserCertificateContext(context.Background(), config)
}

func SignAndSaveUserCertificateContext(ctx context.Context, config *Config) error {

	certPath := fmt.Sprintf("%s/user.pem", config.TlsConfig.UserTlsCertPath)
	keyPath := fmt.Sprintf("%s/user.key", config.TlsConfig.UserTlsCertPath)

	if FileExists(certPath) && FileExists(keyPath) {
		return nil
	}

	return DoSignAndSaveUserCertificateContext(ctx, config)
}

func DoSignAndSaveUserCertificate(config *Config) error {
	return DoSignAndSaveUserCertificateContext(context.Background(), config)
}

func DoSignAndSaveUserCertificateContext(ctx context.Context, config *Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	uid := uint32(os.Getuid())
	privateKey, privateKeyPEM, createKey, err := userCertificateKey(filepath.Join(config.TlsConfig.UserTlsCertPath, "user.key"))
	if err != nil {
		return err
	}

	csrTemplate := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("%d.%s", uid, config.TlsConfig.DomainSuffix),
		},
		DNSNames:           []string{fmt.Sprintf("*.%s", config.TlsConfig.DomainSuffix), "localhost"},
		SignatureAlgorithm: x509.SHA256WithRSA,
	}

	csrBytes, err := x509.CreateCertificateRequest(rand.Reader, csrTemplate, privateKey)
	if err != nil {
		return err
	}

	csrPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrBytes,
	})

	serverAddr := fmt.Sprintf("%s:%s", config.ControlMachineConnectAddr(), config.CraneCtldListenPort)

	creds, err := credentials.NewClientTLSFromFile(config.TlsConfig.CaFilePath, "*."+config.TlsConfig.DomainSuffix)
	if err != nil {
		return err
	}

	conn, err := grpc.NewClient(serverAddr,
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(ClientKeepAliveParams),
		grpc.WithConnectParams(ClientConnectParams),
		grpc.WithIdleTimeout(time.Duration(math.MaxInt64)))

	if err != nil {
		return err
	}

	client := protos.NewCraneCtldClient(conn)
	defer conn.Close()

	request := &protos.SignUserCertificateRequest{Uid: uid, CsrContent: string(csrPEM), AltNames: fmt.Sprintf("localhost, *.%s", config.TlsConfig.DomainSuffix)}

	response, err := client.SignUserCertificate(ctx, request)
	if err != nil {
		return err
	}

	if !response.Ok {
		return fmt.Errorf("%s", ErrMsg(response.Reason))
	}

	certificate := []byte(response.Certificate)
	if _, err := tls.X509KeyPair(certificate, privateKeyPEM); err != nil {
		return fmt.Errorf("invalid signed user certificate: %w", err)
	}
	return saveUserCredentials(ctx, config.TlsConfig.UserTlsCertPath, privateKeyPEM, csrPEM, certificate, createKey)
}

// Renewal keeps the existing key so that replacing only the certificate is an
// atomic update of the usable pair, including concurrent renewal by other CLIs.
func userCertificateKey(path string) (*rsa.PrivateKey, []byte, bool, error) {
	keyPEM, err := os.ReadFile(path)
	if err == nil {
		block, _ := pem.Decode(keyPEM)
		if block == nil {
			return nil, nil, false, fmt.Errorf("invalid user private key in %s", path)
		}
		var key *rsa.PrivateKey
		switch block.Type {
		case "RSA PRIVATE KEY":
			key, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		case "PRIVATE KEY":
			var parsed any
			parsed, err = x509.ParsePKCS8PrivateKey(block.Bytes)
			if err == nil {
				var ok bool
				key, ok = parsed.(*rsa.PrivateKey)
				if !ok {
					err = errors.New("private key is not RSA")
				}
			}
		default:
			err = fmt.Errorf("unsupported PEM type %q", block.Type)
		}
		if err != nil {
			return nil, nil, false, fmt.Errorf("invalid user private key in %s: %w", path, err)
		}
		return key, keyPEM, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, false, err
	}
	key, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to generate RSA private key: %w", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return key, keyPEM, true, nil
}

func saveUserCredentials(ctx context.Context, dir string, key, csr, certificate []byte, createKey bool) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	type credentialFile struct {
		name string
		data []byte
		mode os.FileMode
	}
	files := []credentialFile{{"user.csr", csr, 0600}, {"user.pem", certificate, 0644}}
	if createKey {
		files = append(files, credentialFile{"user.key", key, 0600})
	}
	staged := make(map[string]string, len(files))
	for _, file := range files {
		tmp, err := os.CreateTemp(dir, "."+file.name+"-*")
		if err != nil {
			return err
		}
		defer os.Remove(tmp.Name())
		staged[file.name] = tmp.Name()
		if err = tmp.Chmod(file.mode); err == nil {
			_, err = tmp.Write(file.data)
		}
		if err == nil {
			err = tmp.Sync()
		}
		if err = errors.Join(err, tmp.Close()); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if createKey {
		// Publish a complete key without overwriting one installed by another
		// process while the signing RPC was in flight. The loser can retry.
		if err := os.Link(staged["user.key"], filepath.Join(dir, "user.key")); err != nil {
			return fmt.Errorf("install user private key: %w", err)
		}
	}
	for _, name := range []string{"user.csr", "user.pem"} {
		if err := os.Rename(staged[name], filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}
