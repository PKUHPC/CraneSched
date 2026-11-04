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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"CraneFrontEnd/internal/util"

	"github.com/distribution/reference"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type ImageRef struct {
	Image         string // fully normalized for CRI
	ServerAddress string // registry host[:port]
}

type AuthConfig struct {
	Auth string `json:"auth"`
}

type RegistryConfig struct {
	Auths map[string]AuthConfig `json:"auths"`
}

type registryAuthOutput struct {
	Server string `json:"server"`
}

type loginJsonFlags struct {
	Username      string `json:"username,omitempty"`
	PasswordStdin bool   `json:"password_stdin,omitempty"`
}

const registryConfigFileMode os.FileMode = 0600

func encodeAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func decodeAuth(encodedAuth string) (username, password string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(encodedAuth)
	if err != nil {
		return "", "", err
	}

	auth := string(decoded)
	parts := strings.SplitN(auth, ":", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid auth format")
	}

	return parts[0], parts[1], nil
}

func getRegistryConfigPath() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(homeDir, util.DefaultUserConfigPrefix, "registry.json"), nil
}

func loadRegistryConfig() (*RegistryConfig, error) {
	configPath, err := getRegistryConfigPath()
	if err != nil {
		return nil, fmt.Errorf("failed to get registry config path: %v", err)
	}

	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return &RegistryConfig{Auths: make(map[string]AuthConfig)}, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %v", err)
	}

	var config RegistryConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %v", err)
	}

	if config.Auths == nil {
		config.Auths = make(map[string]AuthConfig)
	}

	return &config, nil
}

func saveRegistryConfig(config *RegistryConfig) error {
	configPath, err := getRegistryConfigPath()
	if err != nil {
		return fmt.Errorf("failed to get registry config path: %v", err)
	}
	configDir := filepath.Dir(configPath)

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %v", err)
	}

	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %v", err)
	}

	if err := os.WriteFile(configPath, data, registryConfigFileMode); err != nil {
		return fmt.Errorf("failed to write config file: %v", err)
	}
	if err := os.Chmod(configPath, registryConfigFileMode); err != nil {
		return fmt.Errorf("failed to set config file permission: %v", err)
	}

	return nil
}

func readPassword(fromStdin bool) (string, error) {
	if fromStdin {
		password, err := io.ReadAll(os.Stdin)
		if err != nil {
			return "", err
		}
		return strings.TrimRight(string(password), "\r\n"), nil
	}

	fmt.Print("Password: ")
	bytePassword, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println()
	return string(bytePassword), nil
}

func canonicalRegistryServer(server string) string {
	switch server {
	case "index.docker.io", "registry-1.docker.io":
		return "docker.io"
	default:
		return server
	}
}

func normalizeRegistryServer(input string) (string, error) {
	server := strings.TrimSpace(input)
	if server == "" {
		return "", fmt.Errorf("registry server is required")
	}

	if strings.Contains(server, "://") {
		parsed, err := url.Parse(server)
		if err != nil || parsed.Host == "" || parsed.User != nil ||
			parsed.RawQuery != "" || parsed.Fragment != "" {
			return "", fmt.Errorf("invalid registry server: %s", input)
		}
		if strings.Trim(parsed.Path, "/") != "" {
			return "", fmt.Errorf("invalid registry server: %s", input)
		}
		server = parsed.Host
	} else {
		server = strings.TrimRight(server, "/")
	}

	server = strings.ToLower(server)
	if server == "" || strings.Contains(server, "/") {
		return "", fmt.Errorf("invalid registry server: %s", input)
	}

	return canonicalRegistryServer(server), nil
}

func authLookupKeys(registry string) []string {
	registry = canonicalRegistryServer(strings.ToLower(strings.TrimSpace(registry)))
	keys := []string{registry}
	if registry == "docker.io" {
		keys = append(keys, "index.docker.io", "registry-1.docker.io")
	}
	return keys
}

func findAuthConfig(config *RegistryConfig, registry string) (AuthConfig, bool) {
	for _, key := range authLookupKeys(registry) {
		authConfig, exists := config.Auths[key]
		if exists {
			return authConfig, true
		}
	}

	keys := make([]string, 0, len(config.Auths))
	for key := range config.Auths {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		normalized, err := normalizeRegistryServer(key)
		if err == nil && normalized == registry {
			return config.Auths[key], true
		}
	}

	return AuthConfig{}, false
}

func deleteAuthConfigs(config *RegistryConfig, registry string) bool {
	deleted := false
	for _, key := range authLookupKeys(registry) {
		if _, exists := config.Auths[key]; exists {
			delete(config.Auths, key)
			deleted = true
		}
	}

	for key := range config.Auths {
		normalized, err := normalizeRegistryServer(key)
		if err == nil && normalized == registry {
			delete(config.Auths, key)
			deleted = true
		}
	}

	return deleted
}

func loginExecute(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "registry server is required")
	}

	server, err := normalizeRegistryServer(args[0])
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}

	f := GetFlags()
	username := f.Login.Username
	password := f.Login.Password
	usePasswordStdin := f.Login.PasswordStdin && password == ""

	if f.Login.PasswordStdin && password != "" {
		return util.NewCraneErr(util.ErrorCmdArg, "--password and --password-stdin are mutually exclusive")
	}

	if f.Login.PasswordStdin && username == "" {
		return util.NewCraneErr(util.ErrorCmdArg, "username is required with --password-stdin")
	}

	if username == "" {
		fmt.Print("Username: ")
		fmt.Scanln(&username)
		if username == "" {
			return util.NewCraneErr(util.ErrorCmdArg, "username is required")
		}
	}

	if password == "" {
		var err error
		password, err = readPassword(usePasswordStdin)
		if err != nil {
			return util.WrapCraneErr(util.ErrorCmdArg, "failed to read password: %v", err)
		}
		if password == "" {
			return util.NewCraneErr(util.ErrorCmdArg, "password is required")
		}
	}

	config, err := loadRegistryConfig()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to load registry config: %v", err)
	}

	authConfig := AuthConfig{
		Auth: encodeAuth(username, password),
	}

	deleteAuthConfigs(config, server)
	config.Auths[server] = authConfig

	if err := saveRegistryConfig(config); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to save registry config: %v", err)
	}

	if f.Global.Json {
		outputJson("login", "", loginJsonFlags{
			Username:      username,
			PasswordStdin: usePasswordStdin,
		}, registryAuthOutput{Server: server})
	} else {
		fmt.Printf("Login Succeeded: %s\n", server)
	}

	return nil
}

func NormalizeImageRef(input string) (ImageRef, error) {
	in := strings.TrimSpace(input)
	if in == "" {
		return ImageRef{}, fmt.Errorf("image reference is empty")
	}

	// Parse + normalize "familiar" Docker image names.
	named, err := reference.ParseDockerRef(in)
	if err != nil {
		return ImageRef{}, err
	}

	return ImageRef{
		Image:         named.String(),
		ServerAddress: reference.Domain(named),
	}, nil
}

// getAuthForRegistry retrieves saved authentication info for a registry
func getAuthForRegistry(registry string) (username, password string, err error) {
	if registry == "" {
		return "", "", fmt.Errorf("registry cannot be empty")
	}
	registry = canonicalRegistryServer(strings.ToLower(strings.TrimSpace(registry)))

	config, err := loadRegistryConfig()
	if err != nil {
		return "", "", err
	}

	authConfig, exists := findAuthConfig(config, registry)
	if !exists {
		return "", "", nil // No auth info available
	}

	return decodeAuth(authConfig.Auth)
}

func logoutExecute(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return util.NewCraneErr(util.ErrorCmdArg, "registry server is required")
	}

	server, err := normalizeRegistryServer(args[0])
	if err != nil {
		return util.NewCraneErr(util.ErrorCmdArg, err.Error())
	}

	config, err := loadRegistryConfig()
	if err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to load registry config: %v", err)
	}

	if !deleteAuthConfigs(config, server) {
		return util.NewCraneErr(util.ErrorCmdArg, fmt.Sprintf("no existing login for %s", server))
	}

	if err := saveRegistryConfig(config); err != nil {
		return util.WrapCraneErr(util.ErrorSystem, "failed to save registry config: %v", err)
	}

	f := GetFlags()
	if f.Global.Json {
		outputJson("logout", "", nil, registryAuthOutput{Server: server})
	} else {
		fmt.Printf("Logout Succeeded: %s\n", server)
	}

	return nil
}
