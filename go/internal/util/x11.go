package util

import (
	"bytes"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const X11TcpPortOffset = 6000 // X11 TCP port offset

// GetX11DisplayEx extracts the local TCP port and hostname/socket path
// from the DISPLAY environment variable. If the DISPLAY variable indicates
// a UNIX socket, the returned port will be 0 and the target will point
// to the local UNIX socket path.
//
// It will return an error if the DISPLAY environment variable is not set
// or cannot be parsed correctly.
func GetX11DisplayEx(noUnix bool) (target string, port uint16, err error) {
	display := os.Getenv("DISPLAY")
	if display == "" {
		return "", 0, errors.New("no DISPLAY environment variable set, cannot set up X11 forwarding")
	}

	// Handle UNIX socket paths (e.g., ":0", ":0.0")
	if !noUnix && strings.HasPrefix(display, ":") {
		// Extract the display number (and ignore screen number, if present)
		periodIndex := strings.Index(display, ".")
		if periodIndex != -1 {
			display = display[:periodIndex]
		}

		// Build the UNIX socket path
		socketPath := filepath.Join("/tmp/.X11-unix", "X"+display[1:])
		if _, err := os.Stat(socketPath); err != nil {
			return "", 0, fmt.Errorf("cannot stat local X11 socket `%s`: %w", socketPath, err)
		}

		return socketPath, 0, nil
	}

	// Handle TCP/IP connections (e.g., "localhost:89", "localhost/unix:89.0")
	colonIndex := strings.Index(display, ":")
	if colonIndex == -1 {
		return "", 0, errors.New("error parsing DISPLAY environment variable, cannot use X11 forwarding")
	}

	// Split hostname and port
	hostname := display[:colonIndex]
	portPart := display[colonIndex+1:]

	// Strip the screen portion from the port if present (e.g., "89.0" -> "89")
	periodIndex := strings.Index(portPart, ".")
	if periodIndex != -1 {
		portPart = portPart[:periodIndex]
	}

	// Convert port to an integer and apply X11 TCP port offset
	portInt, err := strconv.Atoi(portPart)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port number in DISPLAY: %w", err)
	}

	return hostname, uint16(portInt + X11TcpPortOffset), nil
}

func GetX11AuthCookie() (cookie string, err error) {
	const cookiePattern = `(?m)[a-zA-Z0-9./_-]+:[0-9]+\s+MIT-MAGIC-COOKIE-1\s+([0-9a-fA-F]+)$`
	const wildcardPattern = `(?m)#ffff#[0-9a-fA-F./_-]+#:[0-9]+\s+MIT-MAGIC-COOKIE-1\s+([0-9a-fA-F]+)$`

	display := os.Getenv("DISPLAY")
	if display == "" {
		return "", errors.New("DISPLAY environment variable not set")
	}

	cmd := exec.Command("xauth", "list", display)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		log.Debugf("Error running xauth command: %v. Stderr: %s", err, stderr.String())
		return "", errors.New("cannot use X11 forwarding")
	}

	log.Debugf("Get xauth list $DISPLAY(%s): %s", display, strings.TrimSpace(stdout.String()))

	result := stdout.String()
	cookieRegex := regexp.MustCompile(cookiePattern)
	wildcardRegex := regexp.MustCompile(wildcardPattern)

	/*
	 * Examples of valid xauth cookie entries:
	 *    - "riley/unix:10  MIT-MAGIC-COOKIE-1  abcdef0123456789"
	 *    - "riley:10  MIT-MAGIC-COOKIE-1  abcdef0123456789"
	 *
	 * - **Hostname or identifier** (`[a-zA-Z0-9./_-]+`):
	 *   Matches the hostname (e.g., "riley") and optional `/unix`, supporting symbols
	 *   like `.`, `/`, `_`, and `-`.
	 * - **Display number** (`:[0-9]+`):
	 *   The display number (e.g., `:10`) is required and follows a colon.
	 * - **Protocol and cookie value**:
	 *   "MIT-MAGIC-COOKIE-1" is the protocol, followed by a hexadecimal cookie value.
	 */

	match := cookieRegex.FindStringSubmatch(result)
	if match == nil {
		log.Debugf("Could not retrieve magic cookie, checking for wildcard cookie.")

		/*
		 * In certain situations, the only available xauth cookie may be the wildcard
		 * cookie. The wildcard cookie is not included in the initial regular
		 * expression (regex) match intentionally. This ensures that the wildcard
		 * cookie is only used as a fallback when no other matching cookie is available.
		 *
		 * The wildcard cookie format is as follows:
		 * "#ffff#abcdef0123456789#:12345  MIT-MAGIC-COOKIE-1  abcdef0123456789"
		 *
		 * - "#ffff#" indicates that this is a wildcard family.
		 * - "abcdef0123456789" represents the hostname or a hex-encoded identifier.
		 * - ":12345" refers to the display number.
		 * - "MIT-MAGIC-COOKIE-1" is the authentication protocol used.
		 * - "abcdef0123456789" is the actual cookie value (the shared secret).
		 *
		 * - The definition of `FamilyWild` can be found in the <X11/Xauth.h> header file.
		 *   This constant is used to denote wildcard families in the X11 authentication system.
		 *   The format of this wildcard cookie can be interpreted by the function
		 *   `dump_entry()` in the xauth source code, specifically located in `process.c`.
		 *   This function is responsible for parsing and dumping xauth entries into
		 *   human-readable formats.
		 *
		 * - Wildcard cookies are typically used in scenarios where a single xauth cookie
		 *   must be shared across multiple hosts or displays, acting as a "catch-all"
		 *   for authentication purposes. However, they are generally considered less
		 *   secure than display-specific cookies, and should be used with caution.
		 */
		match = wildcardRegex.FindStringSubmatch(result)
		if match == nil {
			return "", errors.New("could not retrieve magic cookie, cannot use X11 forwarding")
		}
	}

	if len(match) < 2 {
		return "", errors.New("invalid match format for magic cookie")
	}

	return match[1], nil
}
