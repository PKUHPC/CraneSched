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
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"strings"
)

func NixShell(uid string) (string, error) {
	out, err := exec.Command("getent", "passwd", uid).Output()
	if err != nil {
		return "", err
	}

	ent := strings.Split(strings.TrimSuffix(string(out), "\n"), ":")
	return ent[6], nil
}

// TcGetpgrp gets the process group ID of the foreground process
// group associated with the terminal referred to by fd.
//
// See POSIX.1 documentation for more details:
// http://pubs.opengroup.org/onlinepubs/009695399/functions/tcgetpgrp.html
func TcGetpgrp(fd int) (pgrp int, err error) {
	return unix.IoctlGetInt(fd, unix.TIOCGPGRP)
}

// TcSetpgrp sets the foreground process group ID associated with the
// terminal referred to by fd to pgrp.
//
// See POSIX.1 documentation for more details:
// https://pubs.opengroup.org/onlinepubs/9699919799/functions/tcsetpgrp.html
func TcSetpgrp(fd int, pgrp int) (err error) {
	return unix.IoctlSetPointerInt(fd, unix.TIOCSPGRP, pgrp)
}

// IsForeground returns true if the calling process is a foreground process.
//
// Note that the foreground/background status of a process can change
// at any moment if the user utilizes the shell job control commands (fg/bg).
//
// Example use for command line tools: suppress extra output if a
// process is running in background, provide verbose output when
// running on foreground.
func IsForeground() bool {
	pgrp1, err := TcGetpgrp(int(os.Stdin.Fd()))
	if err != nil {
		return false
	}
	pgrp2 := unix.Getpgrp()
	return pgrp1 == pgrp2
}
