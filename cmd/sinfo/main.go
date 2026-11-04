// Copyright (c) 2026 Peking University and Peking University
// Changsha Institute for Computing and Digital Economy
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"CraneFrontEnd/compat/sinfo"
	"context"
	"os"
)

func main() {
	os.Exit(sinfo.Run(context.Background(), os.Args, os.Environ(), os.Stdout, os.Stderr))
}
