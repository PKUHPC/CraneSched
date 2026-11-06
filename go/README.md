# CraneSched Go components

This module contains the Crane commands, cfored, cplugind, official SO plugins,
Slurm-compatible commands and CNI tools. Its module path is
`github.com/PKUHPC/CraneSched/go`. Use a complete checkout of the CraneSched
repository: shared protocols, VERSION, LICENSE and configuration inputs live
at the repository root.

Run the native commands in a target build container with the Go version from
go.mod and the generators documented in the product packaging guide:

```bash
make -C go all tool CHECK_GPU=0 PREFIX=/usr
make -C go test
make -C go install DESTDIR=/absolute/staging PREFIX=/usr CHECK_GPU=0
make -C go package CHECK_GPU=0
```

Generated Go code and binaries remain in `go/generated/` and `go/build/`.
The package target produces the client, plugin and optional Slurm compatibility
RPM/DEB packages under `go/build/dist/`. All components use the root product
version; cplugind and the official SO plugins must be built together.

The test target runs tests again on every invocation while retaining Go's build
cache. Installation selects current commands, official plugins and service
templates, so obsolete files left in `go/build/` are not exported. Use an empty
staging directory when exporting a complete installation; installing into an
existing system does not remove previously installed files.

The native installation prefix defaults to `/usr/local`. Use `PREFIX=/usr`
consistently for installation and service templates when targeting `/usr`.
Development retains symbols by default (`STRIP=false`); the package target
rebuilds with `STRIP=true` and includes CNI tools. Keep Go/CGO options consistent
between services and their plugins, including when install rebuilds them.

Service templates and plugin/CNI configuration examples are under root `etc/`.
The source tree has one root LICENSE; packages consume that file directly.

See the [packaging guide](../docs/en/deployment/packaging.md) and
[plugin deployment guide](../docs/en/deployment/frontend/plugins.md).
