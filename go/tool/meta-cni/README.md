# Meta CNI plugin for CraneSched

This directory contains a CNI "meta" plugin that organizes CNI plugins into
**pipelines**, where each pipeline manages a single network interface. It
supports both static pipelines (always executed) and template pipelines
(dynamically expanded based on GRES annotations from the CraneSched backend).

## What it does

- **Static Pipelines**: Run a fixed chain of delegates for a named interface
  (e.g., `eth0`) on every invocation.
- **Template Pipelines**: Expand into 0..N instances at runtime based on
  `cranesched.internal/meta-cni/gres/<pipeline-name>/<index>` Pod annotations.
  Each instance gets a unique interface name (`ifNamePrefix` + index).
- Applies configurable runtime overrides (global → pipeline → delegate) to the
  CNI runtime environment before invoking each delegate.
- Pipeline-internal results are chained (`prevResult` passed along).
  Cross-pipeline results are merged. The merged result is returned to the
  container runtime.
- ADD failures trigger two-level rollback (within pipeline, then across
  pipelines). DEL uses best-effort execution.

## Build and install

Build with the top-level Makefile:

```sh
make tool
```

The binary will be at `build/tool/meta-cni`. For CNI to discover it, place the
binary in your CNI_PATH and name it to match the `type` field in your config
(`type` is the plugin binary name). For example:

```sh
sudo install -m 0755 build/tool/meta-cni /opt/cni/bin/crane-meta
```

## Configuration

The meta plugin reads JSON config from stdin like any other CNI plugin. The
fields below are in addition to standard CNI fields such as `cniVersion`,
`name`, and `type`.

### Top-level fields

- `logLevel` (string): `trace`, `debug`, or `info`. Defaults to info.
- `timeoutSeconds` (int): When > 0, a single timeout for the entire action
  (ADD/CHECK/DEL) across all pipelines.
- `capabilities` (object): Should include
  `"io.kubernetes.cri.pod-annotations": true` to enable GRES annotation
  passthrough from containerd.
- `runtimeOverride` (object): Global runtime override applied to every
  delegate across all pipelines.
- `pipelines` (array, required): List of pipeline definitions.

### Pipeline fields

Each entry in `pipelines` supports:

- `name` (string, required): Unique identifier. For template pipelines, also
  used as the GRES annotation matching key.
- `ifName` (string): Container interface name for static pipelines.
  Mutually exclusive with `ifNamePrefix`.
- `ifNamePrefix` (string): Interface name prefix for template pipelines.
  Instances get `ifNamePrefix` + index (e.g., `roce0`, `roce1`).
  Mutually exclusive with `ifName`.
- `runtimeOverride` (object, optional): Pipeline-level runtime override.
- `delegates` (array, required): List of delegate plugins to invoke in this
  pipeline.

### Delegate fields

Each entry in a pipeline's `delegates` supports:

- `name` (string, optional): Identifier, also used as a default `name` in
  generated delegate configs.
- `type` (string, optional): Delegate plugin type (binary name).
- `conf` (object, optional): Delegate plugin configuration. If omitted, the
  meta plugin will build a minimal config using `type`, `name`, and the parent
  `cniVersion`. String values inside `conf` may contain Go template expressions
  that are rendered at runtime using `.Gres` and `.Args`.
- `runtimeOverride` (object, optional): Per-delegate override (takes precedence
  over pipeline and global overrides).

### Runtime variables in conf

Template expressions run inside string values in `delegates[].conf`.

Simple example:

```json
"conf": {
  "type": "sriov",
  "deviceID": "{{.Gres.Device}}"
}
```

Conditional example:

```json
"conf": {
  "type": "macvlan",
  "master": "{{if .Args.MASTER}}{{.Args.MASTER}}{{else}}eno1{{end}}"
}
```

Available runtime data:

- `.Gres.Device` - the device annotation value for the current template-pipeline instance
- `.Gres.Index` - the template instance index as a string
- `.Args.<KEY>` - a CNI_ARGS entry whose key is a valid Go identifier
- `index .Args "KEY-WITH-DASH"` - access for keys that are not valid Go identifiers

Rules and limitations:

1. Templates are rendered only inside JSON string values in `conf`.
2. The renderer uses standard Go `text/template` behavior.
3. Invalid template syntax and template execution errors fail the pipeline.
4. Missing `.Args` keys follow Go template zero-value semantics and render as empty strings. `.Gres` only exposes `Device` and `Index`; other `.Gres` fields are invalid.
5. Rendered fields stay strings; this feature does not generate JSON numbers, booleans, objects, or arrays.
6. On `ADD`, `DEL`, and `CHECK`, template pipelines with no matching GRES annotations are skipped.

### GRES annotation convention

The CraneSched backend communicates device information via Pod annotations:

```
cranesched.internal/meta-cni/gres/<pipeline-name>/<index> = <device-id>
```

The meta plugin scans these annotations at runtime to determine how many
template pipeline instances to create and what device to assign to each.

## Runtime override

Runtime overrides apply to the environment used for each delegate invocation.
Override priority (lowest to highest):

1. Incoming CNI request values
2. Global `runtimeOverride`
3. Pipeline `ifName` (sets `CNI_IFNAME`)
4. Pipeline `runtimeOverride`
5. Delegate `runtimeOverride`

Override fields:

- `containerID`, `netns`, `ifName`, `cniPath`: Replace the corresponding
  CNI runtime values.
- `args`: A list of manipulator expressions for `CNI_ARGS`.
- `envs`: A list of manipulator expressions for environment variables.

Manipulator expression syntax:

- Set or update: `KEY=value`
- Delete: `-KEY`
- Values may be single- or double-quoted to include spaces.

## Example

See `config/00-crane-calico.conf` for a single-pipeline Calico setup, and
`config/00-meta.example.conf` for a multi-pipeline example with both static
Ethernet and template RDMA pipelines.
