# Native PMI-2 launch

`crun --mpi=pmi2` directly starts MPI ranks using a PMI-2 server owned by each
step's Supervisor. Select the existing PMIx server with `--mpi=pmix`. Choose the
protocol from the MPI installation's build configuration, rather than its vendor name.

`CRANE_ENABLE_PMI2` defaults to ON and is independent of `CRANE_ENABLE_PMIX`.
A disabled protocol is rejected when submitting a job or step. Deploy matching
controller, compute and frontend versions; older versions do not gain PMI-2
support merely because the internal RPC additions are wire compatible.

The `craned` package includes CraneSched's native PMI-2 client. The build installs `libpmi2.so`, exporting the standard PMI-2 interface, under
`${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}/crane/pmi2/`, and the public
header under `${prefix}/include/crane/pmi2/`. This private directory preserves system PMI installations;
no Slurm daemon is required.

Compile with the intended MPI wrapper, deploy its runtime libraries to every
compute node, and launch the application directly:

```bash
crun -N 2 -n 4 --ntasks-per-node=2 --mpi=pmi2 ./mpi_application
```

For Intel MPI, point `I_MPI_PMI_LIBRARY` at the installed PMI-2 client, for example
`/usr/lib64/crane/pmi2/libpmi2.so`. Validate the exact Intel version, actual loaded
client and selected transport. MPICH must be built for PMI-2. Open MPI 4 needs
its PMI direct-launch component built with PMI-2; select `OMPI_MCA_ess=pmi` when
necessary. PMIx-only installations should use `--mpi=pmix`. Keep application
library paths consistent with the compiler wrapper.

PMIx and PMI-2 `crun` steps can run in one batch allocation, with independent
rank state, KVS and channels. Do not wrap the application in `mpirun`, Hydra or
PRRTE; scheduler integration for those launchers is outside this feature.

Supported operations cover static-world initialization/finalization, job ID,
KVS Put/Get/Fence, actual process mapping, node attributes and necessary job
attributes. Each rank inherits its own `PMI_FD`. Active nodes exchange fence
data over a TCP/gRPC tree using the cluster's configured addresses and TLS.
PMIx tuning variables do not select the PMI-2 algorithm or transport.

PMI-1, dynamic Spawn/Connect, cross-job KVS and name services are unsupported;
unimplemented operations receive explicit errors. Initialization, incomplete
requests and fences have a 60-second default timeout. Disconnects, timeouts
and Abort trigger failure propagation and cleanup for the owning step.
The control transport does not determine MPI's data transport; TCP functional
validation does not establish RDMA or physical multinode compatibility.
