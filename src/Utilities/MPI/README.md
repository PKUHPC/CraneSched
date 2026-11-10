# MPI runtime layout

The supervisor selects an `mpi::Runtime` through `CreateRuntime()`. Task launch,
environment merging and peer delivery use that interface without depending on a
particular MPI vendor. `--mpi=pmi2` selects the PMI runtime, which negotiates
PMI-1.1 or PMI-2.0 on each application's channel; `--mpi=pmix` selects PMIx.
Vendor selectors and SDK library paths remain part of the user's environment.

| Path | Responsibility |
| --- | --- |
| `Runtime.*`, `Environment.*` | Runtime selection, task channel ownership and replacement of inherited protocol identity |
| `StepControlClient.*`, `EndpointRendezvous.*` | Protocol-independent endpoint exchange and step termination |
| `PMI/Runtime.*`, `PMI/Server/` | Local channels, step topology, KVS visibility and distributed fences |
| `PMI/Protocol/` | Handshake, PMI-1 and PMI-2 dispatch, framing and reply encoding |
| `PMI/Client/` | Public `libpmi2` client implementation and its installed header |
| `Pmix/Server.*`, `Pmix/Namespace.cpp`, `Pmix/Operations.cpp` | PMIx lifecycle, namespace metadata and host operations |
| `Pmix/Collective/`, `Pmix/Modex/`, `Pmix/Transport/` | Collective state, direct-modex completion and gRPC/UCX transport |

The PMI codecs have one implementation shared by the server and client. The
installed `libpmi2.so.0` ABI and `crane/pmi2/pmi2.h` path are independent of the
source layout. `CRANE_ENABLE_PMI2` controls both PMI wire versions and the client
library; `CRANE_ENABLE_PMIX` and `CRANE_ENABLE_UCX` retain their existing roles.

## Endpoint exchange and lifetime

The scheduler registers a rendezvous before allocating a live MPI step and
erases it at terminal cleanup. Announcements must match the job, step, protocol
and ordered allocation. Identical retries are accepted; conflicting endpoints
are rejected. PMIx endpoints are opaque bytes because a UCX address need not be
UTF-8. Fanout shares one immutable snapshot and performs RPCs outside scheduler
and step-map locks.

PMI uses one local I/O loop and a separate fence thread per step, not one thread
per rank. PMI-1 and PMI-2 handlers share the KVS, fence and finalization state.
Polling buffers and encoded process mappings are reused across requests.

Modex requests are indexed by sequence. Response, transport failure, timeout and
shutdown all remove a pending request before completing it; callbacks run outside
the pending-state lock. A rejected request retains callback ownership at the
caller. Late transport callbacks capture shared pending state instead of the
manager. Successful response data remains owned until PMIx invokes its release
callback. Collective shutdown similarly detaches state before invoking callbacks.

## Protocol sources and upgrades

`protos/mpi/Mpi.proto` owns common process identities and endpoint exchange;
`Pmi.proto` owns PMI peer fences and aborts; `Pmix.proto` owns PMIx peer traffic.
These are scheduler control messages. Application PMI-1/PMI-2 wire protocols and
the PMIx client ABI are separate contracts. C++ and Go generation preserve the
source hierarchy under their generated output roots.

The endpoint RPCs and MPI protobuf service packages replace the previous internal
PMI-2/PMIx RPC names. Upgrade controller, compute daemons, supervisors and generated
clients together after draining MPI steps; mixed versions of these internal RPCs
are not supported.

Regression tests live under `test/MPI/`: environment merging, endpoint validation,
PMI client/server round trips and PMIx completion/lifetime tests. Build them with
the repository's `crane_unit_tests` target and run them through CTest.
