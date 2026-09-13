Supervisor exit regression tests
===============================

Configure with `-DCRANE_ENABLE_TESTS=ON`, then run:

```sh
cmake --build <build-directory> --target supervisor_exit_test -j2
ctest --test-dir <build-directory> -R '^SupervisorExitTest\.' --output-on-failure
```

The tests invoke the production SIGCHLD queue callback with controlled process
exit notifications. They verify that residual cleanup starts after the last
task process exits, while tasks still await output EOF, and runs only once.
They also cover step cgroup cleanup, process group fallback, and exit status
ordering relative to stdout and stderr.

Processes and cgroups record kill requests without sending signals or accessing
the host cgroup filesystem. Event loop threads are stopped before injecting
events, so these tests need neither root nor running Crane services. They check
Supervisor decisions; kernel cgroup killing and the complete crun/Cfored RPC
path require a system test.
