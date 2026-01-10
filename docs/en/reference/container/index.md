# Container Support

CraneSched Container Support provides a containerized runtime environment for cluster users alongside traditional process-based jobs. Using a "Container Job + Container Step" model, it allows resource reuse within a single job, submission of container steps, and collaborative execution with traditional batch/interactive steps.

!!! note "Positioning"
    CraneSched is **not based on** Kubernetes but is an independently implemented HPC + AI hybrid scheduling system. CraneSched Container Support focuses on "batch and compute job scheduling," distinct from Kubernetes's "service-oriented orchestration and autonomy."

## Features

<div class="grid cards" markdown>

- :material-cube-outline: **Containerized Execution**

    ---

    Container images and commands are unified, ensuring consistent and reproducible environments.

- :material-sitemap: **Unified Scheduling**

    ---

    Leverages existing partition, account, QoS, and reservation scheduling policies.

- :material-layers-triple: **In-Job Reuse**

    ---

    After a container job starts its Pod, multiple container steps can be appended.

- :material-transition: **Mixed Steps**

    ---

    Batch scripts, container steps, and interactive steps can coexist within the same job.

- :material-console-line: **Runtime Interaction**

    ---

    Debug and troubleshoot container steps using Attach/Exec.

- :material-shield-account: **Security Isolation**

    ---

    UID Mapping/Idmapped Mount provides secure Fake Root experience for regular users.

</div>

## Basic Model

- **Container Job**: Allocates resources, creates and maintains a Pod to host subsequent container steps. Container jobs can also include non-container steps (such as batch scripts or interactive steps).
- **Container Step**: The actual execution unit appended within a container job, corresponding to at least one container in the Pod. Each container step can specify independent image, command, environment variables, and mount configurations.
- **Pod Metadata**: Job-level container configuration (e.g., DNS, ports) created when submitting a container job, defining the Pod's overall properties.
- **Container Metadata**: Step-level container configuration (e.g., command, environment variables, mounts) specified when submitting a container step, defining the container's specific behavior.

## Entry Points

=== "cbatch --pod"
    Creates a container job using a batch script as the entry point. When the job starts, a Pod is automatically launched on allocated nodes. The script runs as the Primary Step, allowing container steps to be appended for complex container orchestration, and can be mixed with non-container steps, providing a Slurm-like batch script experience.

=== "ccon"
    Creates a container job with a container step as the Primary Step, suitable for simple jobs containing only container steps, providing a Docker/Kubernetes-like command-line experience.

## Related Documentation

- [Core Concepts and Lifecycle](concepts.md)
- [Quick Start](quickstart.md)
- [Examples](examples.md)
- [Troubleshooting](troubleshooting.md)
- [ccon Command Reference](../../command/ccon.md)
- [Container Deployment](../../deployment/container.md)
