#!/usr/bin/env python3
"""Render canonical CraneTestKit Jobs for admission-policy dry-run probes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--namespace", required=True)
    args = parser.parse_args()

    # Imports intentionally happen only in the validated release interpreter.
    import crane_testkit.env.kubernetes as kubernetes
    import crane_testkit.orchestration.profile as profile_module

    release = args.release.resolve(strict=True)
    source_root = (release / "src").resolve(strict=True)
    module_paths = {
        "kubernetes": Path(kubernetes.__file__).resolve(strict=True),
        "profile": Path(profile_module.__file__).resolve(strict=True),
    }
    if any(not _inside(path, source_root) for path in module_paths.values()):
        raise RuntimeError(
            "CraneTestKit renderer was imported outside the validated release"
        )

    profile_path = args.profile.resolve(strict=True)
    profile = profile_module.load_profile(profile_path)
    image_lock = profile_module.load_image_lock(profile.image_lock_path)
    allocation = {}
    next_shard = 0
    for worker in profile.cluster.workers:
        allocation[worker.name] = tuple(range(next_shard, next_shard + worker.slots))
        next_shard += worker.slots

    jobs = kubernetes.render_worker_jobs(
        profile,
        image_lock,
        run_id="ci-admission-probe",
        autotest_image=args.image,
        allocation=allocation,
        namespace=args.namespace,
    )
    if not jobs:
        raise RuntimeError("CraneTestKit renderer produced no admission probe Jobs")
    print(
        json.dumps(
            {
                "jobs": jobs,
                "module_paths": {
                    name: str(path) for name, path in module_paths.items()
                },
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
