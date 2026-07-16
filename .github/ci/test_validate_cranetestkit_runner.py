from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


MODULE_PATH = Path(__file__).with_name("validate-cranetestkit-runner.py")
SPEC = importlib.util.spec_from_file_location(
    "cranesched_ci_runner_validation", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
validation = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = validation
SPEC.loader.exec_module(validation)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _freeze(root: Path) -> None:
    paths = sorted(root.rglob("*"), key=lambda path: len(path.parts), reverse=True)
    for path in paths:
        if path.is_symlink():
            continue
        path.chmod(stat.S_IMODE(path.lstat().st_mode) & ~0o222)
    root.chmod(stat.S_IMODE(root.lstat().st_mode) & ~0o222)


def _make_writable(path: Path) -> None:
    path.chmod(stat.S_IMODE(path.lstat().st_mode) | stat.S_IWUSR)


class ReleaseValidationTest(unittest.TestCase):
    def _git(self, release: Path, *arguments: str) -> str:
        result = subprocess.run(
            ["git", "-C", str(release), *arguments],
            check=True,
            text=True,
            capture_output=True,
        )
        return result.stdout.strip()

    def _release(self, root: Path) -> tuple[Path, Path, Path, str, str]:
        release = root / "release"
        profile = release / "profiles/wrl-lab.yaml"
        executable = release / ".venv/bin/crane_testkit"
        site_packages = release / ".venv/lib/python3.12/site-packages"
        source_package = release / "src/crane_testkit"
        (release / "docker/scripts").mkdir(parents=True)
        profile.parent.mkdir(parents=True)
        executable.parent.mkdir(parents=True)
        site_packages.mkdir(parents=True)
        source_package.mkdir(parents=True)
        (release / ".gitignore").write_text(
            ".venv/\ndocker/scripts/proxy.env\n", encoding="utf-8"
        )
        profile.write_text("profile: test\n", encoding="utf-8")
        (source_package / "__init__.py").write_text("\n", encoding="utf-8")
        (release / ".venv/pyvenv.cfg").write_text(
            "home = /usr/bin\ninclude-system-site-packages = false\n",
            encoding="utf-8",
        )
        (site_packages / "_editable_impl_cranesched_autotest.pth").write_text(
            f"{release / 'src'}\n", encoding="utf-8"
        )
        (site_packages / "_virtualenv.pth").write_text(
            "import _virtualenv\n", encoding="utf-8"
        )
        (site_packages / "_virtualenv.py").write_text("\n", encoding="utf-8")
        interpreter = release / ".venv/bin/python3"
        interpreter.symlink_to(Path(sys.executable).resolve())
        executable.write_text(
            f"#!{interpreter}\nraise SystemExit(0)\n", encoding="utf-8"
        )
        executable.chmod(0o755)
        self._git(release, "init", "-b", "master")
        self._git(release, "config", "user.name", "CI Test")
        self._git(release, "config", "user.email", "ci@example.invalid")
        self._git(
            release,
            "add",
            ".gitignore",
            "profiles/wrl-lab.yaml",
            "src/crane_testkit/__init__.py",
        )
        self._git(release, "commit", "-m", "fixture")
        revision = self._git(release, "rev-parse", "HEAD")
        executable_sha256 = _sha256(executable)
        _freeze(release)
        return release, profile, executable, revision, executable_sha256

    @unittest.skipUnless(os.geteuid() == 0, "release ownership contract requires root")
    def test_exact_frozen_release_and_venv_are_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            release, profile, executable, revision, executable_sha256 = self._release(
                Path(temporary)
            )
            observed = validation._validate_release(
                release, revision, profile, executable_sha256
            )
            interpreter = validation._validate_frozen_environment(release, observed)
            self.assertEqual(observed, executable)
            self.assertEqual(interpreter, release / ".venv/bin/python3")

    @unittest.skipUnless(os.geteuid() == 0, "release ownership contract requires root")
    def test_ignored_proxy_environment_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            release, profile, _executable, revision, executable_sha256 = self._release(
                Path(temporary)
            )
            scripts = release / "docker/scripts"
            _make_writable(scripts)
            (scripts / "proxy.env").write_text(
                "HTTPS_PROXY=http://credential@example.invalid\n", encoding="utf-8"
            )
            _freeze(release)
            with self.assertRaisesRegex(
                validation.ValidationError, "ignored files outside .venv"
            ):
                validation._validate_release(
                    release, revision, profile, executable_sha256
                )

    @unittest.skipUnless(os.geteuid() == 0, "release ownership contract requires root")
    def test_ignored_executable_drift_is_rejected_by_digest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            release, profile, executable, revision, executable_sha256 = self._release(
                Path(temporary)
            )
            _make_writable(executable)
            executable.write_text("#!/bin/sh\nexit 7\n", encoding="utf-8")
            executable.chmod(0o555)
            with self.assertRaisesRegex(validation.ValidationError, "checksum"):
                validation._validate_release(
                    release, revision, profile, executable_sha256
                )

    @unittest.skipUnless(os.geteuid() == 0, "release ownership contract requires root")
    def test_owner_writable_release_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            release, profile, _executable, revision, executable_sha256 = self._release(
                Path(temporary)
            )
            _make_writable(release)
            with self.assertRaisesRegex(validation.ValidationError, "read-only"):
                validation._validate_release(
                    release, revision, profile, executable_sha256
                )

    @unittest.skipUnless(os.geteuid() == 0, "release ownership contract requires root")
    def test_editable_install_must_point_to_validated_source(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            release, _profile, executable, _revision, _digest = self._release(
                Path(temporary)
            )
            editable = (
                release
                / ".venv/lib/python3.12/site-packages/_editable_impl_cranesched_autotest.pth"
            )
            _make_writable(editable)
            editable.write_text("/tmp/untrusted-src\n", encoding="utf-8")
            editable.chmod(0o444)
            with self.assertRaisesRegex(
                validation.ValidationError, "outside the release"
            ):
                validation._validate_frozen_environment(release, executable)


class NfsMountValidationTest(unittest.TestCase):
    def test_exact_nfs_mount_identity_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            mount = Path(temporary) / "nfs"
            root = mount / "home/wenruilin/output/k3s"
            root.mkdir(parents=True)
            value = {
                "filesystems": [
                    {
                        "target": str(mount),
                        "source": "192.168.1.4:/nfs",
                        "fstype": "nfs4",
                    }
                ]
            }
            validation._validate_nfs_mount_identity(
                root,
                expected_server="192.168.1.4",
                expected_export=Path("/nfs"),
                expected_relative=Path("home/wenruilin/output/k3s"),
                findmnt_output=json.dumps(value),
            )
            for field, replacement, message in (
                ("fstype", "ext4", "not mounted from NFS"),
                ("source", "192.168.1.5:/nfs", "server/export"),
            ):
                with self.subTest(field=field):
                    invalid = json.loads(json.dumps(value))
                    invalid["filesystems"][0][field] = replacement
                    with self.assertRaisesRegex(validation.ValidationError, message):
                        validation._validate_nfs_mount_identity(
                            root,
                            expected_server="192.168.1.4",
                            expected_export=Path("/nfs"),
                            expected_relative=Path("home/wenruilin/output/k3s"),
                            findmnt_output=json.dumps(invalid),
                        )


class KubeconfigValidationTest(unittest.TestCase):
    def _config(self, user: dict[str, str]) -> dict[str, object]:
        return {
            "clusters": [
                {
                    "cluster": {
                        "server": "https://192.168.1.21:6443",
                        "certificate-authority-data": "Y2E=",
                    }
                }
            ],
            "contexts": [
                {
                    "context": {
                        "cluster": "k3s",
                        "user": "crane-testkit-ci",
                        "namespace": "crane-testkit",
                    }
                }
            ],
            "users": [{"user": user}],
        }

    def test_only_client_certificate_material_is_accepted(self) -> None:
        certificate = b"certificate"
        private_key = b"private-key"
        observed = validation._validate_kubeconfig_contract(
            self._config(
                {
                    "client-certificate-data": base64.b64encode(certificate).decode(),
                    "client-key-data": base64.b64encode(private_key).decode(),
                }
            ),
            expected_server="https://192.168.1.21:6443",
            namespace="crane-testkit",
        )
        self.assertEqual(observed, (certificate, private_key))

    def test_token_exec_and_insecure_transport_are_rejected(self) -> None:
        cases = (
            ({"token": "forbidden"}, "token"),
            ({"exec": {"command": "/bin/true"}}, "exec"),
        )
        for user, label in cases:
            with self.subTest(label=label):
                with self.assertRaisesRegex(validation.ValidationError, "must not use"):
                    validation._validate_kubeconfig_contract(
                        self._config(user),
                        expected_server="https://192.168.1.21:6443",
                        namespace="crane-testkit",
                    )
        insecure = self._config(
            {
                "client-certificate-data": base64.b64encode(b"certificate").decode(),
                "client-key-data": base64.b64encode(b"key").decode(),
            }
        )
        insecure["clusters"][0]["cluster"]["insecure-skip-tls-verify"] = True  # type: ignore[index]
        with self.assertRaisesRegex(validation.ValidationError, "transport"):
            validation._validate_kubeconfig_contract(
                insecure,
                expected_server="https://192.168.1.21:6443",
                namespace="crane-testkit",
            )

    @unittest.skipUnless(validation.OPENSSL.is_file(), "OpenSSL is required")
    def test_client_certificate_identity_and_key_are_verified(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            certificate = root / "client.crt"
            private_key = root / "client.key"
            subprocess.run(
                [
                    str(validation.OPENSSL),
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    f"/CN={validation.EXPECTED_CLIENT_CERT_CN}",
                    "-keyout",
                    str(private_key),
                    "-out",
                    str(certificate),
                ],
                check=True,
                capture_output=True,
            )
            validation._validate_client_certificate(
                certificate.read_bytes(), private_key.read_bytes()
            )
            wrong_certificate = root / "wrong.crt"
            wrong_key = root / "wrong.key"
            subprocess.run(
                [
                    str(validation.OPENSSL),
                    "req",
                    "-x509",
                    "-newkey",
                    "rsa:2048",
                    "-nodes",
                    "-days",
                    "1",
                    "-subj",
                    "/CN=system:serviceaccount:crane-testkit:wrong",
                    "-keyout",
                    str(wrong_key),
                    "-out",
                    str(wrong_certificate),
                ],
                check=True,
                capture_output=True,
            )
            with self.assertRaisesRegex(validation.ValidationError, "identity"):
                validation._validate_client_certificate(
                    wrong_certificate.read_bytes(), wrong_key.read_bytes()
                )
            with self.assertRaisesRegex(validation.ValidationError, "do not match"):
                validation._validate_client_certificate(
                    certificate.read_bytes(), wrong_key.read_bytes()
                )

    def test_client_certificate_must_cover_four_hour_job_window(self) -> None:
        certificate = b"-----BEGIN CERTIFICATE-----\nfixture\n"
        private_key = b"-----BEGIN PRIVATE KEY-----\nfixture\n"

        def validate_with_remaining(remaining: int) -> None:
            def openssl(arguments, _material, _label):
                if "-subject" in arguments:
                    return f"subject=CN={validation.EXPECTED_CLIENT_CERT_CN}\n".encode()
                if "-checkend" in arguments:
                    requested = int(arguments[arguments.index("-checkend") + 1])
                    self.assertEqual(
                        requested, validation.MIN_CLIENT_CERT_VALIDITY_SECONDS
                    )
                    if remaining < requested:
                        raise validation.ValidationError(
                            "check CI client certificate validity failed"
                        )
                    return b""
                return b"matching-public-key"

            with mock.patch.object(validation, "_openssl", side_effect=openssl):
                validation._validate_client_certificate(certificate, private_key)

        with self.assertRaisesRegex(validation.ValidationError, "validity"):
            validate_with_remaining(validation.MIN_CLIENT_CERT_VALIDITY_SECONDS - 1)
        validate_with_remaining(validation.MIN_CLIENT_CERT_VALIDITY_SECONDS)


class KubernetesPermissionCommandTest(unittest.TestCase):
    def test_subresources_use_explicit_kubectl_flag_instead_of_resource_name(
        self,
    ) -> None:
        for subresource in ("exec", "attach", "portforward", "log"):
            with self.subTest(subresource=subresource):
                command = validation._can_i_command(
                    Path("/usr/local/bin/k3s"),
                    "get",
                    f"pods/{subresource}",
                    "crane-testkit",
                )
                self.assertEqual(
                    command,
                    [
                        "/usr/local/bin/k3s",
                        "kubectl",
                        "auth",
                        "can-i",
                        "get",
                        "pods",
                        f"--subresource={subresource}",
                        "--namespace",
                        "crane-testkit",
                    ],
                )

    def test_plain_resource_does_not_add_subresource_flag(self) -> None:
        command = validation._can_i_command(
            Path("/usr/local/bin/k3s"), "list", "jobs.batch", "crane-testkit"
        )
        self.assertNotIn("--subresource", " ".join(command))
        self.assertIn("jobs.batch", command)


class AdmissionValidationTest(unittest.TestCase):
    def _job(self) -> dict[str, object]:
        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "probe", "namespace": "crane-testkit"},
            "spec": {
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "autotest",
                                "image": "example.invalid/autotest@sha256:" + "a" * 64,
                            }
                        ],
                        "volumes": [
                            {
                                "name": "run-input",
                                "nfs": {
                                    "server": "192.168.1.4",
                                    "path": "/nfs/runs/ci-admission-probe/input",
                                },
                            }
                        ],
                    }
                }
            },
        }

    def test_approved_job_passes_and_all_forbidden_mutations_are_policy_denied(
        self,
    ) -> None:
        approved = self._job()
        calls = []

        def submit(job):
            calls.append(job)
            if job == approved:
                return subprocess.CompletedProcess([], 0, "job.batch/probe", "")
            return subprocess.CompletedProcess(
                [], 1, "", f"denied by {validation.ADMISSION_POLICY_NAME}"
            )

        validation._validate_admission_boundary([approved], submit)
        self.assertEqual(len(calls), 6)
        labels = {
            label for label, _job, _message in validation._admission_mutations(approved)
        }
        self.assertEqual(
            labels, {"nodeName", "lifecycle", "hostNetwork", "image", "NFS path"}
        )

    def test_missing_or_unrelated_admission_policy_fails_closed(self) -> None:
        approved = self._job()

        def accepts_everything(_job):
            return subprocess.CompletedProcess([], 0, "job.batch/probe", "")

        with self.assertRaisesRegex(validation.ValidationError, "accepted forbidden"):
            validation._validate_admission_boundary([approved], accepts_everything)

        calls = 0

        def unrelated_rejection(_job):
            nonlocal calls
            calls += 1
            if calls == 1:
                return subprocess.CompletedProcess([], 0, "job.batch/probe", "")
            return subprocess.CompletedProcess([], 1, "", "quota exceeded")

        with self.assertRaisesRegex(
            validation.ValidationError, "expected admission policy"
        ):
            validation._validate_admission_boundary([approved], unrelated_rejection)


if __name__ == "__main__":
    unittest.main()
