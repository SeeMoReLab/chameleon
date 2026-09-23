"""Exercise the remote runner without SSH, real JVMs, or host/network mutations."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).resolve().parents[3] / "cloudlab_remote_run.sh"
MOCK = r'''#!/usr/bin/env python3
import json, os, pathlib, sys, time
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
if name == "java":
    with open("java_calls.jsonl", "a") as stream:
        stream.write(json.dumps(args) + "\n")
    if "org.example.Utility.ExperimentConfig" in args:
        print("transport=framed\nserverHosts=127.0.0.1")
    elif "-version" in args:
        print('openjdk version "17"')
    else:
        for arg in args:
            if arg.startswith("-XX:StartFlightRecording="):
                filename = arg.split("filename=", 1)[1].split(",", 1)[0]
                pathlib.Path(filename).write_bytes(b"mock recording")
        time.sleep(0.2)
elif name == "ss":
    counter = pathlib.Path("ss_calls")
    count = int(counter.read_text()) + 1 if counter.exists() else 1
    counter.write_text(str(count))
    if count >= int(os.environ.get("FAIL_SS_AT", "999999")):
        print("injected socket collector failure", file=sys.stderr)
        sys.exit(7)
    print("ESTAB 0 123 127.0.0.1:1234 127.0.0.1:9001 retrans:0/0")
elif name == "ip":
    print("1: lo: <LOOPBACK,UP>")
elif name == "tc":
    print("qdisc netem backlog 0b 0p dropped 0")
elif name == "chronyc":
    print("System time: 0.000001 seconds fast of NTP time; Leap status: Normal")
elif name == "cat":
    for arg in args:
        if arg.startswith("/proc/"):
            print("mock network counters")
        else:
            print(pathlib.Path(arg).read_text(), end="")
'''


class CloudLabDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        commands = self.root / "bin"
        commands.mkdir()
        for name in ("java", "ss", "tc", "ip", "chronyc", "cat"):
            command = commands / name
            command.write_text(MOCK)
            command.chmod(0o755)
        self.environment = dict(os.environ, PATH=str(commands) + os.pathsep + os.environ["PATH"])

    def run_remote(self, enabled=True, role="client", fail_at=None):
        if fail_at is not None:
            self.environment["FAIL_SS_AT"] = str(fail_at)
        arguments = ["bash", str(RUNNER), "fake.jar", "config.yaml", "1", "test", "--role", role]
        if role == "servers":
            arguments += ["--server-index", "0"]
        if enabled:
            arguments += ["--diagnostics"]
        return subprocess.run(arguments, cwd=self.root, env=self.environment,
                              capture_output=True, text=True, timeout=15)

    def calls(self):
        return [json.loads(line) for line in (self.root / "java_calls.jsonl").read_text().splitlines()]

    def test_client_artifacts_and_jvm_flags(self):
        result = self.run_remote()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("0", (self.root / "diagnostics_status_client.txt").read_text().strip())
        self.assertTrue((self.root / "diagnostics_cpu_client.jfr").exists())
        log = (self.root / "diagnostics_system_client.log").read_text()
        self.assertIn("ClockTimestampMs=", log)
        self.assertIn("retrans:0/0", log)
        self.assertIn("netem", log)
        workload = next(call for call in self.calls() if "org.example.Client.WorkloadDriver" in call)
        self.assertIn("-Dtuneable.transportDiagnostics=true", workload)

    def test_disabled_does_not_enable_profiling_or_collectors(self):
        result = self.run_remote(enabled=False)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual([], list(self.root.glob("diagnostics_*")))
        self.assertFalse(any("-Dtuneable.transportDiagnostics=true" in call for call in self.calls()))

    def test_preflight_failure_prevents_workload_start(self):
        result = self.run_remote(fail_at=1)
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(any("org.example.Client.WorkloadDriver" in call for call in self.calls()))

    def test_background_failure_marks_run_failed(self):
        result = self.run_remote(fail_at=2)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("7", (self.root / "diagnostics_status_client.txt").read_text().strip())
        self.assertIn("exit_code:    2", (self.root / "run_info.txt").read_text())

    def test_server_artifacts_are_named_by_node(self):
        result = self.run_remote(role="servers")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertTrue((self.root / "diagnostics_cpu_server_0.jfr").exists())
        self.assertEqual("0", (self.root / "diagnostics_status_server_0.txt").read_text().strip())
        self.assertTrue((self.root / "servers_done").exists())


if __name__ == "__main__":
    unittest.main()
