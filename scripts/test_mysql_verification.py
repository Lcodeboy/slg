#!/usr/bin/env python3
"""不启动真实容器，验证故障注入和清理必须通过同一套所有权检查。"""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("mysql_verification", Path(__file__).with_name("verify-mysql.py"))
verification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verification)

NAME = "slg-mysql-it-0123456789ab"
IDENTITY = "1" * 64


def inspection(run=NAME, identity=IDENTITY):
    labels = json.dumps({"com.slg.purpose": "mysql-integration-test", "com.slg.test-run": run})
    return subprocess.CompletedProcess([], 0, identity + " " + labels, "")


class CleanupTests(unittest.TestCase):
    def test_failed_run_still_attempts_cleanup_and_keeps_original_failure(self):
        timeout = subprocess.TimeoutExpired("docker run", 240)
        with patch.object(verification.socket, "socket"), patch.object(verification, "docker", side_effect=timeout), \
                patch.object(verification, "cleanup", side_effect=RuntimeError("cleanup failed")) as cleanup, \
                contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(subprocess.TimeoutExpired) as failure:
                verification.run("mysql:8.4")
        self.assertIs(timeout, failure.exception)
        cleanup.assert_called_once()
        self.assertTrue(cleanup.call_args.args[0].startswith("slg-mysql-it-"))

    def test_missing_container_needs_no_removal(self):
        missing = subprocess.CompletedProcess([], 1, "", "Error: No such object")
        with patch.object(verification.subprocess, "run", return_value=missing), patch.object(verification, "docker") as docker:
            self.assertFalse(verification.cleanup(NAME))
            docker.assert_not_called()

    def test_foreign_labels_or_replaced_container_cannot_be_stopped_started_or_removed(self):
        for foreign in [inspection(run="another-run"), inspection(identity="2" * 64)]:
            with patch.object(verification.subprocess, "run", return_value=foreign), patch.object(verification, "docker") as docker:
                for action in ["start", "stop"]:
                    with self.assertRaisesRegex(RuntimeError, "ownership"):
                        verification.change_mysql_state(NAME, IDENTITY, action)
                with self.assertRaisesRegex(RuntimeError, "ownership"):
                    verification.cleanup(NAME, IDENTITY)
                docker.assert_not_called()
        with patch.object(verification.subprocess, "run") as inspect:
            with self.assertRaises(RuntimeError):
                verification.change_mysql_state("slg-mysql-dev", IDENTITY, "stop")
            inspect.assert_not_called()

    def test_only_the_current_owned_container_and_its_anonymous_volume_are_removed(self):
        with patch.object(verification.subprocess, "run", return_value=inspection()), patch.object(verification, "docker") as docker:
            self.assertTrue(verification.cleanup(NAME, IDENTITY))
            docker.assert_called_once_with(["rm", "--force", "--volumes", IDENTITY], timeout=30)


if __name__ == "__main__":
    unittest.main()
