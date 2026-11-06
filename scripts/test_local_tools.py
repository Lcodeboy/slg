#!/usr/bin/env python3
"""Safety properties for the local launcher; no game service is started or stopped."""
import os
import socket
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest.mock import patch

import local_services


class LauncherTests(unittest.TestCase):
    projection_config = ("SLG_PROJECTION_JDBC_URL=jdbc:mysql://127.0.0.1/slg_projection\n"
                         "SLG_PROJECTION_DB_USER=slg_projection\nSLG_PROJECTION_DB_PASSWORD=projection-only\n")

    @staticmethod
    def mysql_config(root):
        path = root / "mysql.env"
        path.write_text("SLG_UC_JDBC_URL=jdbc:mysql://127.0.0.1/uc\nSLG_DB_PASSWORD=test-fixture-only\n"
                        + LauncherTests.projection_config)
        return path

    def test_env_file_is_literal_not_shell_code(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config"
            path.write_text("SLG_TOKEN_SECRET='$(touch should-never-run)'\nEMPTY=\nURL='jdbc:mysql://localhost/test?sslMode=REQUIRED&connectTimeout=5000'\n")
            values = local_services.read_env(path)
            self.assertEqual("$(touch should-never-run)", values["SLG_TOKEN_SECRET"])
            self.assertEqual("", values["EMPTY"])
            self.assertIn("&connectTimeout=5000", values["URL"])
            self.assertFalse(Path(directory, "should-never-run").exists())

    def test_generated_secret_is_private_stable_and_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            config = self.mysql_config(root)
            with patch.object(local_services, "ROOT", root):
                first = local_services.load_env(config, create_secret=True)
                second = local_services.load_env(config, create_secret=True)
            self.assertGreaterEqual(len(first["SLG_TOKEN_SECRET"]), 32)
            self.assertEqual(first["SLG_TOKEN_SECRET"], second["SLG_TOKEN_SECRET"])
            self.assertEqual(0o600, (root / "data" / "local.env").stat().st_mode & 0o777)

    def test_h2_is_rejected_and_rpc_ports_are_distinct(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            config = self.mysql_config(root)
            with patch.object(local_services, "ROOT", root):
                env = local_services.load_env(config)
                self.assertEqual("127.0.0.1:9082", env["SLG_GAME_RPC_TARGET"])
                self.assertEqual("127.0.0.1:9083", env["SLG_BATTLE_RPC_TARGET"])
                self.assertEqual("8090", env["SLG_GATE_TCP_PORT"])
                with patch.dict(os.environ, {"SLG_GATE_TCP_PORT": "9082"}):
                    with self.assertRaisesRegex(ValueError, "distinct"):
                        local_services.load_env(config)
                with patch.dict(os.environ, {"SLG_BATTLE_RPC_PORT":"8080"}):
                    with self.assertRaisesRegex(ValueError, "distinct"):
                        local_services.load_env(config)
                config.write_text("SLG_UC_JDBC_URL=jdbc:h2:mem:uc\n")
                with self.assertRaisesRegex(ValueError, "must use MySQL"):
                    local_services.load_env(config)

    def test_start_rejects_an_unbound_volume_before_build_or_process_launch(self):
        with patch("subprocess.run") as command, patch("subprocess.Popen") as child:
            with self.assertRaisesRegex(ValueError, "SLG_REALM_DIR"):
                local_services.start({}, no_build=False)
            command.assert_not_called()
            child.assert_not_called()

    def test_realm_identity_must_be_explicit_and_reuses_the_same_volume(self):
        with tempfile.TemporaryDirectory() as directory:
            volume = str(uuid.uuid4())
            env = {"SLG_REALM_DIR": directory, "SLG_REALM_ID": "1", "SLG_REALM_VOLUME_ID": volume}
            self.assertEqual([directory, "1", volume], local_services.realm_identity(env))
            self.assertEqual([], list(Path(directory).iterdir()))
            for key, value in [("SLG_REALM_DIR", "relative"), ("SLG_REALM_ID", "0"),
                               ("SLG_REALM_VOLUME_ID", volume.replace("-", ""))]:
                with self.assertRaises(ValueError):
                    local_services.realm_identity({**env, key: value})

    def test_worker_does_not_receive_account_secrets(self):
        env = {"SLG_TOKEN_SECRET": "token", "SLG_UC_DB_PASSWORD": "UC-only", "SLG_DB_PASSWORD": "shared",
               "SLG_PROJECTION_DB_PASSWORD": "projection-only", "SLG_PROJECTION_JDBC_URL": "projection-url",
               "SLG_MYSQL_ROOT_PASSWORD": "container-only", "SLG_BATTLE_RPC_PORT": "9999"}
        for role in ("uc", "battle", "game", "gate"):
            child = local_services.service_environment(role, env)
            self.assertEqual(role != "battle", "SLG_TOKEN_SECRET" in child)
            for key in ["SLG_UC_DB_PASSWORD", "SLG_DB_PASSWORD"]:
                self.assertEqual(role == "uc", key in child)
            for key in ["SLG_PROJECTION_DB_PASSWORD", "SLG_PROJECTION_JDBC_URL"]:
                self.assertEqual(role == "game", key in child)
            self.assertNotIn("SLG_MYSQL_ROOT_PASSWORD", child)
            self.assertEqual(role in ("battle", "game"), "SLG_BATTLE_RPC_PORT" in child)

    def test_each_rpc_client_receives_only_its_own_identity(self):
        env = {
            "SLG_RPC_TLS_CA": "service-ca", "SLG_RPC_TLS_CERT": "server-cert", "SLG_RPC_TLS_KEY": "server-key",
            "SLG_RPC_CLIENT_TLS_CERT": "gateway-cert", "SLG_RPC_CLIENT_TLS_KEY": "gateway-key",
            "SLG_BATTLE_CLIENT_TLS_CERT": "game-cert", "SLG_BATTLE_CLIENT_TLS_KEY": "game-key",
            "SLG_BATTLE_GAME_CLIENT_CA": "game-only-ca",
            "SLG_GATE_TCP_TLS_KEY": "public-gateway-identity",
        }
        for name, identity in [("uc", None), ("battle", None), ("game", "game"), ("gate", "gateway")]:
            with self.subTest(role=name):
                child = local_services.service_environment(name, env)
                for field in ("CERT", "KEY"):
                    generic = "SLG_RPC_CLIENT_TLS_" + field
                    expected = identity + "-" + field.lower() if identity else None
                    self.assertEqual(expected, child.get(generic))
                    self.assertNotIn("SLG_BATTLE_CLIENT_TLS_" + field, child)
                    self.assertEqual(name != "gate", ("SLG_RPC_TLS_" + field) in child)
                self.assertEqual(name == "battle", "SLG_BATTLE_GAME_CLIENT_CA" in child)
                self.assertEqual(name == "gate", "SLG_GATE_TCP_TLS_KEY" in child)
        gateway_only = {k: v for k, v in env.items() if not k.startswith("SLG_BATTLE_CLIENT_TLS_")}
        game = local_services.service_environment("game", gateway_only)
        self.assertNotIn("SLG_RPC_CLIENT_TLS_KEY", game)
        self.assertNotIn("SLG_RPC_CLIENT_TLS_CERT", game)

    def test_gateway_readiness_requires_the_recorded_gameplay_listener(self):
        record = {"port": 8080, "probe_host": "127.0.0.1"}
        with patch.object(local_services, "health", return_value=True), patch("socket.create_connection") as connect:
            self.assertFalse(local_services.service_ready("gate", record))
            connect.assert_not_called()
            record.update(tcp_port=18090, tcp_probe_host="::1")
            self.assertTrue(local_services.service_ready("gate", record))
            connect.assert_called_once_with(("::1", 18090), timeout=1)
            connect.side_effect = ConnectionRefusedError()
            self.assertFalse(local_services.service_ready("gate", record))

    def test_worker_probe_and_default_rpc_target_follow_its_bind_address(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            config = self.mysql_config(root)
            for bind, expected in [("127.0.0.2", "127.0.0.2"), ("10.0.0.5", "10.0.0.5"),
                                   ("0.0.0.0", "127.0.0.1"), ("::", "::1"), ("::1", "::1")]:
                with self.subTest(bind=bind), patch.object(local_services, "ROOT", root), patch.dict(
                        os.environ, {"SLG_BATTLE_RPC_BIND": bind}):
                    env = local_services.load_env(config)
                    self.assertEqual(local_services.endpoint(expected, 9083), env["SLG_BATTLE_RPC_TARGET"])
                    record = {"port": 9083, "probe_host": local_services.probe_host(bind)}
                    with patch("socket.create_connection") as connect:
                        self.assertTrue(local_services.service_ready("battle", record))
                        connect.assert_called_once_with((expected, 9083), timeout=1)

    def test_reused_pid_never_receives_signal(self):
        record = {"pid": 12345, "jar": "/expected/slg_game.jar", "fingerprint": "old java -jar /expected/slg_game.jar"}
        with patch.object(local_services, "fingerprint", return_value="new unrelated-process"), patch.object(os, "kill") as kill:
            with self.assertRaises(RuntimeError):
                local_services.stop_one("game", record)
            kill.assert_not_called()

    def test_exact_process_fingerprint_and_jar_are_required(self):
        record = {"pid": 12345, "jar": "/expected/slg_game.jar", "fingerprint": "old java -jar /expected/slg_game.jar"}
        with patch.object(local_services, "fingerprint", return_value=record["fingerprint"]):
            self.assertTrue(local_services.is_owned(record))
            record["jar"] = "/different/slg_game.jar"
            self.assertFalse(local_services.is_owned(record))

    def test_duplicate_ports_are_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"SLG_UC_PORT": "8080", "SLG_GATE_PORT": "08080"}, clear=True):
            root = Path(directory)
            config = self.mysql_config(root)
            with patch.object(local_services, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "distinct"):
                    local_services.load_env(config)

    def test_default_configuration_reuses_mysql_credentials_and_session_secret(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            (root / "data").mkdir()
            local = root / "data/local.env"
            mysql = root / "data/mysql.env"
            local.write_text("SLG_TOKEN_SECRET=" + "existing-session-secret-" * 2 + "\n")
            mysql.write_text("SLG_UC_JDBC_URL=jdbc:mysql://127.0.0.1:13306/slg_uc\n"
                             "SLG_UC_DB_USER=slg_uc\n"
                             "SLG_UC_DB_PASSWORD=uc-secret\n" + self.projection_config)
            (root / ".env").write_text("SLG_UC_JDBC_URL=jdbc:h2:mem:unexpected\n")
            before = (local.read_bytes(), mysql.read_bytes())
            with patch.object(local_services, "ROOT", root):
                env = local_services.load_env(create_secret=True)
            self.assertEqual("jdbc:mysql://127.0.0.1:13306/slg_uc", env["SLG_UC_JDBC_URL"])
            self.assertEqual("uc-secret", env["SLG_UC_DB_PASSWORD"])
            self.assertEqual("existing-session-secret-" * 2, env["SLG_TOKEN_SECRET"])
            self.assertEqual(before, (local.read_bytes(), mysql.read_bytes()))

    def test_explicit_file_does_not_inherit_default_mysql_credentials(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            (root / "data").mkdir()
            (root / "data/mysql.env").write_text("SLG_UC_DB_USER=slg_uc\nSLG_UC_DB_PASSWORD=mysql-only\n")
            config = self.mysql_config(root)
            with patch.object(local_services, "ROOT", root):
                env = local_services.load_env(config)
            self.assertEqual("test-fixture-only", env["SLG_DB_PASSWORD"])
            self.assertNotIn("SLG_UC_DB_USER", env)

    def test_missing_mysql_credentials_fail_before_creating_session_files(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            with patch.object(local_services, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "MySQL requires SLG_UC_DB_PASSWORD"):
                    local_services.load_env(create_secret=True)
            self.assertFalse((root / "data/local.env").exists())

    def test_explicit_missing_file_is_not_silently_replaced_by_default_mysql(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"SLG_DB_PASSWORD": "configured"}, clear=True):
            root = Path(directory)
            with patch.object(local_services, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "Configuration file does not exist"):
                    local_services.load_env(root / "missing.env")

    def test_environment_overrides_selected_file_and_explicit_empty_password_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "remote.env"
            config.write_text("SLG_DB_PASSWORD=file-secret\n" + self.projection_config)
            with patch.object(local_services, "ROOT", root), patch.dict(os.environ,
                    {"SLG_DB_PASSWORD": "env-secret", "SLG_UC_DB_PASSWORD": "uc-secret"}, clear=True):
                env = local_services.load_env(config)
                self.assertEqual("env-secret", env["SLG_DB_PASSWORD"])
                self.assertEqual("uc-secret", env["SLG_UC_DB_PASSWORD"])
            with patch.object(local_services, "ROOT", root), patch.dict(os.environ,
                    {"SLG_DB_PASSWORD": "env-secret", "SLG_UC_DB_PASSWORD": ""}, clear=True):
                with self.assertRaisesRegex(ValueError, "SLG_UC_DB_PASSWORD"):
                    local_services.load_env(config)

    def test_mysql_can_be_configured_entirely_through_environment(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ,
                {"SLG_DB_PASSWORD": "env-secret", "SLG_PROJECTION_ENABLED": "false"}, clear=True):
            with patch.object(local_services, "ROOT", Path(directory)):
                env = local_services.load_env()
            self.assertEqual("env-secret", env["SLG_DB_PASSWORD"])

    def test_explicit_rocks_only_fixture_and_projection_preflight(self):
        # 缺少投影凭证不能悄悄用 UC 的共享账号。只有显式关闭时可运行纯原卷 fixture。
        env = {"SLG_DB_PASSWORD": "account-only"}
        with self.assertRaisesRegex(ValueError, "SLG_PROJECTION_JDBC_URL"):
            local_services.validate_database_environment(env)
        env["SLG_PROJECTION_JDBC_URL"] = "jdbc:mysql://127.0.0.1/slg_projection"
        with self.assertRaisesRegex(ValueError, "SLG_PROJECTION_DB_PASSWORD"):
            local_services.validate_database_environment(env)
        with self.assertRaisesRegex(ValueError, "true or false"):
            local_services.validate_database_environment({**env, "SLG_PROJECTION_ENABLED": "typo"})
        local_services.validate_database_environment({**env, "SLG_PROJECTION_ENABLED": "false"})

    def test_java_launcher_path_change_preserves_identity_but_start_time_does_not(self):
        record = {"pid": 12345, "jar": "/expected/slg_game.jar",
                  "fingerprint": "Mon Nov  2 18:13:32 2026 java -jar /expected/slg_game.jar"}
        with patch.object(local_services, "fingerprint", return_value="Mon Nov  2 18:13:32 2026 /usr/bin/java -jar /expected/slg_game.jar"):
            self.assertTrue(local_services.is_owned(record))
        with patch.object(local_services, "fingerprint", return_value="Mon Nov  2 18:13:33 2026 /usr/bin/java -jar /expected/slg_game.jar"):
            self.assertFalse(local_services.is_owned(record))
        with patch.object(local_services, "fingerprint", return_value="Mon Nov  2 18:13:32 2026 /usr/bin/java -jar /expected/slg_game.jar --other"):
            self.assertFalse(local_services.is_owned(record))

    def test_port_probe_rejects_listener_and_allows_closed_tcp_connection(self):
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
            listener.listen(1)
            self.assertFalse(local_services.available("127.0.0.1", port))
            with socket.create_connection(("127.0.0.1", port), timeout=2) as client:
                connection, _ = listener.accept()
                connection.shutdown(socket.SHUT_RDWR)
                connection.close()
                self.assertEqual(b"", client.recv(1))
        self.assertTrue(local_services.available("127.0.0.1", port))


if __name__ == "__main__":
    unittest.main()
