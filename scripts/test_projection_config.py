"""检验开发卷补配置、重跑及 SQL 迁移权限边界，不使用本机的开发凭证。"""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import mysql_local
from local_services import read_env


class ProjectionConfigurationTests(unittest.TestCase):
    def test_preparing_an_old_private_file_preserves_credentials_and_custom_port(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {}, clear=True):
            config = Path(temp) / "mysql.env"
            original = (b"# existing operator comment\nSLG_MYSQL_PORT=14001\n"
                        b"SLG_UC_DB_PASSWORD=uc-old-value\nSLG_MYSQL_ROOT_PASSWORD=root-old-value")
            config.write_bytes(original)
            mysql_local.prepare(config)
            first = config.read_bytes()
            values = read_env(config)
            self.assertTrue(first.startswith(original))
            self.assertEqual("slg_projection", values["SLG_PROJECTION_DB_USER"])
            self.assertEqual(mysql_local.projection_url(14001), values["SLG_PROJECTION_JDBC_URL"])
            self.assertRegex(values["SLG_PROJECTION_DB_PASSWORD"], r"^[A-Za-z0-9_-]{24,128}$")
            self.assertEqual(0o600, config.stat().st_mode & 0o777)
            mysql_local.prepare(config)
            self.assertEqual(first, config.read_bytes())

    def test_preserves_an_existing_projection_password_when_adding_missing_fields(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {}, clear=True):
            config = Path(temp) / "mysql.env"
            password = "existing-fixture-only-password"
            config.write_text("SLG_PROJECTION_DB_PASSWORD=" + password + "\n")
            mysql_local.prepare(config)
            self.assertEqual(password, read_env(config)["SLG_PROJECTION_DB_PASSWORD"])

    def test_new_config_contains_independent_passwords_and_is_validated(self):
        with tempfile.TemporaryDirectory() as temp, patch.dict(os.environ, {}, clear=True):
            config = Path(temp) / "mysql.env"
            mysql_local.prepare(config)
            env = mysql_local.configuration(config)
            passwords = {env[key] for key in ("SLG_MYSQL_ROOT_PASSWORD", "SLG_UC_DB_PASSWORD",
                                                "SLG_PROJECTION_DB_PASSWORD")}
            self.assertEqual(3, len(passwords))
            with patch.dict(os.environ, {"SLG_PROJECTION_DB_PASSWORD": ""}):
                with self.assertRaisesRegex(ValueError, "SLG_PROJECTION_DB_PASSWORD"):
                    mysql_local.configuration(config)

    def test_provision_streams_the_canonical_schema_and_never_puts_credentials_in_argv(self):
        root = "root-fixture-password"
        runtime = "runtime-fixture-password"
        env = {"SLG_MYSQL_ROOT_PASSWORD": root, "SLG_PROJECTION_DB_PASSWORD": runtime}
        calls = []
        def inspect(command, **options):
            ddl = options.get("input")
            calls.append((command, options["env"]["MYSQL_PWD"], ddl))
        with patch.object(mysql_local.subprocess, "run", side_effect=inspect):
            mysql_local.provision(["docker", "compose"], env)
        first, second = calls
        self.assertEqual(root, first[1])
        migrations = mysql_local.ROOT / "scripts/mysql/schema"
        self.assertEqual(b"\n".join(p.read_bytes() for p in (migrations / "001-realm-projection.sql",
                                                       migrations / "003-role-identity-projections.sql")), first[2])
        self.assertEqual(runtime, second[1])
        for command, _, _ in calls:
            self.assertNotIn(root, " ".join(command))
            self.assertNotIn(runtime, " ".join(command))
        self.assertIn("--user=slg_projection", second[0])
        self.assertIn("projection_resource_site", second[0][-1])


if __name__ == "__main__":
    unittest.main()
