from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from pipeline.repositories.sql_repository import connect, duckdb_transaction


class DuckDBConnectionHelperTest(unittest.TestCase):
    def test_connect_uses_env_settings_and_creates_temp_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            duckdb_temp = root / "duckdb_tmp"
            with patch.dict(
                os.environ,
                {
                    "DUCKDB_THREADS": "1",
                    "DUCKDB_MEMORY_LIMIT": "2GB",
                    "DUCKDB_TEMP_DIRECTORY": str(duckdb_temp),
                },
                clear=False,
            ):
                with connect(root / "settings.duckdb") as con:
                    self.assertEqual(con.execute("SELECT current_setting('threads')").fetchone()[0], 1)
                    self.assertIn("GiB", str(con.execute("SELECT current_setting('memory_limit')").fetchone()[0]))
                    self.assertEqual(
                        Path(str(con.execute("SELECT current_setting('temp_directory')").fetchone()[0])).resolve(),
                        duckdb_temp.resolve(),
                    )
            self.assertTrue(duckdb_temp.exists())

    def test_read_only_connection_rejects_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "readonly.duckdb"
            with connect(db_path, use_env=False) as con:
                con.execute("CREATE TABLE items (id INTEGER)")
                con.execute("INSERT INTO items VALUES (1)")

            with connect(db_path, read_only=True, use_env=False) as con:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM items").fetchone()[0], 1)
                with self.assertRaises(Exception):
                    con.execute("INSERT INTO items VALUES (2)")

    def test_duckdb_transaction_rolls_back_on_exception(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "transaction.duckdb"
            with connect(db_path, use_env=False) as con:
                con.execute("CREATE TABLE items (id INTEGER)")
                with self.assertRaises(RuntimeError):
                    with duckdb_transaction(con):
                        con.execute("INSERT INTO items VALUES (1)")
                        raise RuntimeError("boom")
                self.assertEqual(con.execute("SELECT COUNT(*) FROM items").fetchone()[0], 0)

                with duckdb_transaction(con):
                    con.execute("INSERT INTO items VALUES (2)")
                self.assertEqual(con.execute("SELECT COUNT(*) FROM items").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
