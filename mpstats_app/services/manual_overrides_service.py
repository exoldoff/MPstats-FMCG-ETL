from __future__ import annotations

from typing import Any

import pandas as pd

from mpstats_app.config import AppSettings
from pipeline.repositories.manual_override_repository import (
    MANUAL_OVERRIDE_COLUMNS,
    read_manual_overrides,
    write_manual_overrides,
)
from pipeline.services.manual_override_service import ALLOWED_MANUAL_OVERRIDE_MODES


class ManualOverridesService:
    def __init__(self, *, settings: AppSettings) -> None:
        self.settings = settings

    def list_overrides(self) -> dict[str, Any]:
        frame = read_manual_overrides(self.settings.manual_overrides_path)
        return {"path": str(self.settings.manual_overrides_path), "overrides": self._overrides_from_frame(frame)}

    def save_overrides(self, overrides: list[dict[str, Any]]) -> dict[str, Any]:
        frame = self._frame_from_overrides(overrides)
        write_manual_overrides(self.settings.manual_overrides_path, frame)
        return self.list_overrides()

    def _overrides_from_frame(self, frame: pd.DataFrame) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for index, row in enumerate(frame.to_dict(orient="records")):
            out.append(
                {
                    "id": f"manual-override-{index}",
                    "active": self._to_bool(row.get("active", "")),
                    "priority": self._to_int(row.get("priority"), 100),
                    "match_field": str(row.get("match_field", "")).strip(),
                    "match_value": str(row.get("match_value", "")).strip(),
                    "target_column": str(row.get("target_column", "")).strip(),
                    "set_value": str(row.get("set_value", "")).strip(),
                    "mode": str(row.get("mode", "") or "overwrite").strip().lower(),
                    "comment": str(row.get("comment", "")).strip(),
                }
            )
        return out

    def _frame_from_overrides(self, overrides: list[dict[str, Any]]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for index, override in enumerate(overrides, start=2):
            mode = str(override.get("mode", "overwrite") or "overwrite").strip().lower()
            if mode not in ALLOWED_MANUAL_OVERRIDE_MODES:
                raise ValueError(f"Manual override row {index} has invalid mode '{mode}'.")

            row = {
                "active": "1" if bool(override.get("active", True)) else "0",
                "priority": self._to_int(override.get("priority"), 100),
                "match_field": str(override.get("match_field", "")).strip(),
                "match_value": str(override.get("match_value", "")).strip(),
                "target_column": str(override.get("target_column", "")).strip(),
                "set_value": str(override.get("set_value", "")).strip(),
                "mode": mode,
                "comment": str(override.get("comment", "")).strip(),
            }
            if row["active"] == "1":
                for column in ("match_field", "match_value", "target_column", "set_value"):
                    if row[column] == "":
                        raise ValueError(f"Column '{column}' must be non-empty in active manual override row {index}.")
            rows.append(row)

        return pd.DataFrame(rows, columns=list(MANUAL_OVERRIDE_COLUMNS))

    @staticmethod
    def _to_bool(value: object) -> bool:
        text = str(value).strip().lower()
        return text in {"1", "true", "yes", "y", "on", "да"}

    @staticmethod
    def _to_int(value: object, fallback: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return fallback
