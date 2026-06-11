from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def issues_to_dataframe(issues: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for issue in issues:
        row = dict(issue)
        details = row.get("details")
        row["details"] = json.dumps(details or {}, ensure_ascii=False, sort_keys=True)
        rows.append(row)
    return pd.DataFrame(rows)


def write_issues_csv(issues: list[dict[str, Any]], path: str | Path) -> Path:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    issues_to_dataframe(issues).to_csv(output, sep=";", index=False, encoding="utf-8-sig")
    return output
