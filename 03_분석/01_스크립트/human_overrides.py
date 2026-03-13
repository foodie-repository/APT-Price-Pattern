from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OVERRIDE_PATH = ROOT / "02_데이터/02_참조/human_overrides.csv"
KEY_COLUMNS = ["시도", "시군구"]

ACTIVE_VALUES = {"1", "y", "yes", "true", "예"}
SCOPE_ALIAS = {
    "all": {"all", "common", "공통"},
    "gate": {"gate", "investment_gate", "투자검토", "투자검토게이트"},
    "prediction": {"prediction", "report", "예측", "예측검증"},
}


def _empty_override_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *KEY_COLUMNS,
            "scope",
            "active",
            "투자검토분류_override",
            "투자검토분류근거_override",
            "재검토조건_override",
            "사람검증셋판정_override",
            "사람검증셋메모_override",
            "점수요약_override",
            "note",
        ]
    )


def load_human_overrides(path: Path = OVERRIDE_PATH) -> pd.DataFrame:
    if not path.exists():
        return _empty_override_frame()
    overrides = pd.read_csv(path, dtype=str).fillna("")
    for column in _empty_override_frame().columns:
        if column not in overrides.columns:
            overrides[column] = ""
    return overrides[_empty_override_frame().columns].copy()


def _normalize_scope(value: str) -> str:
    text = (value or "").strip().lower()
    if not text:
        return "all"
    for canonical, aliases in SCOPE_ALIAS.items():
        if text in aliases:
            return canonical
    return text


def _is_active(value: str) -> bool:
    text = (value or "").strip().lower()
    if not text:
        return True
    return text in ACTIVE_VALUES


def apply_human_overrides(df: pd.DataFrame, scope: str, path: Path = OVERRIDE_PATH) -> pd.DataFrame:
    if df.empty:
        return df

    overrides = load_human_overrides(path)
    if overrides.empty:
        return df

    scope_key = _normalize_scope(scope)
    overrides["scope"] = overrides["scope"].map(_normalize_scope)
    overrides = overrides[overrides["active"].map(_is_active)]
    overrides = overrides[overrides["scope"].isin({"all", scope_key})]
    if overrides.empty:
        return df

    override_columns = [column for column in overrides.columns if column.endswith("_override")]
    base_columns = [column.removesuffix("_override") for column in override_columns]

    merged = df.copy()
    for base_column in base_columns:
        if base_column not in merged.columns:
            merged[base_column] = ""

    keep_columns = KEY_COLUMNS + override_columns
    override_view = overrides[keep_columns].drop_duplicates(KEY_COLUMNS, keep="last")
    merged = merged.merge(override_view, on=KEY_COLUMNS, how="left")

    for override_column in override_columns:
        base_column = override_column.removesuffix("_override")
        values = merged[override_column].fillna("").astype(str).str.strip()
        if base_column in merged.columns:
            merged[base_column] = merged[base_column].where(values.eq(""), values)

    return merged[df.columns.tolist() + [column for column in base_columns if column not in df.columns]]
