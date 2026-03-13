from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "04_결과" / "01_리포트_codex"
COMMON_DIR = OUT_DIR / "01_매매시장"
INVEST_DIR = OUT_DIR / "05_투자검토"
UNIVERSE_PATH = INVEST_DIR / "05_투자검토대상군_20260313_codex_시군구.csv"
EXISTING_LABEL_PATH = COMMON_DIR / "01_매매시장_행동라벨_20260314_codex.csv"
LABEL_OUT = COMMON_DIR / "01_매매시장_행동라벨_20260314_codex.csv"
REGION_SUMMARY_OUT = COMMON_DIR / "01_매매시장_권역별행동요약_20260314_codex.csv"
SIDO_SUMMARY_OUT = COMMON_DIR / "01_매매시장_시도별행동요약_20260314_codex.csv"


def region_group(sido: str) -> str:
    if sido in {"서울특별시", "경기도", "인천광역시"}:
        return "수도권"
    if sido in {"부산광역시", "대구광역시", "광주광역시", "대전광역시", "울산광역시"}:
        return "지방광역시"
    return "기타지방"


def infer_action(row: pd.Series) -> str:
    review = row.get("투자검토분류")
    rise = row.get("상승확률점수", 0)
    suitability = row.get("투자적합성점수", 0)
    trade_recovery = row.get("trade_recovery_pct", 0)
    overheating = row.get("과열가능점수", 0)
    price_change = row.get("price_12m_change_pct", 0)

    if review == "즉시 제외":
        return "회피"
    if review == "우선 검토":
        if pd.notna(rise) and rise >= 60:
            return "우선 매수 검토"
        return "관찰 유지"
    if review == "판단 보류":
        if (
            (pd.notna(trade_recovery) and trade_recovery > 0 and pd.notna(suitability) and suitability >= 75)
            or (pd.notna(overheating) and overheating >= 65)
            or (pd.notna(price_change) and price_change > 12 and pd.notna(suitability) and suitability >= 80)
        ):
            return "관찰 유지"
        return "보수 접근"
    return "보수 접근"


def build_sale_action_frame() -> pd.DataFrame:
    universe = pd.read_csv(UNIVERSE_PATH, encoding="utf-8-sig")
    overrides = pd.read_csv(EXISTING_LABEL_PATH, encoding="utf-8-sig")
    override_labels = overrides[["시도", "시군구", "심화행동라벨"]].drop_duplicates()

    universe["권역"] = universe["시도"].map(region_group)
    universe["심화행동라벨"] = universe.apply(infer_action, axis=1)

    merged = universe.merge(
        override_labels,
        on=["시도", "시군구"],
        how="left",
        suffixes=("", "_override"),
    )
    merged["심화행동라벨"] = merged["심화행동라벨_override"].fillna(merged["심화행동라벨"])
    merged = merged.drop(columns=["심화행동라벨_override"])

    columns = [
        "심화행동라벨",
        "권역",
        "시도",
        "시군구",
        "예측분류",
        "투자검토분류",
        "상승확률점수",
        "투자적합성점수",
        "저평가가능점수",
        "촉매점수",
        "과열가능점수",
        "trade_recovery_pct",
        "jeonse_ratio_pct",
        "미래입주압력_18개월_pct",
        "저평가가능비중_pct",
        "과대반영비중_pct",
        "정책메모",
        "투자검토분류근거",
        "재검토조건",
    ]
    out = merged[columns].rename(columns={"미래입주압력_18개월_pct": "기존세대수대비_향후18개월입주예정물량_pct"})
    return out.sort_values(
        ["심화행동라벨", "권역", "상승확률점수", "투자적합성점수"],
        ascending=[True, True, False, False],
    ).reset_index(drop=True)


def build_region_summary(frame: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        frame.groupby(["권역", "심화행동라벨"], as_index=False)
        .agg(
            지역수=("시군구", "count"),
            평균상승확률점수=("상승확률점수", "mean"),
            평균투자적합성점수=("투자적합성점수", "mean"),
            평균과열가능점수=("과열가능점수", "mean"),
            평균거래회복률=("trade_recovery_pct", "mean"),
        )
    )
    for col in ["평균상승확률점수", "평균투자적합성점수", "평균과열가능점수", "평균거래회복률"]:
        grouped[col] = grouped[col].round(2)
    return grouped.sort_values(["권역", "심화행동라벨"]).reset_index(drop=True)


def build_sido_summary(frame: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        frame.groupby(["권역", "시도", "심화행동라벨"], as_index=False)
        .agg(
            지역수=("시군구", "count"),
            평균상승확률점수=("상승확률점수", "mean"),
            평균투자적합성점수=("투자적합성점수", "mean"),
        )
    )
    for col in ["평균상승확률점수", "평균투자적합성점수"]:
        grouped[col] = grouped[col].round(2)
    return grouped.sort_values(["권역", "시도", "심화행동라벨"]).reset_index(drop=True)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    frame = build_sale_action_frame()
    region_summary = build_region_summary(frame)
    sido_summary = build_sido_summary(frame)

    frame.to_csv(LABEL_OUT, index=False, encoding="utf-8-sig")
    region_summary.to_csv(REGION_SUMMARY_OUT, index=False, encoding="utf-8-sig")
    sido_summary.to_csv(SIDO_SUMMARY_OUT, index=False, encoding="utf-8-sig")

    print(f"Wrote {LABEL_OUT.relative_to(ROOT)}")
    print(f"Wrote {REGION_SUMMARY_OUT.relative_to(ROOT)}")
    print(f"Wrote {SIDO_SUMMARY_OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
