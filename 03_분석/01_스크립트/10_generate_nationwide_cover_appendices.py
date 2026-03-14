from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "04_결과" / "01_리포트_codex"
SALE_DIR = OUT_DIR / "01_매매시장"
LEASE_DIR = OUT_DIR / "02_임차시장"
POLICY_DIR = OUT_DIR / "03_정책영향"

SALE_PATH = SALE_DIR / "01_매매시장_행동라벨_20260314_codex.csv"
LEASE_PATH = LEASE_DIR / "02_임차시장_행동라벨임차검증_20260314_codex.csv"
POLICY_PATH = POLICY_DIR / "03_정책후회복후보_20260314_codex.csv"

LEASE_REGION_SUMMARY_OUT = LEASE_DIR / "02_임차시장_권역별유형요약_20260314_codex.csv"
LEASE_SIDO_SUMMARY_OUT = LEASE_DIR / "02_임차시장_시도별유형요약_20260314_codex.csv"
POLICY_REGION_SUMMARY_OUT = POLICY_DIR / "03_정책_권역별판정요약_20260314_codex.csv"
POLICY_SIDO_SUMMARY_OUT = POLICY_DIR / "03_정책_시도별판정요약_20260314_codex.csv"

SALE_APPENDIX_OUT = SALE_DIR / "01_매매시장분석_20260314_codex_전국커버부록.md"
LEASE_APPENDIX_OUT = LEASE_DIR / "02_임차시장분석_20260314_codex_전국커버부록.md"
POLICY_APPENDIX_OUT = POLICY_DIR / "03_정책영향분석_20260314_codex_전국커버부록.md"


def region_group(sido: str) -> str:
    if sido in {"서울특별시", "경기도", "인천광역시"}:
        return "수도권"
    if sido in {"부산광역시", "대구광역시", "광주광역시", "대전광역시", "울산광역시"}:
        return "지방광역시"
    return "기타지방"


def build_lease_summaries(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    def summarize(group_cols: list[str]) -> pd.DataFrame:
        out = (
            df.groupby(group_cols, as_index=False)
            .agg(
                지역수=("시군구", "count"),
                임차수요기반양호_지역수=("임차수요기반양호", lambda s: int((s == "예").sum())),
                월세비중높음_지역수=("월세비중높음", lambda s: int((s == "예").sum())),
                임차선행_지역수=("임차선행", lambda s: int((s == "예").sum())),
                기다림필요_지역수=("기다림필요", lambda s: int((s == "예").sum())),
                평균전세가율=("recent_jeonse_ratio_3m", "mean"),
                평균월세비중=("recent_wolse_share_3m", "mean"),
            )
        )
        out["평균전세가율"] = out["평균전세가율"].round(2)
        out["평균월세비중"] = out["평균월세비중"].round(2)
        return out

    return summarize(["권역"]), summarize(["권역", "시도"])


def build_policy_summaries(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    def summarize(group_cols: list[str]) -> pd.DataFrame:
        pivot = (
            df.pivot_table(index=group_cols, columns="정책후판정", values="시군구", aggfunc="count", fill_value=0)
            .reset_index()
        )
        for col in ["우선 회복 후보", "정책 민감 관찰 후보", "정책 해석 주의", "정책 리스크 경계", "정책 중립"]:
            if col not in pivot.columns:
                pivot[col] = 0
        return pivot

    return summarize(["권역"]), summarize(["권역", "시도"])


def markdown_table(df: pd.DataFrame) -> str:
    safe = df.copy()
    for col in safe.columns:
        safe[col] = safe[col].apply(lambda x: "" if pd.isna(x) else str(x).replace("|", "/"))
    header = "| " + " | ".join(safe.columns) + " |"
    divider = "| " + " | ".join(["---"] * len(safe.columns)) + " |"
    rows = ["| " + " | ".join(map(str, row)) + " |" for row in safe.to_numpy().tolist()]
    return "\n".join([header, divider] + rows)


def write_appendix(title: str, description: str, summary_df: pd.DataFrame, full_df: pd.DataFrame, out_path: Path) -> None:
    lines = [
        f"# {title}",
        "",
        description,
        "",
        "## 요약",
        "",
        markdown_table(summary_df),
        "",
        "## 전국 전수표",
        "",
        markdown_table(full_df),
        "",
    ]
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    LEASE_DIR.mkdir(parents=True, exist_ok=True)
    POLICY_DIR.mkdir(parents=True, exist_ok=True)
    sale = pd.read_csv(SALE_PATH, encoding="utf-8-sig")
    lease = pd.read_csv(LEASE_PATH, encoding="utf-8-sig")
    policy = pd.read_csv(POLICY_PATH, encoding="utf-8-sig")

    for frame in [sale, lease, policy]:
        frame["권역"] = frame["시도"].map(region_group)

    lease_region, lease_sido = build_lease_summaries(lease)
    policy_region, policy_sido = build_policy_summaries(policy)

    lease_region.to_csv(LEASE_REGION_SUMMARY_OUT, index=False, encoding="utf-8-sig")
    lease_sido.to_csv(LEASE_SIDO_SUMMARY_OUT, index=False, encoding="utf-8-sig")
    policy_region.to_csv(POLICY_REGION_SUMMARY_OUT, index=False, encoding="utf-8-sig")
    policy_sido.to_csv(POLICY_SIDO_SUMMARY_OUT, index=False, encoding="utf-8-sig")

    sale_summary = (
        sale.groupby(["권역", "심화행동라벨"], as_index=False)
        .agg(
            지역수=("시군구", "count"),
            평균상승확률점수=("상승확률점수", "mean"),
            평균투자적합성점수=("투자적합성점수", "mean"),
        )
    )
    sale_summary["평균상승확률점수"] = sale_summary["평균상승확률점수"].round(2)
    sale_summary["평균투자적합성점수"] = sale_summary["평균투자적합성점수"].round(2)

    sale_full = sale[
        [
            "권역",
            "시도",
            "시군구",
            "심화행동라벨",
            "예측분류",
            "투자검토분류",
            "상승확률점수",
            "투자적합성점수",
            "trade_recovery_pct",
            "jeonse_ratio_pct",
            "기존세대수대비_향후18개월입주예정물량_pct",
        ]
    ].copy()
    for col in ["상승확률점수", "투자적합성점수", "trade_recovery_pct", "jeonse_ratio_pct", "기존세대수대비_향후18개월입주예정물량_pct"]:
        sale_full[col] = sale_full[col].round(2)

    lease_full = lease[
        [
            "권역",
            "시도",
            "시군구",
            "심화행동라벨",
            "recent_jeonse_ratio_3m",
            "recent_wolse_share_3m",
            "lead_lag_months",
            "임차수요기반양호",
            "월세비중높음",
            "임차선행",
            "기다림필요",
        ]
    ].copy()
    for col in ["recent_jeonse_ratio_3m", "recent_wolse_share_3m", "lead_lag_months"]:
        lease_full[col] = lease_full[col].round(2)

    policy_full = policy[
        [
            "권역",
            "시도",
            "시군구",
            "정책후판정",
            "정책세부유형",
            "심화행동라벨",
            "토허활성여부",
            "completed_unsold_ratio_pct",
            "월세비중높음",
            "임차선행",
            "정책판정메모",
        ]
    ].copy()
    policy_full["completed_unsold_ratio_pct"] = policy_full["completed_unsold_ratio_pct"].round(3)

    write_appendix(
        title="전국 매매 시장 분석 전국 커버 부록",
        description="이 부록은 전국 전체 시군구를 `심화행동라벨` 기준으로 한 번 이상 커버하기 위한 전수표다.",
        summary_df=sale_summary,
        full_df=sale_full,
        out_path=SALE_APPENDIX_OUT,
    )
    write_appendix(
        title="전국 임차 시장 분석 전국 커버 부록",
        description="이 부록은 전국 전체 시군구를 `임차 구조`와 `행동 라벨 검증` 기준으로 한 번 이상 커버하기 위한 전수표다.",
        summary_df=lease_region,
        full_df=lease_full,
        out_path=LEASE_APPENDIX_OUT,
    )
    write_appendix(
        title="전국 정책 영향 분석 전국 커버 부록",
        description="이 부록은 전국 전체 시군구를 `정책 후 판정` 기준으로 한 번 이상 커버하기 위한 전수표다.",
        summary_df=policy_region,
        full_df=policy_full,
        out_path=POLICY_APPENDIX_OUT,
    )

    for path in [
        LEASE_REGION_SUMMARY_OUT,
        LEASE_SIDO_SUMMARY_OUT,
        POLICY_REGION_SUMMARY_OUT,
        POLICY_SIDO_SUMMARY_OUT,
        SALE_APPENDIX_OUT,
        LEASE_APPENDIX_OUT,
        POLICY_APPENDIX_OUT,
    ]:
        print(f"Wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
