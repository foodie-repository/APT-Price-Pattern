#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

import pandas as pd

from human_overrides import apply_human_overrides


ROOT = Path(__file__).resolve().parents[2]
REPORT_DIR = ROOT / "04_결과/01_리포트_codex"
PRED_DIR = REPORT_DIR / "06_예측검증"
INVEST_DIR = REPORT_DIR / "05_투자검토"
CURRENT_PATH = PRED_DIR / "06_예측점수_20260313_codex_시군구.csv"
METRICS_PATH = PRED_DIR / "06_백테스트성능_20260313_codex.csv"
CALIBRATION_PATH = PRED_DIR / "06_점수버킷보정_20260313_codex.csv"
SIMILAR_PATH = PRED_DIR / "06_유사국면_20260313_codex.csv"
GATE_PATH = INVEST_DIR / "05_투자검토대상군_20260313_codex_시군구.csv"
REPORT_PATH = PRED_DIR / "06_예측및검증_20260313_codex.md"

REVIEW_GATE_COLUMNS = [
    "투자검토분류",
    "투자검토분류근거",
    "재검토조건",
    "사람검증셋판정",
    "사람검증셋메모",
    "점수요약",
]


def drop_existing_review_gate_columns(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = []
    for col in df.columns:
        if any(
            col == base
            or col == f"{base}_x"
            or col == f"{base}_y"
            for base in REVIEW_GATE_COLUMNS
        ):
            drop_cols.append(col)
    if drop_cols:
        return df.drop(columns=drop_cols)
    return df


def df_to_code_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "```text\n(비어 있음)\n```"
    show = df.copy()
    show = normalize_display_table(show)
    return "```text\n" + show.to_string(index=False) + "\n```"


DISPLAY_COLUMN_MAP = {
    "저평가가능점수": "가격반영부족가능성점수",
    "촉매점수": "변화계기점수",
    "과열가능점수": "과열가능성점수",
    "미래입주압력_18개월_pct": "기존세대수대비_향후18개월입주예정물량_pct",
    "completed_unsold_ratio_pct": "준공후미분양비중_pct",
    "median_peer_gap_pct": "동급생활권가격괴리율_pct",
    "저평가가능비중_pct": "가격반영부족가능비중_pct",
    "recent_12m_trades": "최근12개월거래량",
    "signal_complex_count": "비교단지수",
}

DISPLAY_TEXT_REPLACEMENTS = [
    ("우선검토", "우선 매수 검토"),
    ("우선 검토", "우선 매수 검토"),
    ("임차 지지 강화", "임차 수요 기반 개선"),
    ("임차 지지와", "임차 수요 기반과"),
    ("임차 지지", "임차 수요 기반"),
    ("전세지지", "전세가율"),
    ("공급 압박", "공급 부담"),
]


def normalize_display_text(value: object) -> object:
    if not isinstance(value, str):
        return value
    text = value
    for before, after in DISPLAY_TEXT_REPLACEMENTS:
        text = text.replace(before, after)
    return text


def normalize_display_table(df: pd.DataFrame) -> pd.DataFrame:
    show = df.rename(columns=DISPLAY_COLUMN_MAP).copy()
    object_cols = show.select_dtypes(include=["object", "string"]).columns
    for col in object_cols:
        show[col] = show[col].map(normalize_display_text)
    return show


def build_candidate_reason(row: pd.Series, kind: str) -> str:
    reasons: list[str] = []

    if kind == "rising":
        if pd.notna(row.get("trade_recovery_pct")) and row["trade_recovery_pct"] >= 20:
            reasons.append(f"거래회복 {row['trade_recovery_pct']:.1f}%")
        if pd.notna(row.get("jeonse_ratio_pct")) and row["jeonse_ratio_pct"] >= 80:
            reasons.append(f"전세가율 {row['jeonse_ratio_pct']:.1f}%")
        if pd.notna(row.get("미래입주압력_18개월_pct")) and row["미래입주압력_18개월_pct"] <= 2:
            reasons.append(f"입주예정 물량 비율 낮음 {row['미래입주압력_18개월_pct']:.1f}%")
        if pd.notna(row.get("저평가가능점수")) and row["저평가가능점수"] >= 65:
            reasons.append("가격 반영 부족 신호")
        if pd.notna(row.get("과열가능점수")) and row["과열가능점수"] < 40:
            reasons.append("과열 낮음")
    elif kind == "undervalued":
        if pd.notna(row.get("저평가가능비중_pct")) and row["저평가가능비중_pct"] > 0:
            reasons.append(f"저평가가능 비중 {row['저평가가능비중_pct']:.1f}%")
        if pd.notna(row.get("jeonse_ratio_pct")) and row["jeonse_ratio_pct"] >= 80:
            reasons.append(f"전세가율 높음 {row['jeonse_ratio_pct']:.1f}%")
        if pd.notna(row.get("촉매점수")) and row["촉매점수"] >= 70:
            reasons.append("변화 계기 양호")
        if pd.notna(row.get("미래입주압력_18개월_pct")) and row["미래입주압력_18개월_pct"] <= 2:
            reasons.append("공급 부담 낮음")
    elif kind == "observe":
        if pd.notna(row.get("투자적합성점수")) and row["투자적합성점수"] >= 80:
            reasons.append(f"투자적합성 높음 {row['투자적합성점수']:.1f}")
        if pd.notna(row.get("촉매점수")) and row["촉매점수"] >= 70:
            reasons.append(f"변화 계기 강함 {row['촉매점수']:.1f}")
        if pd.notna(row.get("과열가능점수")) and row["과열가능점수"] >= 65:
            reasons.append(f"과열 경계 {row['과열가능점수']:.1f}")
        if pd.notna(row.get("상승확률점수")) and row["상승확률점수"] < 55:
            reasons.append(f"방향성 추가 확인 {row['상승확률점수']:.1f}")
    elif kind == "avoid":
        if pd.notna(row.get("과열가능점수")) and row["과열가능점수"] >= 70:
            reasons.append(f"과열 {row['과열가능점수']:.1f}")
        if pd.notna(row.get("미래입주압력_18개월_pct")) and row["미래입주압력_18개월_pct"] >= 5:
            reasons.append(f"입주예정 물량 비율 {row['미래입주압력_18개월_pct']:.1f}%")
        if pd.notna(row.get("completed_unsold_ratio_pct")) and row["completed_unsold_ratio_pct"] >= 60:
            reasons.append(f"준공후미분양비중 {row['completed_unsold_ratio_pct']:.1f}%")
        if pd.notna(row.get("jeonse_ratio_pct")) and row["jeonse_ratio_pct"] < 70:
            reasons.append(f"전세가율 낮음 {row['jeonse_ratio_pct']:.1f}%")

    if not reasons:
        if kind == "observe":
            reasons.append("지금은 관찰이 우선")
        elif kind == "avoid":
            reasons.append("보수 접근이 적절")
        else:
            reasons.append("복합 점수 상위")

    return ", ".join(reasons[:3])


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    current = pd.read_csv(CURRENT_PATH)
    gate = pd.read_csv(GATE_PATH)
    metrics = pd.read_csv(METRICS_PATH)
    calibration = pd.read_csv(CALIBRATION_PATH)
    similar = pd.read_csv(SIMILAR_PATH)

    gate_cols = ["시도", "시군구", *REVIEW_GATE_COLUMNS]
    gate = gate[gate_cols].drop_duplicates(["시도", "시군구"])

    current = drop_existing_review_gate_columns(current)
    current = current.merge(gate, on=["시도", "시군구"], how="left")
    current = apply_human_overrides(current, scope="prediction")
    return current, metrics, calibration, similar


def render_report(current: pd.DataFrame, metrics: pd.DataFrame, calibration: pd.DataFrame, similar: pd.DataFrame) -> str:
    priority = current[current["투자검토분류"] == "우선 검토"].copy()
    hold = current[current["투자검토분류"] == "판단 보류"].copy()
    excluded = current[current["투자검토분류"] == "즉시 제외"].copy()

    rising = (
        priority[(priority["예측분류"] == "상승") & (priority["과열가능점수"] < 70)]
        .sort_values(["상승확률점수", "투자적합성점수", "촉매점수"], ascending=[False, False, False])
        .head(15)
        .copy()
    )
    rising["판단근거"] = rising.apply(lambda row: build_candidate_reason(row, "rising"), axis=1)

    undervalued = (
        priority.sort_values(
            ["저평가가능점수", "투자적합성점수", "촉매점수", "상승확률점수"],
            ascending=[False, False, False, False],
        )
        .head(15)
        .copy()
    )
    undervalued["판단근거"] = undervalued.apply(lambda row: build_candidate_reason(row, "undervalued"), axis=1)

    observe = (
        hold.sort_values(["투자적합성점수", "촉매점수", "상승확률점수"], ascending=[False, False, False])
        .head(15)
        .copy()
    )
    observe["판단근거"] = observe.apply(lambda row: build_candidate_reason(row, "observe"), axis=1)

    avoid = (
        hold.sort_values(
            ["과열가능점수", "미래입주압력_18개월_pct", "completed_unsold_ratio_pct", "상승확률점수"],
            ascending=[False, False, False, True],
        )
        .head(15)
        .copy()
    )
    avoid["판단근거"] = avoid.apply(lambda row: build_candidate_reason(row, "avoid"), axis=1)

    excluded_watch = excluded.sort_values(["상승확률점수", "투자적합성점수"], ascending=[False, True]).head(20).copy()

    plain_but_investable = (
        hold[hold["투자적합성점수"] >= 80]
        .sort_values(["투자적합성점수", "상승확률점수"], ascending=[False, False])
        .head(10)
        .copy()
    )
    strong_but_excluded = (
        excluded[excluded["상승확률점수"] >= 60]
        .sort_values(["상승확률점수", "투자적합성점수"], ascending=[False, True])
        .head(10)
        .copy()
    )

    similar_lines = "\n".join(
        f"- `{int(row.ym)}` (거리 `{row.distance:.2f}`)" for row in similar.itertuples(index=False)
    )

    metrics_text = df_to_code_table(metrics.round(4))
    bucket_text = df_to_code_table(calibration.sort_values("평균점수", ascending=False).head(3).round(4))
    rising_text = df_to_code_table(
        rising[
            ["시도", "시군구", "상승확률점수", "투자적합성점수", "저평가가능점수", "촉매점수", "과열가능점수", "판단근거", "투자검토분류근거"]
        ].round(2)
    )
    undervalued_text = df_to_code_table(
        undervalued[
            ["시도", "시군구", "저평가가능점수", "투자적합성점수", "상승확률점수", "촉매점수", "판단근거", "투자검토분류근거"]
        ].round(2)
    )
    observe_text = df_to_code_table(
        observe[
            ["시도", "시군구", "상승확률점수", "투자적합성점수", "촉매점수", "과열가능점수", "판단근거", "재검토조건"]
        ].round(2)
    )
    avoid_text = df_to_code_table(
        avoid[
            ["시도", "시군구", "투자적합성점수", "과열가능점수", "상승확률점수", "미래입주압력_18개월_pct", "판단근거", "재검토조건"]
        ].round(2)
    )
    excluded_text = df_to_code_table(
        excluded_watch[
            ["시도", "시군구", "상승확률점수", "투자적합성점수", "제외사유", "재검토조건"]
        ].round(2)
    )
    plain_text = df_to_code_table(
        plain_but_investable[
            ["시도", "시군구", "상승확률점수", "투자적합성점수", "투자검토분류근거", "재검토조건"]
        ].round(2)
    )
    strong_excluded_text = df_to_code_table(
        strong_but_excluded[
            ["시도", "시군구", "상승확률점수", "투자적합성점수", "제외사유", "재검토조건"]
        ].round(2)
    )

    return f"""# 예측 및 검증 (2026-03-13, codex)

## 0. 문서 성격

- 이 문서는 `3. 매매`, `4. 임차`, `5. 정책`, `6. 공급 프록시`, `7. 투자 검토 대상군 및 투자 적합성 보정` 결과를 합쳐 시군구 기준 `향후 6개월 대표가격 방향성`을 다시 좁혀보는 단계다.
- 이번 버전의 핵심은 방향성 점수만으로 후보를 뽑지 않고, 단계 7의 `우선 검토 / 판단 보류 / 즉시 제외` 게이트를 먼저 적용한 뒤 예측 결과를 읽는 것이다.
- 공개 보고서에서는 단계 7의 `우선 검토`를 `우선 매수 검토 후보군`으로 번역해 읽는다.
- 따라서 이 문서는 단순 점수표가 아니라 `실제 투자 검토 가능성까지 반영한 shortlist 문서`다.

## 1. 예측 기준

- 예측 대상: 시군구 단위 `전용면적_구분='중소형'` 대표평당가의 향후 6개월 방향성
- 기본 라벨:
  - `향후 6개월 가격변화율 >= +3.0%`: 상승
  - `-3.0% < 향후 6개월 가격변화율 < +3.0%`: 보합
  - `<= -3.0%`: 하락
- 현재 기준월: `2026-02`
- 예측 분류 기준:
  - `상승확률 점수 >= 55`: 상승
  - `45 < 상승확률 점수 < 55`: 보합
  - `<= 45`: 하락
- 후보 제시 원칙:
  - `우선 검토`: 실제 shortlist 후보
  - `판단 보류`: 좋은 지역일 수 있으나 과열, 방향성 부족, 촉매 확인 필요
  - `즉시 제외`: 지금 버전에서는 투자 검토 대상으로 올리지 않음

## 2. 과거 유사 패턴 근거

현재 `2026-02`와 가장 가까운 월별 권역 구조는 아래와 같았다.
{similar_lines}

- 해석:
  - 현재는 `수도권 상대 강세`, `지방 비핵심 공급 부담`, `임차 수요 기반의 지역 차별화`가 같이 나타나는 구간이다.
  - 단순 전면 상승장이 아니라 `확산과 선별이 동시에 진행되는 회복/확산 혼합 구간`으로 보는 편이 맞다.

## 3. 백테스트 방식

- origin 구간: `2011-01 ~ 2025-08`
- 최근 홀드아웃: `2024-01 ~ 2025-08`
- 타깃 라벨: origin `t`에서 `t+6` 가격변화율
- 평가 지표:
  - 3분류 정확도
  - Macro F1
  - `상승` 정밀도 / 재현율
  - Top 20 후보 적중률
  - Bottom 20 회피 적중률

## 4. 성능 요약

{metrics_text}

상위 점수 버킷의 실제 성과는 아래처럼 나왔다.

{bucket_text}

- 해석:
  - 점수 상위 버킷일수록 실제 `상승` 비율과 평균 6개월 변화율이 올라가는 구조는 유지된다.
  - 다만 이 점수만으로 바로 투자 후보를 고르지 않고, 단계 7 게이트를 선행 적용한다.

## 5. 상승 후보 / 저평가 가능 후보 / 관찰 후보 / 보수·회피 후보 / 제외 대상

### 5.1 상승 후보

- 아래 표는 단계 7 내부 라벨인 `우선 검토`로 분류된 지역 중, 공개 보고서 기준 `우선 매수 검토`로 먼저 볼 만한 지역만 포함한다.

{rising_text}

### 5.2 저평가 가능 후보

- `저평가`는 단순 저가가 아니라, `우선 매수 검토 후보군`으로 좁힌 지역 중 상대적으로 덜 반영된 후보를 뜻한다.

{undervalued_text}

### 5.3 관찰 후보

- 관찰 후보는 `판단 보류` 군 중, 질은 좋지만 지금 당장 매수 우선순위를 높이기보다 추가 확인이 필요한 지역이다.

{observe_text}

### 5.4 보수·회피 후보

- 보수·회피 후보는 `판단 보류` 군 안에서도 특히 과열, 공급 부담, 임차 수요 기반 약화가 커서 더 보수적으로 봐야 하는 지역이다.

{avoid_text}

### 5.5 제외 대상

- 아래 지역은 단계 7 게이트에서 `즉시 제외`로 분류된 곳이다.

{excluded_text}

## 6. 각 후보 및 제외 대상의 판단 근거 또는 제외 사유

### 6.1 방향성은 강하지만 투자 검토 대상에서 제외된 지역

{strong_excluded_text}

- 해석:
  - 이런 지역은 `반등 가능성` 일부는 읽혀도, 현재 단계에서는 `유동성`, `시장 규모`, `비교 가능성`, `출구 전략`이 약하다고 본다.
  - 즉 시장 참고용으로는 보되, 실제 투자 shortlist에는 올리지 않는다.

### 6.2 방향성은 평범하지만 투자 검토 가치가 높은 지역

{plain_text}

- 해석:
  - 이런 지역은 `좋은 지역인데 아직 방향성이 확실하지 않거나`, `과열 때문에 당장 추격 매수는 부담스러운` 경우다.
  - 실제 투자 관점에서는 오히려 이런 지역이 더 중요할 수 있으므로 `판단 보류`로 남기고 재검토 조건을 붙인다.

## 7. 시나리오별 대응 원칙

### 상승 시나리오

- 촉발 조건:
  - 수도권 핵심지와 대체 핵심지의 거래 회복 지속
  - 전세가율 또는 전세 보증금 회복 유지
  - `준공후 미분양` 완만한 하향
  - 예정 입주 부담이 높은 지역에서도 실제 소화 확인
- 무효화 조건:
  - 상위 후보에서 거래 급감과 전세 약세가 동시에 발생
  - 고부담 지역에서 입주 직전 호가 급락과 가격기준 급매 증가
- 투자자 대응:
  - `우선 매수 검토` 후보에서 생활권과 단지 상품성까지 내려가 shortlist를 더 좁힌다.

### 보합 시나리오

- 촉발 조건:
  - 대출 규제와 공급 부담이 상쇄되어 방향성 없이 선별만 진행
  - 임차는 버티지만 매매 추격 수요가 약한 상태 지속
- 무효화 조건:
  - 특정 권역에서 전세와 거래가 동시에 강해져 확산이 빨라짐
  - 또는 준공후 미분양과 예정 입주 부담이 겹쳐 하방 압력이 커짐
- 투자자 대응:
  - `판단 보류` 후보 위주로 관찰하고, 무리한 추격보다 가격 조정·거래 회복 확인을 기다린다.

### 하락 시나리오

- 촉발 조건:
  - 외곽·비핵심지에서 입주 집중, 임차 약세, 준공후 미분양 누적이 결합
  - 현재 과열 후보에서 실제 할인 거래 확산
- 무효화 조건:
  - 공급 부담이 높아도 임차 흡수와 거래 회복이 확인되는 경우
- 투자자 대응:
  - `즉시 제외`와 `보수·회피` 후보를 더욱 보수적으로 보고, 현금 보존과 관찰 중심으로 대응한다.

## 8. 신뢰도와 해석 한계

- 이번 산출물은 기존 정량 점수에 단계 7 게이트를 결합한 결과다.
- 즉 `06_예측점수...csv`의 방향성 자체는 기존과 같고, 투자자 관점 후보 분류는 `05_투자검토대상군...csv`를 선행 적용했다.
- 로컬 실행 환경에는 `duckdb` 패키지가 없어 이번 재실행은 DB 재계산이 아니라 `기존 정량 산출물 + 단계 7 게이트` 결합 방식으로 처리했다.
- 따라서 점수 재산출이 아니라 후보 해석과 보고서 정렬에 초점이 있다.
- 다음에 환경이 정리되면 `07_prediction_validation.py`를 DB까지 포함해 다시 돌려 같은 구조를 재계산하는 것이 맞다.

## 9. 생성/수정한 파일

- `06_예측점수_20260313_codex_시군구.csv`
- `06_예측및검증_20260313_codex.md`
- `03_분석/01_스크립트/07_prediction_validation.py`
- `03_분석/01_스크립트/refresh_prediction_outputs_with_gate.py`
"""


def main() -> None:
    current, metrics, calibration, similar = load_data()
    current.to_csv(CURRENT_PATH, index=False, encoding="utf-8-sig")
    report = render_report(current, metrics, calibration, similar)
    REPORT_PATH.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
