#!/usr/bin/env python3
from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = ROOT / "04_결과/01_리포트_codex/06_예측점수_20260313_codex_시군구.csv"
OUTPUT_CSV = ROOT / "04_결과/01_리포트_codex/05_투자검토대상군_20260313_codex_시군구.csv"
VALIDATION_CSV = ROOT / "02_데이터/02_참조/투자검토대상군_검증셋_20260313.csv"

GATE_FIELDS = [
    "투자검토분류",
    "투자검토분류근거",
    "재검토조건",
    "사람검증셋판정",
    "사람검증셋메모",
    "점수요약",
]


@dataclass
class ValidationEntry:
    verdict: str
    priority: str
    basis: str
    memo: str
    review_date: str


def load_validation_map(path: Path) -> dict[tuple[str, str], ValidationEntry]:
    if not path.exists():
        return {}

    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        result: dict[tuple[str, str], ValidationEntry] = {}
        for row in reader:
            key = (row.get("시도", "").strip(), row.get("시군구", "").strip())
            if not key[0] or not key[1]:
                continue
            result[key] = ValidationEntry(
                verdict=row.get("판정", "").strip(),
                priority=row.get("우선순위", "").strip(),
                basis=row.get("근거유형", "").strip(),
                memo=row.get("메모", "").strip(),
                review_date=row.get("검토일", "").strip(),
            )
        return result


def to_float(row: dict[str, str], key: str) -> float:
    value = row.get(key, "").strip()
    if value == "":
        return 0.0
    return float(value)


def classify(row: dict[str, str], validation: ValidationEntry | None) -> tuple[str, str, str]:
    up = to_float(row, "상승확률점수")
    fit = to_float(row, "투자적합성점수")
    underv = to_float(row, "저평가가능점수")
    catalyst = to_float(row, "촉매점수")
    heat = to_float(row, "과열가능점수")
    invest_ok = row.get("투자검토가능") == "예"
    low_conf = row.get("신뢰도낮음") == "예"
    exclude_reason = row.get("제외사유", "").strip()

    if validation and validation.verdict == "제외우선":
        return (
            "즉시 제외",
            f"사람 검증셋에서 제외우선으로 표시됨. {validation.memo}".strip(),
            "거래 회복, 임차 지지, 공급 압박 완화가 실제로 확인되면 재검토",
        )
    if validation and validation.verdict == "우선검토":
        return (
            "우선 검토",
            f"사람 검증셋에서 우선검토로 표시됨. {validation.memo}".strip(),
            "실거래 흐름과 실제 매수 가능 단지를 함께 점검",
        )
    if validation and validation.verdict == "판단보류":
        return (
            "판단 보류",
            f"사람 검증셋에서 판단보류로 표시됨. {validation.memo}".strip(),
            "추가 데이터와 현재 시장 체감 지표를 확인한 뒤 재판정",
        )

    if not invest_ok:
        reason = exclude_reason or "투자 검토 대상군 게이트를 통과하지 못함"
        if low_conf:
            revisit = "거래량과 비교 단지 표본이 쌓이면 재검토"
        else:
            revisit = "임차 지지와 공급 압박 완화가 확인되면 재검토"
        return ("즉시 제외", reason, revisit)

    if low_conf and fit < 55:
        return (
            "즉시 제외",
            f"신뢰도 낮음 구간이며 투자적합성점수도 낮음({fit:.1f})",
            "표본이 보강되고 비교 단지 구성이 안정되면 재검토",
        )

    if (up >= 60 and fit >= 55 and heat < 65) or (underv >= 60 and fit >= 55 and heat < 70 and up >= 45):
        if up >= 60:
            reason = (
                f"상승확률({up:.1f})과 투자적합성({fit:.1f})이 모두 높고 "
                f"과열 가능성({heat:.1f})이 과하지 않음"
            )
        else:
            reason = (
                f"저평가 가능성({underv:.1f})과 투자적합성({fit:.1f})이 높고 "
                f"방향성도 급락 구간이 아님"
            )
        revisit = "단지 단계에서 상품성·호가·매수 실행 가능성을 추가 점검"
        return ("우선 검토", reason, revisit)

    if fit >= 65:
        if heat >= 65:
            reason = (
                f"투자적합성({fit:.1f})은 높지만 과열 가능성({heat:.1f})이 커서 "
                "지금은 추격보다 관찰이 적절함"
            )
        elif up < 55:
            reason = (
                f"투자적합성({fit:.1f})은 높지만 상승확률({up:.1f})이 아직 강하지 않아 "
                "방향성 확인이 더 필요함"
            )
        else:
            reason = (
                f"투자적합성({fit:.1f})은 높지만 우선 검토 기준에는 조금 못 미쳐 "
                "한 단계 낮은 검토군으로 둠"
            )
        revisit = "가격 조정, 거래 회복, 임차 지지 강화가 보이면 상향 검토"
        return ("판단 보류", reason, revisit)

    if catalyst >= 70 and heat < 70:
        return (
            "판단 보류",
            f"촉매 점수({catalyst:.1f})는 높지만 투자적합성({fit:.1f}) 또는 방향성({up:.1f})이 아직 부족함",
            "촉발 이벤트가 실제 거래와 가격에 반영되는지 확인 후 재검토",
        )

    return (
        "판단 보류",
        f"방향성({up:.1f}) 또는 투자적합성({fit:.1f})이 우선 검토 기준에는 부족하지만 즉시 제외 수준은 아님",
        "다음 1~2개 분기 거래·임차·공급 변화를 확인 후 재판정",
    )


def build_highlight(row: dict[str, str]) -> str:
    return (
        f"상승확률 {to_float(row, '상승확률점수'):.1f}, "
        f"투자적합성 {to_float(row, '투자적합성점수'):.1f}, "
        f"저평가 {to_float(row, '저평가가능점수'):.1f}, "
        f"촉매 {to_float(row, '촉매점수'):.1f}, "
        f"과열 {to_float(row, '과열가능점수'):.1f}"
    )


def strip_existing_gate_fields(row: dict[str, str]) -> dict[str, str]:
    cleaned: dict[str, str] = {}
    for key, value in row.items():
        if any(key == field or key == f"{field}_x" or key == f"{field}_y" for field in GATE_FIELDS):
            continue
        cleaned[key] = value
    return cleaned


def main() -> None:
    validation_map = load_validation_map(VALIDATION_CSV)

    with INPUT_CSV.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        input_fields = reader.fieldnames or []

    input_fields = [
        field
        for field in input_fields
        if not any(field == gate or field == f"{gate}_x" or field == f"{gate}_y" for gate in GATE_FIELDS)
    ]
    extra_fields = list(GATE_FIELDS)

    output_rows: list[dict[str, str]] = []
    for row in rows:
        key = (row.get("시도", "").strip(), row.get("시군구", "").strip())
        validation = validation_map.get(key)
        tier, reason, revisit = classify(row, validation)

        new_row = strip_existing_gate_fields(row)
        new_row["투자검토분류"] = tier
        new_row["투자검토분류근거"] = reason
        new_row["재검토조건"] = revisit
        new_row["사람검증셋판정"] = validation.verdict if validation else ""
        new_row["사람검증셋메모"] = validation.memo if validation else ""
        new_row["점수요약"] = build_highlight(row)
        output_rows.append(new_row)

    priority_order = {"우선 검토": 0, "판단 보류": 1, "즉시 제외": 2}
    output_rows.sort(
        key=lambda row: (
            priority_order.get(row["투자검토분류"], 9),
            -to_float(row, "상승확률점수"),
            -to_float(row, "투자적합성점수"),
        )
    )

    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=input_fields + extra_fields)
        writer.writeheader()
        writer.writerows(output_rows)


if __name__ == "__main__":
    main()
