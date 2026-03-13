-- Step 2 preprocessing and analysis-view draft for apartment.duckdb
-- Scope:
--   - checklist_execution.md sections 3, 4, 5
--   - no market pattern analysis, no report writing
--
-- Assumptions already fixed by user:
--   - 공동주택_전국 is the national master table
--   - 공동주택_전국 uses 단지구분코드 = 1 only
--   - 공동주택_기본정보 is Seoul-only and excluded from nationwide core analysis
--   - 매매 excludes 거래유형='직거래'
--   - 매매 excludes rows where 해제사유발생일 != '-'
--   - 거래유형='-' and 계약년월 <= 202110 is a "brokered_but_unverifiable" period
--   - 전월세 base analysis uses 전월세구분 IN ('전세', '월세')
--   - 전월세구분='월세 전세금 변환' is excluded from base analysis
--   - complex join key uses 지번주소 + 단지명_공백제거
--   - automatic join is allowed only for unique matches
--   - monthly sample size < 10 implies quarter-level aggregation is recommended
--
-- Notes:
--   - This file is designed to be executed against apartment.duckdb.
--   - 정책_이벤트 table is currently empty, so v_policy_event_ref remains empty
--     until policy rows are loaded from the manual.

CREATE OR REPLACE VIEW v_params AS
SELECT
    GREATEST(
        (SELECT MAX(계약년월) FROM 매매),
        (SELECT MAX(계약년월) FROM 전월세)
    ) AS 최신계약년월,
    CAST(FLOOR(
        GREATEST(
            (SELECT MAX(계약년월) FROM 매매),
            (SELECT MAX(계약년월) FROM 전월세)
        ) / 100
    ) AS INTEGER) AS 기준연도,
    CAST(
        STRFTIME(
            TRY_STRPTIME(
                CAST(
                    GREATEST(
                        (SELECT MAX(계약년월) FROM 매매),
                        (SELECT MAX(계약년월) FROM 전월세)
                    ) AS VARCHAR
                ) || '01',
                '%Y%m%d'
            ),
            '%Y-%m'
        ) AS VARCHAR
    ) AS 최신계약년월_문자열;

CREATE OR REPLACE VIEW v_policy_event_ref AS
SELECT
    id,
    일자,
    정책_유형,
    규제_방향,
    규제_강도,
    주체,
    핵심_내용,
    영향_지역,
    시행일,
    주요_변경사항,
    검색쿼리,
    수집방법,
    수집일시,
    원본_검색결과
FROM 정책_이벤트;

CREATE OR REPLACE VIEW v_transport_axis_ref AS
SELECT
    교통축,
    노선기반,
    순서,
    시도,
    시군구,
    개통년월,
    CASE
        WHEN NULLIF(TRIM(개통년월), '') IS NOT NULL
        THEN CAST(TRY_STRPTIME(개통년월 || '-01', '%Y-%m-%d') AS DATE)
        ELSE NULL
    END AS 개통일자,
    비고,
    CASE
        WHEN NULLIF(TRIM(개통년월), '') IS NOT NULL THEN '개통'
        ELSE '미개통/행정보조'
    END AS 교통축상태
FROM read_csv_auto(
    '/Users/foodie/myproject/Real-Estate-Investment/APT-Price-Pattern/02_데이터/02_참조/교통축_인접_매핑.csv',
    header = true
);

CREATE OR REPLACE VIEW v_complex_master_base AS
WITH base AS (
    SELECT
        단지고유번호,
        필지고유번호,
        TRIM(주소) AS 주소,
        TRIM(주소) AS 지번주소,
        REGEXP_REPLACE(주소, ' [0-9].*$', '', 'g') AS 주소_행정,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(COALESCE(단지명, ''), '\\([^)]*\\)', '', 'g'),
                '\\s+',
                ' ',
                'g'
            )
        ) AS 조인_단지명,
        단지구분코드,
        동수,
        세대수,
        사용승인일,
        CAST(TRY_STRPTIME(NULLIF(TRIM(사용승인일), ''), '%Y%m%d') AS DATE) AS 사용승인일자,
        CASE
            WHEN NULLIF(TRIM(사용승인일), '') IS NOT NULL
            THEN CAST(STRFTIME(TRY_STRPTIME(사용승인일, '%Y%m%d'), '%Y%m') AS BIGINT)
            ELSE NULL
        END AS 사용승인년월,
        수집일자
    FROM 공동주택_전국
    WHERE 단지구분코드 = '1'
)
SELECT
    단지고유번호,
    필지고유번호,
    주소,
    지번주소,
    주소_행정,
    SPLIT_PART(주소_행정, ' ', 1) AS 시도,
    CASE
        WHEN SPLIT_PART(주소_행정, ' ', 1) = '세종특별자치시' THEN '세종시'
        WHEN SPLIT_PART(주소_행정, ' ', 4) <> '' THEN SPLIT_PART(주소_행정, ' ', 2) || ' ' || SPLIT_PART(주소_행정, ' ', 3)
        ELSE SPLIT_PART(주소_행정, ' ', 2)
    END AS 시군구,
    CASE
        WHEN SPLIT_PART(주소_행정, ' ', 1) = '세종특별자치시' THEN SPLIT_PART(주소_행정, ' ', 2)
        WHEN SPLIT_PART(주소_행정, ' ', 4) <> '' THEN SPLIT_PART(주소_행정, ' ', 4)
        ELSE SPLIT_PART(주소_행정, ' ', 3)
    END AS 읍면동,
    정규화_단지명,
    단지명_공백제거,
    CASE
        WHEN NULLIF(조인_단지명, '') IS NOT NULL THEN 조인_단지명
        ELSE 정규화_단지명
    END AS 조인_단지명,
    단지구분코드,
    동수,
    세대수,
    사용승인일,
    사용승인일자,
    사용승인년월,
    수집일자
FROM base;

CREATE OR REPLACE VIEW v_complex_master_seoul_supplement AS
WITH info_base AS (
    SELECT
        단지코드,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(COALESCE(단지명, ''), '\\([^)]*\\)', '', 'g'),
                '\\s+',
                ' ',
                'g'
            )
        ) AS 조인_단지명,
        REGEXP_REPLACE(COALESCE(법정동주소, ''), ' [0-9].*$', '', 'g') AS 법정동주소_행정,
        법정동주소,
        도로명주소,
        주택유형,
        세대수,
        사용승인일,
        CAST(TRY_STRPTIME(NULLIF(TRIM(사용승인일), ''), '%Y%m%d') AS DATE) AS 사용승인일자,
        시공사,
        시행사,
        지하철노선,
        지하철역,
        지하철역거리,
        버스정류장거리,
        교육시설,
        수집일자
    FROM 공동주택_기본정보
    WHERE SPLIT_PART(TRIM(COALESCE(법정동주소, 도로명주소, '')), ' ', 1) = '서울특별시'
),
info_ranked AS (
    SELECT
        *,
        SPLIT_PART(법정동주소_행정, ' ', 1) AS 시도,
        CASE
            WHEN SPLIT_PART(법정동주소_행정, ' ', 1) = '세종특별자치시' THEN '세종시'
            WHEN SPLIT_PART(법정동주소_행정, ' ', 4) <> '' THEN SPLIT_PART(법정동주소_행정, ' ', 2) || ' ' || SPLIT_PART(법정동주소_행정, ' ', 3)
            ELSE SPLIT_PART(법정동주소_행정, ' ', 2)
        END AS 시군구,
        CASE
            WHEN SPLIT_PART(법정동주소_행정, ' ', 1) = '세종특별자치시' THEN SPLIT_PART(법정동주소_행정, ' ', 2)
            WHEN SPLIT_PART(법정동주소_행정, ' ', 4) <> '' THEN SPLIT_PART(법정동주소_행정, ' ', 4)
            ELSE SPLIT_PART(법정동주소_행정, ' ', 3)
        END AS 읍면동,
        ROW_NUMBER() OVER (
            PARTITION BY
                SPLIT_PART(법정동주소_행정, ' ', 1),
                CASE
                    WHEN SPLIT_PART(법정동주소_행정, ' ', 1) = '세종특별자치시' THEN '세종시'
                    WHEN SPLIT_PART(법정동주소_행정, ' ', 4) <> '' THEN SPLIT_PART(법정동주소_행정, ' ', 2) || ' ' || SPLIT_PART(법정동주소_행정, ' ', 3)
                    ELSE SPLIT_PART(법정동주소_행정, ' ', 2)
                END,
                CASE
                    WHEN SPLIT_PART(법정동주소_행정, ' ', 1) = '세종특별자치시' THEN SPLIT_PART(법정동주소_행정, ' ', 2)
                    WHEN SPLIT_PART(법정동주소_행정, ' ', 4) <> '' THEN SPLIT_PART(법정동주소_행정, ' ', 4)
                    ELSE SPLIT_PART(법정동주소_행정, ' ', 3)
                END,
                단지명_공백제거
            ORDER BY 수집일자 DESC, 세대수 DESC, 단지코드
        ) AS rn
    FROM info_base
)
SELECT
    m.*,
    i.단지코드 AS 보강_단지코드,
    i.주택유형 AS 보강_주택유형,
    i.시공사,
    i.시행사,
    i.지하철노선,
    i.지하철역,
    i.지하철역거리,
    i.버스정류장거리,
    i.교육시설,
    CASE WHEN i.단지코드 IS NOT NULL THEN 1 ELSE 0 END AS 기본정보_매칭여부
FROM v_complex_master_base m
LEFT JOIN info_ranked i
    ON m.시도 = i.시도
   AND m.시군구 = i.시군구
   AND COALESCE(m.읍면동, '') = COALESCE(i.읍면동, '')
   AND m.단지명_공백제거 = i.단지명_공백제거
   AND i.rn = 1;

CREATE OR REPLACE VIEW v_sale_prepared AS
WITH base AS (
    SELECT
        TRIM(m.시군구) AS 시군구,
        TRIM(COALESCE(m.번지, '')) AS 번지,
        m.본번,
        m.부번,
        TRIM(COALESCE(m.단지명, '')) AS 단지명,
        m."전용면적(㎡)",
        m.계약년월,
        m.계약일,
        m."거래금액(만원)",
        TRIM(COALESCE(m.동, '')) AS 동,
        m.층,
        TRIM(COALESCE(m.매수자, '')) AS 매수자,
        TRIM(COALESCE(m.매도자, '')) AS 매도자,
        m.건축년도,
        TRIM(COALESCE(m.도로명, '')) AS 도로명,
        TRIM(COALESCE(m.해제사유발생일, '')) AS 해제사유발생일,
        TRIM(COALESCE(m.거래유형, '')) AS 거래유형,
        TRIM(COALESCE(m.중개사소재지, '')) AS 중개사소재지,
        TRIM(COALESCE(m.등기일자, '')) AS 등기일자,
        TRIM(COALESCE(m.주택유형, '')) AS 주택유형,
        TRIM(COALESCE(m.시도, '')) AS 시도,
        TRIM(COALESCE(m.읍면동, '')) AS 읍면동,
        TRIM(COALESCE(m.리, '')) AS 리,
        p.최신계약년월,
        p.기준연도,
        CAST(TRY_CAST(NULLIF(REPLACE("거래금액(만원)", ',', ''), '-') AS BIGINT) AS BIGINT) AS 거래금액_만원,
        "전용면적(㎡)" AS 전용면적_㎡,
        TRIM(COALESCE(m.시군구, '')) AS 시군구_분리,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(COALESCE(단지명, ''), '\\([^)]*\\)', '', 'g'),
                '\\s+',
                ' ',
                'g'
            )
        ) AS 조인_단지명,
        CASE
            WHEN 계약년월 <= 202110 AND 거래유형 = '-' THEN '중개거래_판별불가'
            WHEN 거래유형 = '중개거래' THEN '중개거래'
            WHEN 거래유형 = '직거래' THEN '직거래'
            ELSE COALESCE(NULLIF(TRIM(거래유형), ''), '미상')
        END AS 거래유형_정규화,
        CASE
            WHEN 계약년월 <= 202110 AND 거래유형 = '-' THEN 1 ELSE 0
        END AS 직거래판별불가구간,
        CASE
            WHEN "전용면적(㎡)" <= 40 THEN '초소형'
            WHEN "전용면적(㎡)" <= 60 THEN '소형'
            WHEN "전용면적(㎡)" <= 85 THEN '중소형'
            WHEN "전용면적(㎡)" <= 135 THEN '중대형'
            ELSE '대형'
        END AS 전용면적_구분,
        "전용면적(㎡)" * 0.4 AS 추정평형,
        CASE
            WHEN "전용면적(㎡)" * 0.4 < 10 THEN '10평 미만'
            WHEN "전용면적(㎡)" * 0.4 < 20 THEN '10평대'
            WHEN "전용면적(㎡)" * 0.4 < 30 THEN '20평대'
            WHEN "전용면적(㎡)" * 0.4 < 40 THEN '30평대'
            WHEN "전용면적(㎡)" * 0.4 < 50 THEN '40평대'
            WHEN "전용면적(㎡)" * 0.4 < 60 THEN '50평대'
            ELSE '60평 이상'
        END AS 평형대_구분,
        CAST(FLOOR(계약년월 / 100) AS INTEGER) AS 계약연도,
        GREATEST(p.기준연도 - 건축년도, 1) AS 연식
    FROM 매매 m
    CROSS JOIN v_params p
    WHERE 주택유형 = '아파트'
)
SELECT
    *,
    CASE
        WHEN 번지 IS NOT NULL AND TRIM(번지) NOT IN ('', '-')
        THEN TRIM(
            CONCAT_WS(
                ' ',
                NULLIF(TRIM(시도), ''),
                NULLIF(TRIM(시군구_분리), ''),
                NULLIF(TRIM(읍면동), ''),
                NULLIF(TRIM(리), ''),
                NULLIF(TRIM(번지), '')
            )
        )
        ELSE NULL
    END AS 지번주소,
    CASE
        WHEN 연식 < 5 THEN '5년 미만'
        WHEN 연식 < 10 THEN '5~10년 미만'
        WHEN 연식 < 20 THEN '10~20년 미만'
        WHEN 연식 < 30 THEN '20~30년 미만'
        ELSE '30년 이상'
    END AS 연식_구분,
    CASE
        WHEN 거래금액_만원 IS NULL OR 거래금액_만원 <= 0 OR 전용면적_㎡ IS NULL OR 전용면적_㎡ <= 0 THEN 1
        ELSE 0
    END AS 명백오류여부,
    CASE
        WHEN 해제사유발생일 IS NOT NULL AND TRIM(해제사유발생일) NOT IN ('', '-') THEN 1
        ELSE 0
    END AS 해제여부,
    (
        COALESCE(
            CASE
                WHEN 번지 IS NOT NULL AND TRIM(번지) NOT IN ('', '-')
                THEN TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(시도), ''),
                        NULLIF(TRIM(시군구_분리), ''),
                        NULLIF(TRIM(읍면동), ''),
                        NULLIF(TRIM(리), ''),
                        NULLIF(TRIM(번지), '')
                    )
                )
                ELSE NULL
            END,
            ''
        ) || '|' ||
        COALESCE(단지명_공백제거, '')
    ) AS 단지기본키,
    (
        COALESCE(
            CASE
                WHEN 번지 IS NOT NULL AND TRIM(번지) NOT IN ('', '-')
                THEN TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(시도), ''),
                        NULLIF(TRIM(시군구_분리), ''),
                        NULLIF(TRIM(읍면동), ''),
                        NULLIF(TRIM(리), ''),
                        NULLIF(TRIM(번지), '')
                    )
                )
                ELSE NULL
            END,
            ''
        ) || '|' ||
        COALESCE(단지명_공백제거, '') || '|' ||
        COALESCE(전용면적_구분, '')
    ) AS 단지면적키
FROM base;

CREATE OR REPLACE VIEW v_sale_clean AS
SELECT *
FROM v_sale_prepared
WHERE 명백오류여부 = 0
  AND COALESCE(거래유형_정규화, '') <> '직거래'
  AND 해제여부 = 0;

CREATE OR REPLACE VIEW v_lease_prepared AS
WITH base AS (
    SELECT
        TRIM(l.시군구) AS 시군구,
        TRIM(COALESCE(l.번지, '')) AS 번지,
        l.본번,
        l.부번,
        TRIM(COALESCE(l.단지명, '')) AS 단지명,
        TRIM(COALESCE(l.전월세구분, '')) AS 전월세구분,
        l."전용면적(㎡)",
        l.계약년월,
        l.계약일,
        l."보증금(만원)",
        l."월세금(만원)",
        l.층,
        l.건축년도,
        TRIM(COALESCE(l.도로명, '')) AS 도로명,
        TRIM(COALESCE(l.계약기간, '')) AS 계약기간,
        TRIM(COALESCE(l.계약구분, '')) AS 계약구분,
        TRIM(COALESCE(l."갱신요구권 사용", '')) AS "갱신요구권 사용",
        TRIM(COALESCE(l."종전계약 보증금(만원)", '')) AS "종전계약 보증금(만원)",
        TRIM(COALESCE(l."종전계약 월세(만원)", '')) AS "종전계약 월세(만원)",
        TRIM(COALESCE(l.주택유형, '')) AS 주택유형,
        TRIM(COALESCE(l.시도, '')) AS 시도,
        TRIM(COALESCE(l.읍면동, '')) AS 읍면동,
        TRIM(COALESCE(l.리, '')) AS 리,
        p.최신계약년월,
        p.기준연도,
        CAST(TRY_CAST(NULLIF(REPLACE("보증금(만원)", ',', ''), '-') AS BIGINT) AS BIGINT) AS 보증금_만원,
        CAST(TRY_CAST(NULLIF(REPLACE("월세금(만원)", ',', ''), '-') AS BIGINT) AS BIGINT) AS 월세금_만원,
        "전용면적(㎡)" AS 전용면적_㎡,
        TRIM(COALESCE(l.시군구, '')) AS 시군구_분리,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(COALESCE(단지명, ''), '\\([^)]*\\)', '', 'g'),
                '\\s+',
                ' ',
                'g'
            )
        ) AS 조인_단지명,
        CASE
            WHEN "전용면적(㎡)" <= 40 THEN '초소형'
            WHEN "전용면적(㎡)" <= 60 THEN '소형'
            WHEN "전용면적(㎡)" <= 85 THEN '중소형'
            WHEN "전용면적(㎡)" <= 135 THEN '중대형'
            ELSE '대형'
        END AS 전용면적_구분,
        "전용면적(㎡)" * 0.4 AS 추정평형,
        CASE
            WHEN "전용면적(㎡)" * 0.4 < 10 THEN '10평 미만'
            WHEN "전용면적(㎡)" * 0.4 < 20 THEN '10평대'
            WHEN "전용면적(㎡)" * 0.4 < 30 THEN '20평대'
            WHEN "전용면적(㎡)" * 0.4 < 40 THEN '30평대'
            WHEN "전용면적(㎡)" * 0.4 < 50 THEN '40평대'
            WHEN "전용면적(㎡)" * 0.4 < 60 THEN '50평대'
            ELSE '60평 이상'
        END AS 평형대_구분,
        CAST(FLOOR(계약년월 / 100) AS INTEGER) AS 계약연도,
        GREATEST(p.기준연도 - 건축년도, 1) AS 연식
    FROM 전월세 l
    CROSS JOIN v_params p
    WHERE 주택유형 = '아파트'
)
SELECT
    *,
    CASE
        WHEN 번지 IS NOT NULL AND TRIM(번지) NOT IN ('', '-')
        THEN TRIM(
            CONCAT_WS(
                ' ',
                NULLIF(TRIM(시도), ''),
                NULLIF(TRIM(시군구_분리), ''),
                NULLIF(TRIM(읍면동), ''),
                NULLIF(TRIM(리), ''),
                NULLIF(TRIM(번지), '')
            )
        )
        ELSE NULL
    END AS 지번주소,
    CASE
        WHEN 연식 < 5 THEN '5년 미만'
        WHEN 연식 < 10 THEN '5~10년 미만'
        WHEN 연식 < 20 THEN '10~20년 미만'
        WHEN 연식 < 30 THEN '20~30년 미만'
        ELSE '30년 이상'
    END AS 연식_구분,
    CASE
        WHEN 보증금_만원 IS NULL OR 보증금_만원 < 0 OR 전용면적_㎡ IS NULL OR 전용면적_㎡ <= 0 THEN 1
        WHEN 전월세구분 = '월세' AND (월세금_만원 IS NULL OR 월세금_만원 < 0) THEN 1
        ELSE 0
    END AS 명백오류여부,
    (
        COALESCE(
            CASE
                WHEN 번지 IS NOT NULL AND TRIM(번지) NOT IN ('', '-')
                THEN TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(시도), ''),
                        NULLIF(TRIM(시군구_분리), ''),
                        NULLIF(TRIM(읍면동), ''),
                        NULLIF(TRIM(리), ''),
                        NULLIF(TRIM(번지), '')
                    )
                )
                ELSE NULL
            END,
            ''
        ) || '|' ||
        COALESCE(단지명_공백제거, '')
    ) AS 단지기본키,
    (
        COALESCE(
            CASE
                WHEN 번지 IS NOT NULL AND TRIM(번지) NOT IN ('', '-') THEN TRIM(시군구) || ' ' || TRIM(번지)
                ELSE NULL
            END,
            ''
        ) || '|' ||
        COALESCE(단지명_공백제거, '') || '|' ||
        COALESCE(전용면적_구분, '')
    ) AS 단지면적키
FROM base;

CREATE OR REPLACE VIEW v_lease_clean AS
SELECT *
FROM v_lease_prepared
WHERE 명백오류여부 = 0
  AND 전월세구분 IN ('전세', '월세');

CREATE OR REPLACE VIEW v_jeonse_clean AS
SELECT *
FROM v_lease_clean
WHERE 전월세구분 = '전세';

CREATE OR REPLACE VIEW v_wolse_clean AS
SELECT *
FROM v_lease_clean
WHERE 전월세구분 = '월세';

CREATE OR REPLACE VIEW v_sale_monthly_metrics AS
SELECT
    시도,
    시군구_분리 AS 시군구,
    읍면동,
    단지명_공백제거 AS 단지명_정규화,
    전용면적_구분,
    계약년월,
    COUNT(*) AS 거래건수,
    MEDIAN(거래금액_만원) AS 매매대표가격_만원,
    MEDIAN(거래금액_만원 * 3.3 / NULLIF(전용면적_㎡, 0)) AS 매매대표평당가_만원,
    CASE WHEN COUNT(*) < 10 THEN '분기' ELSE '월' END AS 권장집계단위
FROM v_sale_clean
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW v_jeonse_monthly_metrics AS
SELECT
    시도,
    시군구_분리 AS 시군구,
    읍면동,
    단지명_공백제거 AS 단지명_정규화,
    전용면적_구분,
    계약년월,
    COUNT(*) AS 거래건수,
    MEDIAN(보증금_만원) AS 전세대표보증금_만원,
    MEDIAN(보증금_만원 * 3.3 / NULLIF(전용면적_㎡, 0)) AS 전세대표평당가_만원,
    CASE WHEN COUNT(*) < 10 THEN '분기' ELSE '월' END AS 권장집계단위
FROM v_jeonse_clean
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW v_wolse_monthly_metrics AS
SELECT
    시도,
    시군구_분리 AS 시군구,
    읍면동,
    단지명_공백제거 AS 단지명_정규화,
    전용면적_구분,
    계약년월,
    COUNT(*) AS 거래건수,
    MEDIAN(보증금_만원) AS 월세대표보증금_만원,
    MEDIAN(월세금_만원) AS 월세대표월세액_만원,
    CASE WHEN COUNT(*) < 10 THEN '분기' ELSE '월' END AS 권장집계단위
FROM v_wolse_clean
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW v_grade_base_price AS
WITH params AS (
    SELECT
        최신계약년월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 11 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 기준시작년월
    FROM v_params
)
SELECT
    s.시도,
    s.시군구_분리 AS 시군구,
    s.읍면동,
    s.단지명_공백제거 AS 단지명_정규화,
    s.전용면적_구분,
    MEDIAN(s.거래금액_만원 * 3.3 / NULLIF(s.전용면적_㎡, 0)) AS 기준평당가_만원,
    COUNT(*) AS 기준표본수
FROM v_sale_clean s
CROSS JOIN params p
WHERE s.계약년월 BETWEEN p.기준시작년월 AND p.최신계약년월
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE VIEW v_supply_proxy_monthly AS
SELECT
    시도,
    시군구,
    읍면동,
    단지명_공백제거 AS 단지명_정규화,
    세대수,
    사용승인년월,
    COUNT(*) AS 단지수,
    SUM(COALESCE(세대수, 0)) AS 세대수합계
FROM v_complex_master_base
WHERE 사용승인년월 IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW v_movein_plan_invalid_month AS
SELECT
    입주예정월,
    지역,
    사업유형,
    주소,
    아파트명,
    세대수,
    시도,
    시군구,
    읍면동,
    리
FROM 입주물량
WHERE 입주예정월 IS NULL
   OR MOD(입주예정월, 100) NOT BETWEEN 1 AND 12;

CREATE OR REPLACE VIEW v_movein_plan_base AS
SELECT
    입주예정월,
    CAST(FLOOR(입주예정월 / 100) AS INTEGER) AS 입주예정연도,
    MOD(입주예정월, 100) AS 입주예정월_번호,
    지역,
    사업유형,
    주소,
    아파트명,
    세대수,
    시도,
    시군구,
    읍면동,
    리
FROM 입주물량
WHERE 입주예정월 IS NOT NULL
  AND MOD(입주예정월, 100) BETWEEN 1 AND 12;

CREATE OR REPLACE VIEW v_movein_plan_sigungu_monthly AS
SELECT
    시도,
    시군구,
    읍면동,
    입주예정월,
    입주예정연도,
    COUNT(*) AS 사업장수,
    SUM(COALESCE(세대수, 0)) AS 입주예정세대수
FROM v_movein_plan_base
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE VIEW v_movein_plan_region_monthly AS
SELECT
    CASE
        WHEN 시도 IN ('서울특별시', '경기도', '인천광역시') THEN '수도권'
        WHEN 시도 IN ('부산광역시', '대구광역시', '광주광역시', '대전광역시', '울산광역시') THEN '지방광역시'
        ELSE '기타지방'
    END AS 권역구분,
    입주예정월,
    입주예정연도,
    COUNT(*) AS 사업장수,
    SUM(COALESCE(세대수, 0)) AS 입주예정세대수
FROM v_movein_plan_base
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW v_tohuga_approval_base AS
WITH raw AS (
    SELECT
        TRIM(COALESCE(주소, '')) AS 주소,
        TRIM(COALESCE(도로명주소, '')) AS 도로명주소,
        TRIM(COALESCE(지번_지목, '')) AS 지번_지목,
        CAST(허가년월일 AS DATE) AS 허가년월일,
        CAST(STRFTIME(CAST(허가년월일 AS DATE), '%Y%m') AS INTEGER) AS 허가년월,
        CAST(이용의무종료일 AS DATE) AS 이용의무종료일,
        TRIM(COALESCE(이용목적, '')) AS 이용목적,
        TRIM(COALESCE(허가사항, '')) AS 허가사항,
        위도,
        경도,
        TRIM(COALESCE(구, '')) AS 구,
        CAST(수집일자 AS DATE) AS 수집일자
    FROM 토지거래허가구역
),
parsed AS (
    SELECT
        *,
        COALESCE(
            NULLIF(SPLIT_PART(도로명주소, ' ', 1), ''),
            CASE
                WHEN NULLIF(구, '') IS NOT NULL
                 AND NULLIF(SPLIT_PART(주소, ' ', 1), '') = NULLIF(구, '')
                THEN '서울특별시'
                ELSE NULL
            END,
            NULL
        ) AS 시도,
        COALESCE(
            NULLIF(SPLIT_PART(도로명주소, ' ', 2), ''),
            NULLIF(SPLIT_PART(주소, ' ', 1), ''),
            NULLIF(구, '')
        ) AS 시군구,
        COALESCE(
            NULLIF(SPLIT_PART(도로명주소, ' ', 3), ''),
            NULLIF(SPLIT_PART(주소, ' ', 2), '')
        ) AS 읍면동_원문,
        COALESCE(
            NULLIF(도로명주소, ''),
            NULLIF(주소, '')
        ) AS 주소키
    FROM raw
)
SELECT
    시도,
    시군구,
    읍면동_원문,
    구,
    주소,
    도로명주소,
    주소키,
    지번_지목,
    허가년월일,
    허가년월,
    CAST(FLOOR(허가년월 / 100) AS INTEGER) AS 허가연도,
    이용목적,
    이용의무종료일,
    허가사항,
    위도,
    경도,
    수집일자
FROM parsed;

CREATE OR REPLACE VIEW v_tohuga_address_latest AS
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 주소키
            ORDER BY 수집일자 DESC, 허가년월일 DESC, 주소키
        ) AS rn
    FROM v_tohuga_approval_base
    WHERE 주소키 IS NOT NULL
)
SELECT
    시도,
    시군구,
    읍면동_원문,
    구,
    주소,
    도로명주소,
    주소키,
    지번_지목,
    허가년월일,
    허가년월,
    허가연도,
    이용목적,
    이용의무종료일,
    허가사항,
    위도,
    경도,
    수집일자
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW v_tohuga_sigungu_monthly AS
SELECT
    COALESCE(시도, '시도미상') AS 시도,
    COALESCE(시군구, '시군구미상') AS 시군구,
    COALESCE(NULLIF(구, ''), 시군구, '구미상') AS 구,
    허가년월,
    허가연도,
    COUNT(*) AS 허가승인건수,
    COUNT(DISTINCT 주소키) AS 허가대상주소수
FROM v_tohuga_approval_base
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE VIEW v_tohuga_latest_collection_sigungu AS
WITH latest AS (
    SELECT MAX(수집일자) AS 최신수집일자
    FROM v_tohuga_approval_base
)
SELECT
    l.최신수집일자,
    COALESCE(b.시도, '시도미상') AS 시도,
    COALESCE(b.시군구, '시군구미상') AS 시군구,
    COALESCE(NULLIF(b.구, ''), b.시군구, '구미상') AS 구,
    COUNT(*) AS 최신수집허가건수,
    COUNT(DISTINCT b.주소키) AS 최신수집주소수,
    MIN(b.허가년월일) AS 최신수집허가최소일,
    MAX(b.허가년월일) AS 최신수집허가최대일
FROM v_tohuga_approval_base b
CROSS JOIN latest l
WHERE b.수집일자 = l.최신수집일자
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW v_tohuga_recent_sigungu_summary AS
WITH latest AS (
    SELECT
        MAX(허가년월일) AS 최신허가일,
        MAX(수집일자) AS 최신수집일자
    FROM v_tohuga_approval_base
)
SELECT
    COALESCE(b.시도, '시도미상') AS 시도,
    COALESCE(b.시군구, '시군구미상') AS 시군구,
    COALESCE(NULLIF(b.구, ''), b.시군구, '구미상') AS 구,
    l.최신허가일,
    l.최신수집일자,
    MAX(b.허가년월일) AS 최근허가일,
    SUM(CASE WHEN b.허가년월일 >= l.최신허가일 - INTERVAL 30 DAY THEN 1 ELSE 0 END) AS 최근30일허가건수,
    SUM(CASE WHEN b.허가년월일 >= l.최신허가일 - INTERVAL 60 DAY THEN 1 ELSE 0 END) AS 최근60일허가건수,
    COUNT(DISTINCT CASE WHEN b.허가년월일 >= l.최신허가일 - INTERVAL 60 DAY THEN b.주소키 END) AS 최근60일허가주소수,
    CASE
        WHEN SUM(CASE WHEN b.허가년월일 >= l.최신허가일 - INTERVAL 60 DAY THEN 1 ELSE 0 END) > 0 THEN 1
        ELSE 0
    END AS 토허지연해석주의,
    CASE
        WHEN SUM(CASE WHEN b.허가년월일 >= l.최신허가일 - INTERVAL 30 DAY THEN 1 ELSE 0 END) > 0
            THEN '최근 30일 허가 승인 존재'
        WHEN SUM(CASE WHEN b.허가년월일 >= l.최신허가일 - INTERVAL 60 DAY THEN 1 ELSE 0 END) > 0
            THEN '최근 60일 허가 승인 존재'
        ELSE NULL
    END AS 토허지연해석메모
FROM v_tohuga_approval_base b
CROSS JOIN latest l
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE VIEW v_tohuga_active_manual_ref AS
SELECT
    TRY_CAST(NULLIF(TRIM(COALESCE(확인기준일, '')), '') AS DATE) AS 확인기준일,
    TRIM(COALESCE(시도, '')) AS 시도,
    TRIM(COALESCE(시군구, '')) AS 시군구원문,
    TRIM(COALESCE(세부구역명, '')) AS 세부구역명,
    TRIM(COALESCE(적용범위요약, '')) AS 적용범위요약,
    TRIM(COALESCE(현재판정, '')) AS 현재판정,
    CASE
        WHEN TRIM(COALESCE(현재판정, '')) IN ('활성확인', '활성추정_공고확인') THEN 1
        ELSE 0
    END AS 현재활성플래그,
    TRIM(COALESCE(근거요약, '')) AS 근거요약,
    TRY_CAST(NULLIF(TRIM(COALESCE(출처기준일, '')), '') AS DATE) AS 출처기준일,
    TRIM(COALESCE(출처유형, '')) AS 출처유형,
    TRIM(COALESCE(출처URL, '')) AS 출처URL,
    TRY_CAST(NULLIF(TRIM(COALESCE(지정시작일, '')), '') AS DATE) AS 지정시작일,
    TRY_CAST(NULLIF(TRIM(COALESCE(지정종료일, '')), '') AS DATE) AS 지정종료일,
    TRIM(COALESCE(비고, '')) AS 비고
FROM read_csv_auto(
    '/Users/foodie/myproject/Real-Estate-Investment/APT-Price-Pattern/02_데이터/02_참조/토지거래허가구역_활성참조_20260313.csv',
    header = true,
    all_varchar = true
);

CREATE OR REPLACE VIEW v_tohuga_active_manual_sigungu AS
WITH base AS (
    SELECT *
    FROM v_tohuga_active_manual_ref
),
seoul_sigungu AS (
    SELECT DISTINCT 시군구
    FROM v_sale_clean
    WHERE 시도 = '서울특별시'
      AND NULLIF(TRIM(COALESCE(시군구, '')), '') IS NOT NULL
),
exploded AS (
    SELECT
        b.확인기준일,
        b.시도,
        TRIM(s.시군구) AS 시군구,
        b.시군구원문,
        b.세부구역명,
        b.적용범위요약,
        b.현재판정,
        b.현재활성플래그,
        b.근거요약,
        b.출처기준일,
        b.출처유형,
        b.출처URL,
        b.지정시작일,
        b.지정종료일,
        b.비고,
        '서울시전체확장' AS 시군구매핑규칙
    FROM base b
    CROSS JOIN seoul_sigungu s
    WHERE b.시도 = '서울특별시'
      AND b.시군구원문 = '서울시 전체'

    UNION ALL

    SELECT
        b.확인기준일,
        b.시도,
        TRIM(part) AS 시군구,
        b.시군구원문,
        b.세부구역명,
        b.적용범위요약,
        b.현재판정,
        b.현재활성플래그,
        b.근거요약,
        b.출처기준일,
        b.출처유형,
        b.출처URL,
        b.지정시작일,
        b.지정종료일,
        b.비고,
        '세미콜론분해' AS 시군구매핑규칙
    FROM base b,
         UNNEST(STRING_SPLIT(b.시군구원문, ';')) AS u(part)
    WHERE b.시군구원문 LIKE '%;%'

    UNION ALL

    SELECT
        b.확인기준일,
        b.시도,
        b.시군구원문 AS 시군구,
        b.시군구원문,
        b.세부구역명,
        b.적용범위요약,
        b.현재판정,
        b.현재활성플래그,
        b.근거요약,
        b.출처기준일,
        b.출처유형,
        b.출처URL,
        b.지정시작일,
        b.지정종료일,
        b.비고,
        '단일시군구' AS 시군구매핑규칙
    FROM base b
    WHERE b.시군구원문 NOT LIKE '%;%'
      AND b.시군구원문 NOT IN ('서울시 전체', '서울시 다수 지역')
      AND NULLIF(TRIM(COALESCE(b.시군구원문, '')), '') IS NOT NULL
)
SELECT DISTINCT
    확인기준일,
    시도,
    시군구,
    시군구원문,
    세부구역명,
    적용범위요약,
    현재판정,
    현재활성플래그,
    근거요약,
    출처기준일,
    출처유형,
    출처URL,
    지정시작일,
    지정종료일,
    비고,
    시군구매핑규칙
FROM exploded;

CREATE OR REPLACE VIEW v_tohuga_current_sigungu_context AS
WITH manual AS (
    SELECT
        시도,
        시군구,
        MAX(현재활성플래그) AS 현재활성토허플래그,
        MAX(CASE WHEN 현재판정 = '활성확인' THEN 1 ELSE 0 END) AS 활성확인플래그,
        MAX(CASE WHEN 현재판정 = '활성추정_공고확인' THEN 1 ELSE 0 END) AS 활성추정플래그,
        STRING_AGG(DISTINCT 현재판정, '; ' ORDER BY 현재판정) AS 현재판정요약,
        STRING_AGG(DISTINCT 세부구역명, '; ' ORDER BY 세부구역명) AS 세부구역명요약,
        STRING_AGG(DISTINCT 적용범위요약, '; ' ORDER BY 적용범위요약) AS 적용범위요약,
        STRING_AGG(DISTINCT 시군구매핑규칙, '; ' ORDER BY 시군구매핑규칙) AS 시군구매핑규칙요약,
        MIN(CASE WHEN 현재활성플래그 = 1 THEN 지정시작일 END) AS 활성지정시작일,
        MAX(CASE WHEN 현재활성플래그 = 1 THEN 지정종료일 END) AS 활성지정종료일,
        STRING_AGG(DISTINCT 비고, '; ' ORDER BY 비고) AS 수동참조비고
    FROM v_tohuga_active_manual_sigungu
    GROUP BY 1, 2
)
SELECT
    COALESCE(m.시도, r.시도) AS 시도,
    COALESCE(m.시군구, r.시군구) AS 시군구,
    COALESCE(r.구, m.시군구) AS 구,
    COALESCE(m.현재활성토허플래그, 0) AS 현재활성토허플래그,
    COALESCE(m.활성확인플래그, 0) AS 활성확인플래그,
    COALESCE(m.활성추정플래그, 0) AS 활성추정플래그,
    m.현재판정요약,
    m.세부구역명요약,
    m.적용범위요약,
    m.시군구매핑규칙요약,
    m.활성지정시작일,
    m.활성지정종료일,
    m.수동참조비고,
    r.최신허가일,
    r.최신수집일자,
    r.최근허가일,
    COALESCE(r.최근30일허가건수, 0) AS 최근30일허가건수,
    COALESCE(r.최근60일허가건수, 0) AS 최근60일허가건수,
    COALESCE(r.최근60일허가주소수, 0) AS 최근60일허가주소수,
    COALESCE(r.토허지연해석주의, 0) AS 허가이력기준지연주의,
    r.토허지연해석메모 AS 허가이력기준지연메모,
    CASE
        WHEN COALESCE(m.현재활성토허플래그, 0) = 1 THEN 1
        WHEN COALESCE(r.토허지연해석주의, 0) = 1 THEN 1
        ELSE 0
    END AS 토허현재해석보정플래그,
    CASE
        WHEN COALESCE(m.현재활성토허플래그, 0) = 1
             AND COALESCE(r.최근60일허가건수, 0) > 0
            THEN '활성 토허구역이며 최근 60일 허가 승인 존재: 최근 1~2개월 실거래 지연 가능'
        WHEN COALESCE(m.현재활성토허플래그, 0) = 1
            THEN '활성 토허구역: 최근 1~2개월 실거래 지연 가능'
        WHEN COALESCE(r.토허지연해석주의, 0) = 1
            THEN r.토허지연해석메모
        ELSE NULL
    END AS 토허현재해석보정메모
FROM manual m
FULL OUTER JOIN v_tohuga_recent_sigungu_summary r
    ON m.시도 = r.시도
   AND m.시군구 = r.시군구;

CREATE OR REPLACE VIEW v_complex_product_national AS
WITH params AS (
    SELECT 기준연도 FROM v_params
)
SELECT
    m.*,
    CAST(FLOOR(m.사용승인년월 / 100) AS INTEGER) AS 사용승인연도,
    CASE
        WHEN m.사용승인년월 IS NOT NULL
        THEN GREATEST(p.기준연도 - CAST(FLOOR(m.사용승인년월 / 100) AS INTEGER), 1)
        ELSE NULL
    END AS 기준연식,
    CASE
        WHEN m.사용승인년월 IS NULL THEN '미상'
        WHEN GREATEST(p.기준연도 - CAST(FLOOR(m.사용승인년월 / 100) AS INTEGER), 1) < 5 THEN '5년 미만'
        WHEN GREATEST(p.기준연도 - CAST(FLOOR(m.사용승인년월 / 100) AS INTEGER), 1) < 10 THEN '5~10년 미만'
        WHEN GREATEST(p.기준연도 - CAST(FLOOR(m.사용승인년월 / 100) AS INTEGER), 1) < 20 THEN '10~20년 미만'
        WHEN GREATEST(p.기준연도 - CAST(FLOOR(m.사용승인년월 / 100) AS INTEGER), 1) < 30 THEN '20~30년 미만'
        ELSE '30년 이상'
    END AS 기준연식_구분,
    CASE
        WHEN m.세대수 IS NULL THEN '미상'
        WHEN m.세대수 < 500 THEN '500세대 미만'
        WHEN m.세대수 < 1000 THEN '500~999세대'
        WHEN m.세대수 < 1500 THEN '1000~1499세대'
        ELSE '1500세대 이상'
    END AS 세대수_구간,
    CASE
        WHEN m.동수 IS NULL THEN '미상'
        WHEN m.동수 < 5 THEN '5동 미만'
        WHEN m.동수 < 10 THEN '5~9동'
        WHEN m.동수 < 20 THEN '10~19동'
        ELSE '20동 이상'
    END AS 동수_구간
FROM v_complex_master_base m
CROSS JOIN params p;

CREATE OR REPLACE VIEW v_seoul_internal_location_ref AS
WITH info_base AS (
    SELECT
        단지코드,
        TRIM(COALESCE(단지명, '')) AS 단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        RTRIM(
            CASE
                WHEN RIGHT(TRIM(COALESCE(법정동주소, '')), LENGTH(TRIM(COALESCE(단지명, '')))) = TRIM(COALESCE(단지명, ''))
                THEN LEFT(
                    TRIM(COALESCE(법정동주소, '')),
                    LENGTH(TRIM(COALESCE(법정동주소, ''))) - LENGTH(TRIM(COALESCE(단지명, '')))
                )
                ELSE TRIM(COALESCE(법정동주소, ''))
            END
        ) AS 지번주소,
        TRIM(COALESCE(도로명주소, '')) AS 도로명주소,
        TRIM(COALESCE(난방방식, '')) AS 난방방식,
        TRIM(COALESCE(복도유형, '')) AS 복도유형,
        동수,
        세대수,
        최고층수,
        지상주차대수,
        지하주차대수,
        시공사,
        시행사,
        교육시설,
        버스정류장거리,
        지하철노선,
        지하철역,
        지하철역거리,
        수집일자
    FROM 공동주택_기본정보
    WHERE SPLIT_PART(TRIM(COALESCE(법정동주소, '')), ' ', 1) = '서울특별시'
),
info_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 지번주소, 단지명_공백제거
            ORDER BY 수집일자 DESC, 세대수 DESC, 단지코드
        ) AS rn
    FROM info_base
)
SELECT
    i.단지코드,
    i.지번주소,
    i.도로명주소,
    i.단지명,
    i.정규화_단지명,
    i.단지명_공백제거,
    i.난방방식,
    i.복도유형,
    i.동수,
    i.세대수,
    i.최고층수,
    i.지상주차대수,
    i.지하주차대수,
    i.시공사,
    i.시행사,
    i.교육시설,
    i.버스정류장거리,
    i.지하철노선,
    i.지하철역,
    i.지하철역거리,
    z.경도,
    z.위도,
    CASE WHEN z.경도 IS NOT NULL AND z.위도 IS NOT NULL THEN 1 ELSE 0 END AS 내부좌표여부,
    '내부_공동주택기본정보+좌표' AS 내부좌표출처,
    i.수집일자
FROM info_ranked i
LEFT JOIN 좌표 z
    ON i.도로명주소 = z.도로명주소
WHERE i.rn = 1;

CREATE OR REPLACE VIEW v_sudogwon_external_coordinate_ref AS
WITH base AS (
    SELECT
        TRIM(시군구) AS 행정주소,
        TRIM(COALESCE(번지, '')) AS 번지,
        TRIM(COALESCE(단지명, '')) AS 단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        TRIM(CONCAT_WS(' ', TRIM(시군구), TRIM(COALESCE(번지, '')))) AS 지번주소,
        TRIM(COALESCE(주소, '')) AS 원본주소,
        위도,
        경도,
        TRIM(COALESCE(수집여부, '')) AS 수집여부
    FROM read_csv_auto(
        '/Users/foodie/myproject/Real-Estate-Investment/APT-Price-Pattern/02_데이터/03_가공/상품성/단지_좌표.csv',
        header = true
    )
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 지번주소, 단지명_공백제거
            ORDER BY 수집여부 DESC, 원본주소
        ) AS rn
    FROM base
)
SELECT
    SPLIT_PART(행정주소, ' ', 1) AS 시도,
    CASE
        WHEN SPLIT_PART(행정주소, ' ', 4) <> '' THEN SPLIT_PART(행정주소, ' ', 2) || ' ' || SPLIT_PART(행정주소, ' ', 3)
        ELSE SPLIT_PART(행정주소, ' ', 2)
    END AS 시군구,
    CASE
        WHEN SPLIT_PART(행정주소, ' ', 4) <> '' THEN SPLIT_PART(행정주소, ' ', 4)
        ELSE SPLIT_PART(행정주소, ' ', 3)
    END AS 읍면동,
    행정주소,
    번지,
    지번주소,
    단지명,
    정규화_단지명,
    단지명_공백제거,
    원본주소,
    위도,
    경도,
    수집여부,
    '외부_단지좌표.csv' AS 좌표출처
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW v_sudogwon_external_location_ref AS
WITH loc_base AS (
    SELECT
        TRIM(시군구) AS 행정주소,
        TRIM(COALESCE(번지, '')) AS 번지,
        TRIM(COALESCE(단지명, '')) AS 단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        TRIM(CONCAT_WS(' ', TRIM(시군구), TRIM(COALESCE(번지, '')))) AS 지번주소,
        TRIM(COALESCE(주소, '')) AS 원본주소,
        위도,
        경도,
        TRIM(COALESCE(수집여부, '')) AS 수집여부,
        역세권_지수,
        학교_500m이내,
        공원_최단거리,
        한강_최단거리,
        한강뷰_더미,
        한강_프리미엄점수,
        CBD_최단거리,
        종합병원_최단거리,
        대규모점포_최단거리,
        학원밀집도,
        입시학원수
    FROM read_csv_auto(
        '/Users/foodie/myproject/Real-Estate-Investment/APT-Price-Pattern/02_데이터/03_가공/상품성/아파트_단지_위치변수.csv',
        header = true
    )
),
loc_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 지번주소, 단지명_공백제거
            ORDER BY 수집여부 DESC, 원본주소
        ) AS rn
    FROM loc_base
)
SELECT
    SPLIT_PART(행정주소, ' ', 1) AS 시도,
    CASE
        WHEN SPLIT_PART(행정주소, ' ', 4) <> '' THEN SPLIT_PART(행정주소, ' ', 2) || ' ' || SPLIT_PART(행정주소, ' ', 3)
        ELSE SPLIT_PART(행정주소, ' ', 2)
    END AS 시군구,
    CASE
        WHEN SPLIT_PART(행정주소, ' ', 4) <> '' THEN SPLIT_PART(행정주소, ' ', 4)
        ELSE SPLIT_PART(행정주소, ' ', 3)
    END AS 읍면동,
    행정주소,
    번지,
    지번주소,
    단지명,
    정규화_단지명,
    단지명_공백제거,
    원본주소,
    위도,
    경도,
    수집여부,
    역세권_지수,
    학교_500m이내,
    공원_최단거리,
    한강_최단거리,
    한강뷰_더미,
    한강_프리미엄점수,
    CBD_최단거리,
    종합병원_최단거리,
    대규모점포_최단거리,
    학원밀집도,
    입시학원수,
    '외부_아파트_단지_위치변수.csv' AS 입지출처
FROM loc_ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW v_sudogwon_external_meta_ref AS
WITH meta_base AS (
    SELECT
        TRIM(시군구) AS 시군구,
        TRIM(COALESCE(단지명, '')) AS 단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
        TRIM(REGEXP_REPLACE(COALESCE(단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
        동수,
        총세대수,
        최소면적,
        최대면적,
        평균면적,
        면적표준편차,
        최소공시가격,
        최대공시가격,
        평균공시가격,
        중위공시가격,
        공시가격표준편차,
        면적다양성,
        가격다양성,
        단지규모
    FROM read_csv_auto(
        '/Users/foodie/myproject/Real-Estate-Investment/APT-Price-Pattern/02_데이터/03_가공/상품성/단지_메타정보.csv',
        header = true
    )
),
meta_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 시군구, 단지명_공백제거
            ORDER BY 총세대수 DESC, 평균공시가격 DESC NULLS LAST, 단지명
        ) AS rn,
        COUNT(*) OVER (
            PARTITION BY 시군구, 단지명_공백제거
        ) AS 후보수
    FROM meta_base
)
SELECT
    *,
    CASE WHEN 후보수 = 1 THEN 1 ELSE 0 END AS 보조메타_유일매칭여부
FROM meta_ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW v_trade_complex_lookup_base AS
SELECT
    '매매' AS 거래구분,
    시도,
    시군구_분리 AS 시군구,
    읍면동,
    TRIM(COALESCE(번지, '')) AS 번지,
    지번주소,
    단지명_공백제거,
    건축년도,
    COUNT(*) AS 거래건수
FROM v_sale_clean
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

UNION ALL

SELECT
    '임차' AS 거래구분,
    시도,
    시군구_분리 AS 시군구,
    읍면동,
    TRIM(COALESCE(번지, '')) AS 번지,
    지번주소,
    단지명_공백제거,
    건축년도,
    COUNT(*) AS 거래건수
FROM v_lease_clean
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

CREATE OR REPLACE VIEW v_trade_complex_product_lookup AS
WITH master AS (
    SELECT *
    FROM v_complex_product_national
),
addr_counts AS (
    SELECT
        지번주소,
        COUNT(*) AS 주소후보수
    FROM master
    GROUP BY 1
),
name_counts AS (
    SELECT
        지번주소,
        단지명_공백제거,
        COUNT(*) AS 단지명후보수
    FROM master
    GROUP BY 1, 2
),
year_counts AS (
    SELECT
        지번주소,
        사용승인연도,
        COUNT(*) AS 승인연도후보수
    FROM master
    GROUP BY 1, 2
),
trade_with_rule_base AS (
    SELECT
        t.거래구분,
        t.시도,
        t.시군구,
        t.읍면동,
        t.번지,
        t.지번주소,
        t.단지명_공백제거,
        t.건축년도,
        t.거래건수,
        COALESCE(n.단지명후보수, 0) AS 단지명후보수,
        COALESCE(a.주소후보수, 0) AS 주소후보수,
        COUNT(y.사용승인연도) AS 승인연도유일후보수,
        MIN(y.사용승인연도) AS 승인연도유일후보
    FROM v_trade_complex_lookup_base t
    LEFT JOIN name_counts n
        ON t.지번주소 = n.지번주소
       AND t.단지명_공백제거 = n.단지명_공백제거
    LEFT JOIN addr_counts a
        ON t.지번주소 = a.지번주소
    LEFT JOIN year_counts y
        ON t.지번주소 = y.지번주소
       AND ABS(t.건축년도 - y.사용승인연도) <= 1
       AND y.승인연도후보수 = 1
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
),
resolved AS (
    SELECT
        *,
        CASE
            WHEN 단지명후보수 = 1 THEN '주소+단지명'
            WHEN 주소후보수 = 1 THEN '주소유일'
            WHEN 승인연도유일후보수 = 1 THEN '주소+승인연도'
            ELSE '미매칭'
        END AS 전국상품성_매칭규칙
    FROM trade_with_rule_base
)
SELECT
    r.*,
    COALESCE(m_name.단지고유번호, m_addr.단지고유번호, m_year.단지고유번호) AS 단지고유번호,
    COALESCE(m_name.필지고유번호, m_addr.필지고유번호, m_year.필지고유번호) AS 필지고유번호,
    COALESCE(m_name.세대수, m_addr.세대수, m_year.세대수) AS 세대수,
    COALESCE(m_name.세대수_구간, m_addr.세대수_구간, m_year.세대수_구간) AS 세대수_구간,
    COALESCE(m_name.동수, m_addr.동수, m_year.동수) AS 동수,
    COALESCE(m_name.동수_구간, m_addr.동수_구간, m_year.동수_구간) AS 동수_구간,
    COALESCE(m_name.사용승인일, m_addr.사용승인일, m_year.사용승인일) AS 사용승인일,
    COALESCE(m_name.사용승인일자, m_addr.사용승인일자, m_year.사용승인일자) AS 사용승인일자,
    COALESCE(m_name.사용승인년월, m_addr.사용승인년월, m_year.사용승인년월) AS 사용승인년월,
    COALESCE(m_name.사용승인연도, m_addr.사용승인연도, m_year.사용승인연도) AS 사용승인연도,
    COALESCE(m_name.기준연식, m_addr.기준연식, m_year.기준연식) AS 기준연식,
    COALESCE(m_name.기준연식_구분, m_addr.기준연식_구분, m_year.기준연식_구분) AS 기준연식_구분,
    CASE
        WHEN COALESCE(m_name.단지고유번호, m_addr.단지고유번호, m_year.단지고유번호) IS NOT NULL THEN 1
        ELSE 0
    END AS 전국상품성_매칭여부
FROM resolved r
LEFT JOIN master m_name
    ON r.전국상품성_매칭규칙 = '주소+단지명'
   AND r.지번주소 = m_name.지번주소
   AND r.단지명_공백제거 = m_name.단지명_공백제거
LEFT JOIN master m_addr
    ON r.전국상품성_매칭규칙 = '주소유일'
   AND r.지번주소 = m_addr.지번주소
LEFT JOIN master m_year
    ON r.전국상품성_매칭규칙 = '주소+승인연도'
   AND r.지번주소 = m_year.지번주소
   AND r.승인연도유일후보 = m_year.사용승인연도;

CREATE OR REPLACE VIEW v_trade_sudogwon_location_lookup AS
SELECT
    t.거래구분,
    t.시도,
    t.시군구,
    t.읍면동,
    t.번지,
    t.지번주소,
    t.단지명_공백제거,
    t.건축년도,
    t.거래건수,
    COALESCE(i.경도, e.경도) AS 경도,
    COALESCE(i.위도, e.위도) AS 위도,
    i.단지코드 AS 서울보강_단지코드,
    i.시공사 AS 서울보강_시공사,
    i.시행사 AS 서울보강_시행사,
    i.난방방식,
    i.복도유형,
    COALESCE(i.동수, NULL) AS 서울보강_동수,
    COALESCE(i.세대수, NULL) AS 서울보강_세대수,
    i.최고층수,
    i.지상주차대수,
    i.지하주차대수,
    i.교육시설,
    i.버스정류장거리,
    i.지하철노선,
    i.지하철역,
    i.지하철역거리,
    e.역세권_지수,
    e.학교_500m이내,
    e.공원_최단거리,
    e.한강_최단거리,
    e.한강뷰_더미,
    e.한강_프리미엄점수,
    e.CBD_최단거리,
    e.종합병원_최단거리,
    e.대규모점포_최단거리,
    e.학원밀집도,
    e.입시학원수,
    CASE
        WHEN t.시도 = '서울특별시' AND i.내부좌표여부 = 1 THEN '내부_공동주택기본정보+좌표'
        WHEN e.지번주소 IS NOT NULL THEN '외부_단지좌표및위치변수'
        ELSE '미매칭'
    END AS 수도권심화입지_매칭소스,
    CASE
        WHEN (t.시도 = '서울특별시' AND i.내부좌표여부 = 1) OR e.지번주소 IS NOT NULL THEN 1
        ELSE 0
    END AS 수도권심화입지_매칭여부
FROM v_trade_complex_lookup_base t
LEFT JOIN v_seoul_internal_location_ref i
    ON t.시도 = '서울특별시'
   AND t.지번주소 = i.지번주소
   AND t.단지명_공백제거 = i.단지명_공백제거
LEFT JOIN v_sudogwon_external_location_ref e
    ON t.시도 IN ('서울특별시', '경기도', '인천광역시')
   AND t.지번주소 = e.지번주소
   AND t.단지명_공백제거 = e.단지명_공백제거
WHERE t.시도 IN ('서울특별시', '경기도', '인천광역시');

CREATE OR REPLACE VIEW v_sale_enriched_base AS
SELECT
    s.*,
    p.단지고유번호,
    p.필지고유번호,
    p.세대수 AS 보강_세대수,
    p.세대수_구간,
    p.동수 AS 보강_동수,
    p.동수_구간,
    p.사용승인일,
    p.사용승인일자,
    p.사용승인년월,
    p.사용승인연도,
    p.기준연식,
    p.기준연식_구분,
    p.전국상품성_매칭규칙,
    p.전국상품성_매칭여부,
    l.경도,
    l.위도,
    l.서울보강_단지코드,
    l.서울보강_시공사,
    l.서울보강_시행사,
    l.난방방식,
    l.복도유형,
    l.최고층수,
    l.지상주차대수,
    l.지하주차대수,
    l.교육시설,
    l.버스정류장거리,
    l.지하철노선,
    l.지하철역,
    l.지하철역거리,
    l.역세권_지수,
    l.학교_500m이내,
    l.공원_최단거리,
    l.한강_최단거리,
    l.한강뷰_더미,
    l.한강_프리미엄점수,
    l.CBD_최단거리,
    l.종합병원_최단거리,
    l.대규모점포_최단거리,
    l.학원밀집도,
    l.입시학원수,
    l.수도권심화입지_매칭소스,
    COALESCE(l.수도권심화입지_매칭여부, 0) AS 수도권심화입지_매칭여부
FROM v_sale_clean s
LEFT JOIN v_trade_complex_product_lookup p
    ON p.거래구분 = '매매'
   AND s.지번주소 = p.지번주소
   AND s.단지명_공백제거 = p.단지명_공백제거
   AND s.건축년도 = p.건축년도
LEFT JOIN v_trade_sudogwon_location_lookup l
    ON l.거래구분 = '매매'
   AND s.지번주소 = l.지번주소
   AND s.단지명_공백제거 = l.단지명_공백제거
   AND s.건축년도 = l.건축년도;

CREATE OR REPLACE VIEW v_lease_enriched_base AS
SELECT
    s.*,
    p.단지고유번호,
    p.필지고유번호,
    p.세대수 AS 보강_세대수,
    p.세대수_구간,
    p.동수 AS 보강_동수,
    p.동수_구간,
    p.사용승인일,
    p.사용승인일자,
    p.사용승인년월,
    p.사용승인연도,
    p.기준연식,
    p.기준연식_구분,
    p.전국상품성_매칭규칙,
    p.전국상품성_매칭여부,
    l.경도,
    l.위도,
    l.서울보강_단지코드,
    l.서울보강_시공사,
    l.서울보강_시행사,
    l.난방방식,
    l.복도유형,
    l.최고층수,
    l.지상주차대수,
    l.지하주차대수,
    l.교육시설,
    l.버스정류장거리,
    l.지하철노선,
    l.지하철역,
    l.지하철역거리,
    l.역세권_지수,
    l.학교_500m이내,
    l.공원_최단거리,
    l.한강_최단거리,
    l.한강뷰_더미,
    l.한강_프리미엄점수,
    l.CBD_최단거리,
    l.종합병원_최단거리,
    l.대규모점포_최단거리,
    l.학원밀집도,
    l.입시학원수,
    l.수도권심화입지_매칭소스,
    COALESCE(l.수도권심화입지_매칭여부, 0) AS 수도권심화입지_매칭여부
FROM v_lease_clean s
LEFT JOIN v_trade_complex_product_lookup p
    ON p.거래구분 = '임차'
   AND s.지번주소 = p.지번주소
   AND s.단지명_공백제거 = p.단지명_공백제거
   AND s.건축년도 = p.건축년도
LEFT JOIN v_trade_sudogwon_location_lookup l
    ON l.거래구분 = '임차'
   AND s.지번주소 = l.지번주소
   AND s.단지명_공백제거 = l.단지명_공백제거
   AND s.건축년도 = l.건축년도;

CREATE OR REPLACE VIEW v_trade_feature_match_summary AS
WITH product_summary AS (
    SELECT
        거래구분,
        '전국공통상품성' AS 구분,
        전국상품성_매칭규칙 AS 세부구분,
        COUNT(*) AS 키수,
        SUM(거래건수) AS 거래건수
    FROM v_trade_complex_product_lookup
    GROUP BY 1, 2, 3
),
location_summary AS (
    SELECT
        거래구분,
        '수도권심화입지' AS 구분,
        수도권심화입지_매칭소스 AS 세부구분,
        COUNT(*) AS 키수,
        SUM(거래건수) AS 거래건수
    FROM v_trade_sudogwon_location_lookup
    GROUP BY 1, 2, 3
),
unioned AS (
    SELECT * FROM product_summary
    UNION ALL
    SELECT * FROM location_summary
),
totals AS (
    SELECT
        거래구분,
        구분,
        SUM(거래건수) AS 총거래건수
    FROM unioned
    GROUP BY 1, 2
)
SELECT
    u.거래구분,
    u.구분,
    u.세부구분,
    u.키수,
    u.거래건수,
    t.총거래건수,
    ROUND(100.0 * u.거래건수 / NULLIF(t.총거래건수, 0), 2) AS 거래건수비중_pct
FROM unioned u
JOIN totals t
    ON u.거래구분 = t.거래구분
   AND u.구분 = t.구분;

CREATE OR REPLACE VIEW v_jeonse_ratio_monthly AS
SELECT
    s.시도,
    s.시군구,
    s.읍면동,
    s.단지명_정규화,
    s.전용면적_구분,
    s.계약년월,
    s.거래건수 AS 매매거래건수,
    j.거래건수 AS 전세거래건수,
    s.매매대표가격_만원,
    j.전세대표보증금_만원,
    s.매매대표평당가_만원,
    j.전세대표평당가_만원,
    ROUND(100.0 * j.전세대표보증금_만원 / NULLIF(s.매매대표가격_만원, 0), 2) AS 전세가율_pct,
    ROUND(100.0 * j.전세대표평당가_만원 / NULLIF(s.매매대표평당가_만원, 0), 2) AS 평당전세가율_pct
FROM v_sale_monthly_metrics s
JOIN v_jeonse_monthly_metrics j
    ON s.시도 = j.시도
   AND s.시군구 = j.시군구
   AND COALESCE(s.읍면동, '') = COALESCE(j.읍면동, '')
   AND s.단지명_정규화 = j.단지명_정규화
   AND s.전용면적_구분 = j.전용면적_구분
   AND s.계약년월 = j.계약년월;

CREATE OR REPLACE VIEW v_sale_monthly_yoy AS
WITH prev AS (
    SELECT
        시도,
        시군구,
        읍면동,
        단지명_정규화,
        전용면적_구분,
        계약년월 + 100 AS 비교년월,
        매매대표가격_만원 AS 전년동월_매매대표가격_만원,
        매매대표평당가_만원 AS 전년동월_매매대표평당가_만원
    FROM v_sale_monthly_metrics
)
SELECT
    c.*,
    p.전년동월_매매대표가격_만원,
    p.전년동월_매매대표평당가_만원,
    ROUND(100.0 * (c.매매대표가격_만원 / NULLIF(p.전년동월_매매대표가격_만원, 0) - 1), 2) AS 매매대표가격_YoY_pct,
    ROUND(100.0 * (c.매매대표평당가_만원 / NULLIF(p.전년동월_매매대표평당가_만원, 0) - 1), 2) AS 매매대표평당가_YoY_pct
FROM v_sale_monthly_metrics c
LEFT JOIN prev p
    ON c.시도 = p.시도
   AND c.시군구 = p.시군구
   AND COALESCE(c.읍면동, '') = COALESCE(p.읍면동, '')
   AND c.단지명_정규화 = p.단지명_정규화
   AND c.전용면적_구분 = p.전용면적_구분
   AND c.계약년월 = p.비교년월;

CREATE OR REPLACE VIEW v_jeonse_monthly_yoy AS
WITH prev AS (
    SELECT
        시도,
        시군구,
        읍면동,
        단지명_정규화,
        전용면적_구분,
        계약년월 + 100 AS 비교년월,
        전세대표보증금_만원 AS 전년동월_전세대표보증금_만원,
        전세대표평당가_만원 AS 전년동월_전세대표평당가_만원
    FROM v_jeonse_monthly_metrics
)
SELECT
    c.*,
    p.전년동월_전세대표보증금_만원,
    p.전년동월_전세대표평당가_만원,
    ROUND(100.0 * (c.전세대표보증금_만원 / NULLIF(p.전년동월_전세대표보증금_만원, 0) - 1), 2) AS 전세대표보증금_YoY_pct,
    ROUND(100.0 * (c.전세대표평당가_만원 / NULLIF(p.전년동월_전세대표평당가_만원, 0) - 1), 2) AS 전세대표평당가_YoY_pct
FROM v_jeonse_monthly_metrics c
LEFT JOIN prev p
    ON c.시도 = p.시도
   AND c.시군구 = p.시군구
   AND COALESCE(c.읍면동, '') = COALESCE(p.읍면동, '')
   AND c.단지명_정규화 = p.단지명_정규화
   AND c.전용면적_구분 = p.전용면적_구분
   AND c.계약년월 = p.비교년월;

CREATE OR REPLACE VIEW v_wolse_monthly_yoy AS
WITH prev AS (
    SELECT
        시도,
        시군구,
        읍면동,
        단지명_정규화,
        전용면적_구분,
        계약년월 + 100 AS 비교년월,
        월세대표보증금_만원 AS 전년동월_월세대표보증금_만원,
        월세대표월세액_만원 AS 전년동월_월세대표월세액_만원
    FROM v_wolse_monthly_metrics
)
SELECT
    c.*,
    p.전년동월_월세대표보증금_만원,
    p.전년동월_월세대표월세액_만원,
    ROUND(100.0 * (c.월세대표보증금_만원 / NULLIF(p.전년동월_월세대표보증금_만원, 0) - 1), 2) AS 월세대표보증금_YoY_pct,
    ROUND(100.0 * (c.월세대표월세액_만원 / NULLIF(p.전년동월_월세대표월세액_만원, 0) - 1), 2) AS 월세대표월세액_YoY_pct
FROM v_wolse_monthly_metrics c
LEFT JOIN prev p
    ON c.시도 = p.시도
   AND c.시군구 = p.시군구
   AND COALESCE(c.읍면동, '') = COALESCE(p.읍면동, '')
   AND c.단지명_정규화 = p.단지명_정규화
   AND c.전용면적_구분 = p.전용면적_구분
   AND c.계약년월 = p.비교년월;

CREATE OR REPLACE VIEW v_lease_conversion_mix_monthly AS
SELECT
    시도,
    시군구_분리 AS 시군구,
    읍면동,
    단지명_공백제거 AS 단지명_정규화,
    전용면적_구분,
    계약년월,
    COUNT(*) FILTER (WHERE 전월세구분 = '전세') AS 전세거래건수,
    COUNT(*) FILTER (WHERE 전월세구분 = '월세') AS 월세거래건수,
    COUNT(*) AS 전체거래건수,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE 전월세구분 = '월세') / NULLIF(COUNT(*), 0),
        2
    ) AS 월세비중_pct,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE 전월세구분 = '전세') / NULLIF(COUNT(*), 0),
        2
    ) AS 전세비중_pct
FROM v_lease_clean
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE OR REPLACE VIEW v_sale_same_area_peer_base_12m AS
WITH params AS (
    SELECT
        최신계약년월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 11 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 기준시작년월
    FROM v_params
),
complex_base AS (
    SELECT
        COALESCE(CAST(단지고유번호 AS VARCHAR), 단지기본키) AS 비교단지키,
        시도,
        시군구_분리 AS 시군구,
        읍면동,
        지번주소,
        단지명_공백제거,
        전용면적_구분,
        COALESCE(기준연식_구분, 연식_구분, '미상') AS 비교연식구분,
        COALESCE(세대수_구간, '미상') AS 비교세대수구간,
        MEDIAN(거래금액_만원 * 3.3 / NULLIF(전용면적_㎡, 0)) AS 단지대표평당가_만원,
        COUNT(*) AS 거래건수
    FROM v_sale_enriched_base
    CROSS JOIN params p
    WHERE 계약년월 BETWEEN p.기준시작년월 AND p.최신계약년월
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
peer_base AS (
    SELECT
        시도,
        시군구,
        읍면동,
        전용면적_구분,
        비교연식구분,
        비교세대수구간,
        MEDIAN(단지대표평당가_만원) AS 생활권동급_대표평당가_만원,
        COUNT(*) AS 비교단지수
    FROM complex_base
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
    c.*,
    p.생활권동급_대표평당가_만원,
    p.비교단지수
FROM complex_base c
LEFT JOIN peer_base p
    ON c.시도 = p.시도
   AND c.시군구 = p.시군구
   AND COALESCE(c.읍면동, '') = COALESCE(p.읍면동, '')
   AND c.전용면적_구분 = p.전용면적_구분
   AND c.비교연식구분 = p.비교연식구분
   AND c.비교세대수구간 = p.비교세대수구간;

CREATE OR REPLACE VIEW v_sale_conditional_signal_12m AS
SELECT
    *,
    ROUND(
        100.0 * (단지대표평당가_만원 / NULLIF(생활권동급_대표평당가_만원, 0) - 1),
        2
    ) AS 생활권동급_괴리율_pct,
    CASE
        WHEN 비교단지수 >= 5
         AND 거래건수 >= 5
         AND 단지대표평당가_만원 / NULLIF(생활권동급_대표평당가_만원, 0) <= 0.90
        THEN '저평가가능'
        WHEN 비교단지수 >= 5
         AND 거래건수 >= 5
         AND 단지대표평당가_만원 / NULLIF(생활권동급_대표평당가_만원, 0) >= 1.10
        THEN '과대반영가능'
        ELSE '중립'
    END AS 조건부가격신호
FROM v_sale_same_area_peer_base_12m;

CREATE OR REPLACE VIEW v_sale_complex_ref_sudogwon AS
WITH base AS (
    SELECT
        COALESCE(CAST(단지고유번호 AS VARCHAR), 단지기본키) AS 실거래비교단지키,
        시도,
        시군구_분리 AS 시군구,
        읍면동,
        지번주소,
        단지명_공백제거,
        조인_단지명,
        MAX(COALESCE(보강_세대수, 0)) AS 세대수,
        MAX(COALESCE(보강_동수, 0)) AS 동수,
        MAX(사용승인년월) AS 사용승인년월
    FROM v_sale_enriched_base
    WHERE 시도 IN ('서울특별시', '경기도', '인천광역시')
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
SELECT
    *,
    COUNT(*) OVER (
        PARTITION BY 지번주소, 단지명_공백제거
    ) AS 정확단지명_복합후보수,
    COUNT(*) OVER (
        PARTITION BY 지번주소, 조인_단지명
    ) AS 조인단지명_복합후보수,
    COUNT(*) OVER (
        PARTITION BY 지번주소
    ) AS 주소기반_복합후보수
FROM base;

CREATE OR REPLACE VIEW v_naver_listing_snapshot_base AS
WITH latest_snapshot AS (
    SELECT
        시도,
        거래유형,
        MAX(수집일자) AS 최신수집일자
    FROM 네이버부동산_매물
    WHERE 거래유형 IN ('매매', '전세')
    GROUP BY 1, 2
)
SELECT
    m.*,
    TRIM(COALESCE(m.지번주소, '')) AS 정규화_지번주소,
    TRIM(REGEXP_REPLACE(COALESCE(m.단지명, ''), '\\s+', ' ', 'g')) AS 정규화_단지명,
    TRIM(REGEXP_REPLACE(COALESCE(m.단지명, ''), '\\s+', '', 'g')) AS 단지명_공백제거,
    TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(COALESCE(m.단지명, ''), '\\([^)]*\\)', '', 'g'),
            '\\s+',
            ' ',
            'g'
        )
    ) AS 조인_단지명,
    CASE
        WHEN m.전용면적 <= 40 THEN '초소형'
        WHEN m.전용면적 <= 60 THEN '소형'
        WHEN m.전용면적 <= 85 THEN '중소형'
        WHEN m.전용면적 <= 135 THEN '중대형'
        ELSE '대형'
    END AS 전용면적_구분,
    CAST(ROUND(m.전용면적) AS INTEGER) AS 전용면적_반올림_㎡,
    CASE
        WHEN COALESCE(TRIM(m.급매_태그), '') = '급매'
          OR COALESCE(m.태그, '') LIKE '%급매%'
          OR COALESCE(m.매물특징, '') LIKE '%급매%'
        THEN 1
        ELSE 0
    END AS 급매태그여부,
    s.최신수집일자
FROM 네이버부동산_매물 m
JOIN latest_snapshot s
    ON m.시도 = s.시도
   AND m.거래유형 = s.거래유형
   AND m.수집일자 = s.최신수집일자
WHERE m.거래유형 IN ('매매', '전세');

CREATE OR REPLACE VIEW v_naver_listing_match_lookup AS
WITH exact_ref AS (
    SELECT *
    FROM v_sale_complex_ref_sudogwon
    WHERE 정확단지명_복합후보수 = 1
),
join_ref AS (
    SELECT *
    FROM v_sale_complex_ref_sudogwon
    WHERE 조인단지명_복합후보수 = 1
),
addr_ref AS (
    SELECT *
    FROM v_sale_complex_ref_sudogwon
    WHERE 주소기반_복합후보수 = 1
),
resolved AS (
    SELECT
        l.*,
        CASE
            WHEN e.실거래비교단지키 IS NOT NULL THEN '주소+정확단지명'
            WHEN j.실거래비교단지키 IS NOT NULL THEN '주소+조인단지명'
            WHEN a.실거래비교단지키 IS NOT NULL THEN '주소유일'
            ELSE '미매칭'
        END AS 호가실거래_매칭규칙,
        COALESCE(e.실거래비교단지키, j.실거래비교단지키, a.실거래비교단지키) AS 실거래비교단지키,
        COALESCE(e.세대수, j.세대수, a.세대수) AS 실거래보강_세대수,
        COALESCE(e.동수, j.동수, a.동수) AS 실거래보강_동수,
        COALESCE(e.사용승인년월, j.사용승인년월, a.사용승인년월) AS 실거래보강_사용승인년월
    FROM v_naver_listing_snapshot_base l
    LEFT JOIN exact_ref e
        ON l.정규화_지번주소 = e.지번주소
       AND l.단지명_공백제거 = e.단지명_공백제거
    LEFT JOIN join_ref j
        ON l.정규화_지번주소 = j.지번주소
       AND l.조인_단지명 = j.조인_단지명
       AND e.실거래비교단지키 IS NULL
    LEFT JOIN addr_ref a
        ON l.정규화_지번주소 = a.지번주소
       AND e.실거래비교단지키 IS NULL
       AND j.실거래비교단지키 IS NULL
)
SELECT
    *,
    CASE WHEN 실거래비교단지키 IS NOT NULL THEN 1 ELSE 0 END AS 호가실거래_매칭여부
FROM resolved;

CREATE OR REPLACE VIEW v_naver_listing_match_summary AS
WITH agg AS (
    SELECT
        거래유형,
        호가실거래_매칭규칙,
        COUNT(*) AS 매물건수,
        COUNT(DISTINCT 매물번호) AS 고유매물수
    FROM v_naver_listing_match_lookup
    GROUP BY 1, 2
),
totals AS (
    SELECT
        거래유형,
        SUM(매물건수) AS 총매물건수
    FROM agg
    GROUP BY 1
)
SELECT
    a.거래유형,
    a.호가실거래_매칭규칙,
    a.매물건수,
    a.고유매물수,
    t.총매물건수,
    ROUND(100.0 * a.매물건수 / NULLIF(t.총매물건수, 0), 2) AS 매물건수비중_pct
FROM agg a
JOIN totals t
    ON a.거래유형 = t.거래유형;

CREATE OR REPLACE VIEW v_sale_recent_reference_area_12m AS
WITH params AS (
    SELECT
        최신계약년월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 2 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 시작3개월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 5 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 시작6개월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 11 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 시작12개월
    FROM v_params
),
base AS (
    SELECT
        COALESCE(CAST(단지고유번호 AS VARCHAR), 단지기본키) AS 실거래비교단지키,
        시도,
        시군구_분리 AS 시군구,
        읍면동,
        지번주소,
        단지명_공백제거,
        전용면적_구분,
        CAST(ROUND(전용면적_㎡) AS INTEGER) AS 전용면적_반올림_㎡,
        계약년월,
        TRY_CAST(NULLIF(TRIM(CAST(계약일 AS VARCHAR)), '') AS INTEGER) AS 계약일_정수,
        거래금액_만원
    FROM v_sale_enriched_base
    CROSS JOIN params p
    WHERE 시도 IN ('서울특별시', '경기도', '인천광역시')
      AND 계약년월 BETWEEN p.시작12개월 AND p.최신계약년월
),
agg AS (
    SELECT
        b.실거래비교단지키,
        b.시도,
        b.시군구,
        b.읍면동,
        b.지번주소,
        b.단지명_공백제거,
        b.전용면적_구분,
        b.전용면적_반올림_㎡,
        COUNT(*) FILTER (WHERE b.계약년월 BETWEEN p.시작3개월 AND p.최신계약년월) AS 최근3개월거래건수,
        COUNT(*) FILTER (WHERE b.계약년월 BETWEEN p.시작6개월 AND p.최신계약년월) AS 최근6개월거래건수,
        COUNT(*) FILTER (WHERE b.계약년월 BETWEEN p.시작12개월 AND p.최신계약년월) AS 최근12개월거래건수,
        MEDIAN(b.거래금액_만원) FILTER (WHERE b.계약년월 BETWEEN p.시작3개월 AND p.최신계약년월) AS 최근3개월중앙실거래가_만원,
        MEDIAN(b.거래금액_만원) FILTER (WHERE b.계약년월 BETWEEN p.시작6개월 AND p.최신계약년월) AS 최근6개월중앙실거래가_만원,
        MEDIAN(b.거래금액_만원) FILTER (WHERE b.계약년월 BETWEEN p.시작12개월 AND p.최신계약년월) AS 최근12개월중앙실거래가_만원
    FROM base b
    CROSS JOIN params p
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
latest_tx AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 실거래비교단지키, 전용면적_반올림_㎡
            ORDER BY 계약년월 DESC, 계약일_정수 DESC NULLS LAST, 거래금액_만원 DESC
        ) AS rn
    FROM base
)
SELECT
    a.*,
    l.거래금액_만원 AS 최근실거래가_만원,
    l.계약년월 AS 최근실거래_계약년월,
    l.계약일_정수 AS 최근실거래_계약일
FROM agg a
LEFT JOIN latest_tx l
    ON a.실거래비교단지키 = l.실거래비교단지키
   AND a.전용면적_반올림_㎡ = l.전용면적_반올림_㎡
   AND l.rn = 1;

CREATE OR REPLACE VIEW v_sale_recent_reference_band_12m AS
WITH params AS (
    SELECT
        최신계약년월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 2 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 시작3개월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 5 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 시작6개월,
        CAST(
            STRFTIME(
                TRY_STRPTIME(CAST(최신계약년월 AS VARCHAR) || '01', '%Y%m%d') - INTERVAL 11 MONTH,
                '%Y%m'
            ) AS BIGINT
        ) AS 시작12개월
    FROM v_params
),
base AS (
    SELECT
        COALESCE(CAST(단지고유번호 AS VARCHAR), 단지기본키) AS 실거래비교단지키,
        시도,
        시군구_분리 AS 시군구,
        읍면동,
        지번주소,
        단지명_공백제거,
        전용면적_구분,
        계약년월,
        TRY_CAST(NULLIF(TRIM(CAST(계약일 AS VARCHAR)), '') AS INTEGER) AS 계약일_정수,
        거래금액_만원
    FROM v_sale_enriched_base
    CROSS JOIN params p
    WHERE 시도 IN ('서울특별시', '경기도', '인천광역시')
      AND 계약년월 BETWEEN p.시작12개월 AND p.최신계약년월
),
agg AS (
    SELECT
        b.실거래비교단지키,
        b.시도,
        b.시군구,
        b.읍면동,
        b.지번주소,
        b.단지명_공백제거,
        b.전용면적_구분,
        COUNT(*) FILTER (WHERE b.계약년월 BETWEEN p.시작3개월 AND p.최신계약년월) AS 최근3개월거래건수,
        COUNT(*) FILTER (WHERE b.계약년월 BETWEEN p.시작6개월 AND p.최신계약년월) AS 최근6개월거래건수,
        COUNT(*) FILTER (WHERE b.계약년월 BETWEEN p.시작12개월 AND p.최신계약년월) AS 최근12개월거래건수,
        MEDIAN(b.거래금액_만원) FILTER (WHERE b.계약년월 BETWEEN p.시작3개월 AND p.최신계약년월) AS 최근3개월중앙실거래가_만원,
        MEDIAN(b.거래금액_만원) FILTER (WHERE b.계약년월 BETWEEN p.시작6개월 AND p.최신계약년월) AS 최근6개월중앙실거래가_만원,
        MEDIAN(b.거래금액_만원) FILTER (WHERE b.계약년월 BETWEEN p.시작12개월 AND p.최신계약년월) AS 최근12개월중앙실거래가_만원
    FROM base b
    CROSS JOIN params p
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
latest_tx AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 실거래비교단지키, 전용면적_구분
            ORDER BY 계약년월 DESC, 계약일_정수 DESC NULLS LAST, 거래금액_만원 DESC
        ) AS rn
    FROM base
)
SELECT
    a.*,
    l.거래금액_만원 AS 최근실거래가_만원,
    l.계약년월 AS 최근실거래_계약년월,
    l.계약일_정수 AS 최근실거래_계약일
FROM agg a
LEFT JOIN latest_tx l
    ON a.실거래비교단지키 = l.실거래비교단지키
   AND a.전용면적_구분 = l.전용면적_구분
   AND l.rn = 1;

CREATE OR REPLACE VIEW v_naver_sale_listing_vs_actual_latest AS
WITH base AS (
    SELECT
        l.*,
        area.최근실거래가_만원 AS 면적기준_최근실거래가_만원,
        area.최근실거래_계약년월 AS 면적기준_최근실거래_계약년월,
        area.최근3개월거래건수 AS 면적기준_최근3개월거래건수,
        area.최근6개월거래건수 AS 면적기준_최근6개월거래건수,
        area.최근12개월거래건수 AS 면적기준_최근12개월거래건수,
        area.최근3개월중앙실거래가_만원 AS 면적기준_최근3개월중앙실거래가_만원,
        area.최근6개월중앙실거래가_만원 AS 면적기준_최근6개월중앙실거래가_만원,
        area.최근12개월중앙실거래가_만원 AS 면적기준_최근12개월중앙실거래가_만원,
        band.최근실거래가_만원 AS 구간기준_최근실거래가_만원,
        band.최근실거래_계약년월 AS 구간기준_최근실거래_계약년월,
        band.최근3개월거래건수 AS 구간기준_최근3개월거래건수,
        band.최근6개월거래건수 AS 구간기준_최근6개월거래건수,
        band.최근12개월거래건수 AS 구간기준_최근12개월거래건수,
        band.최근3개월중앙실거래가_만원 AS 구간기준_최근3개월중앙실거래가_만원,
        band.최근6개월중앙실거래가_만원 AS 구간기준_최근6개월중앙실거래가_만원,
        band.최근12개월중앙실거래가_만원 AS 구간기준_최근12개월중앙실거래가_만원,
        COALESCE(
            area.최근6개월중앙실거래가_만원,
            area.최근실거래가_만원,
            band.최근6개월중앙실거래가_만원,
            band.최근실거래가_만원,
            area.최근12개월중앙실거래가_만원,
            band.최근12개월중앙실거래가_만원
        ) AS 급매판정기준가격_만원
    FROM v_naver_listing_match_lookup l
    LEFT JOIN v_sale_recent_reference_area_12m area
        ON l.실거래비교단지키 = area.실거래비교단지키
       AND l.전용면적_반올림_㎡ = area.전용면적_반올림_㎡
    LEFT JOIN v_sale_recent_reference_band_12m band
        ON l.실거래비교단지키 = band.실거래비교단지키
       AND l.전용면적_구분 = band.전용면적_구분
    WHERE l.거래유형 = '매매'
)
SELECT
    *,
    ROUND(
        100.0 * (가격_만원 / NULLIF(급매판정기준가격_만원, 0) - 1),
        2
    ) AS 기준가격대비_호가괴리율_pct,
    ROUND(
        100.0 * (1 - 가격_만원 / NULLIF(급매판정기준가격_만원, 0)),
        2
    ) AS 기준가격대비_호가할인율_pct,
    CASE
        WHEN 급매판정기준가격_만원 IS NULL THEN '판정불가'
        WHEN 100.0 * (1 - 가격_만원 / NULLIF(급매판정기준가격_만원, 0)) >= 10 THEN '강한가격기준급매'
        WHEN 100.0 * (1 - 가격_만원 / NULLIF(급매판정기준가격_만원, 0)) >= 5 THEN '가격기준급매'
        WHEN 급매태그여부 = 1 THEN '태그만급매'
        ELSE '일반'
    END AS 급매판정
FROM base;

CREATE OR REPLACE VIEW v_naver_listing_stock_summary_latest AS
SELECT
    거래유형,
    시도,
    시군구,
    법정동,
    COALESCE(실거래비교단지키, 정규화_지번주소 || '|' || 단지명_공백제거) AS 매물요약키,
    실거래비교단지키,
    정규화_지번주소 AS 지번주소,
    단지명_공백제거,
    전용면적_구분,
    COUNT(*) AS 매물건수,
    COUNT(DISTINCT 매물번호) AS 고유매물건수,
    MIN(가격_만원) AS 최저호가_만원,
    MEDIAN(가격_만원) AS 중간호가_만원,
    MAX(가격_만원) AS 최고호가_만원,
    SUM(급매태그여부) AS 급매태그매물수,
    ROUND(100.0 * SUM(급매태그여부) / NULLIF(COUNT(*), 0), 2) AS 급매태그비중_pct,
    MAX(최신수집일자) AS 기준수집일자
FROM v_naver_listing_match_lookup
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9;

CREATE OR REPLACE VIEW v_naver_sale_listing_summary_latest AS
SELECT
    시도,
    시군구,
    법정동,
    COALESCE(실거래비교단지키, 정규화_지번주소 || '|' || 단지명_공백제거) AS 매물요약키,
    실거래비교단지키,
    정규화_지번주소 AS 지번주소,
    단지명_공백제거,
    전용면적_구분,
    COUNT(*) AS 매매매물건수,
    COUNT(DISTINCT 매물번호) AS 고유매물건수,
    MIN(가격_만원) AS 최저호가_만원,
    MEDIAN(가격_만원) AS 중간호가_만원,
    MAX(가격_만원) AS 최고호가_만원,
    SUM(급매태그여부) AS 급매태그매물수,
    COUNT(*) FILTER (WHERE 급매판정 = '가격기준급매') AS 가격기준급매매물수,
    COUNT(*) FILTER (WHERE 급매판정 = '강한가격기준급매') AS 강한가격기준급매매물수,
    COUNT(*) FILTER (WHERE 급매판정 = '태그만급매') AS 태그만급매매물수,
    ROUND(MEDIAN(기준가격대비_호가괴리율_pct), 2) AS 중간호가괴리율_pct,
    ROUND(MAX(기준가격대비_호가할인율_pct), 2) AS 최대호가할인율_pct,
    MAX(최신수집일자) AS 기준수집일자
FROM v_naver_sale_listing_vs_actual_latest
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;
