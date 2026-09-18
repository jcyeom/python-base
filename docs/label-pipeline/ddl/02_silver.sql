-- =====================================================================
-- L1 : Silver - 후보 라벨 (파싱·검증 완료)
-- 설계서 02-design.md §4.2, 논문 표 2
--
-- 원칙 P1: append-only. 정정은 새 run_id로.
-- 원칙 P3: 기권(is_abstain)은 값 영역과 분리.
-- =====================================================================

CREATE TABLE IF NOT EXISTS silver_label_candidate (
    label_id            STRING    NOT NULL COMMENT 'UUIDv7, 표면 키',

    -- ---- 식별 ------------------------------------------------------
    sample_id           STRING    NOT NULL COMMENT 'feature 테이블 조인 키',
    label_task          STRING    NOT NULL COMMENT '한 샘플에 복수 태스크 공존 가능',
    target_ref          STRUCT<
                            kind : STRING,   -- whole | span | bbox | cell
                            start: BIGINT,
                            `end`: BIGINT,
                            x    : DOUBLE, y: DOUBLE, w: DOUBLE, h: DOUBLE,
                            key  : STRING
                        >                    COMMENT '라벨 적용 범위',
    target_ref_hash     STRING    NOT NULL   COMMENT 'canon(target_ref)의 sha256. PK 구성',
    ontology_ver        STRING    NOT NULL   COMMENT '클래스 체계 버전 (§6)',

    -- ---- 값 --------------------------------------------------------
    value_type          STRING    NOT NULL   COMMENT 'class | numeric | multilabel',
    value_class         STRING,
    value_num           DOUBLE,
    value_json          STRING              COMMENT 'multilabel/구조화 출력',
    is_abstain          BOOLEAN   NOT NULL   COMMENT '판단 유보. 부정 라벨과 구분 (P3)',

    -- ---- 라벨러 ----------------------------------------------------
    method              STRING    NOT NULL   COMMENT 'rule | llm | human | hybrid',
    method_ver          STRING    NOT NULL   COMMENT 'alias 금지. 날짜 고정 ID (§5.2)',
    labeler_id          STRING    NOT NULL,
    prompt_hash         STRING              COMMENT 'LLM: 렌더링된 최종 프롬프트',
    params_hash         STRING              COMMENT 'LLM: temperature/top_p/seed/...',
    retrieval_ctx_hash  STRING              COMMENT 'RAG 사용 시 검색 컨텍스트',

    -- ---- 신뢰도 ----------------------------------------------------
    confidence_raw      DOUBLE              COMMENT '라벨러 원 보고값. 척도는 라벨러별 상이',
    confidence          DOUBLE              COMMENT '앵커 기반 보정값 ∈ [0,1] (§8)',
    calibration_model_ver STRING,

    -- ---- 근거 ------------------------------------------------------
    rationale_ref       STRING              COMMENT 'silver_label_rationale.rationale_id',

    -- ---- 재생 ------------------------------------------------------
    inputs_hash         STRING    NOT NULL   COMMENT 'canon(input_spec 투영)의 sha256',
    feature_snapshot_id STRING    NOT NULL   COMMENT 'feature 테이블 버전/스냅샷 (§5.3)',
    raw_response_ref    STRING              COMMENT 'bronze_label_raw.raw_id',

    -- ---- 실행 ------------------------------------------------------
    run_id              STRING    NOT NULL   COMMENT '멱등성 키 (§12.1)',
    labeled_at          TIMESTAMP NOT NULL,
    cost_usd            DOUBLE,
    latency_ms          BIGINT,
    token_usage         MAP<STRING, BIGINT>,

    parse_status        STRING    NOT NULL   COMMENT 'ok | repaired | failed',
    schema_ver          STRING    NOT NULL   COMMENT 'L1 스키마 자체의 버전'
)
USING DELTA
PARTITIONED BY (labeled_date DATE GENERATED ALWAYS AS (CAST(labeled_at AS DATE)), method)
TBLPROPERTIES (
    'delta.logRetentionDuration'          = 'interval 730 days',
    'delta.deletedFileRetentionDuration'  = 'interval 730 days',
    'delta.autoOptimize.optimizeWrite'    = 'true',
    'labelpipe.layer'                     = 'silver',
    'labelpipe.immutable'                 = 'true',
    'labelpipe.pk'                        = 'sample_id,label_task,target_ref_hash,labeler_id,run_id'
);

-- 조인 패턴이 sample_id 기준이므로 클러스터링 (일 1회 compaction과 함께)
--   OPTIMIZE silver_label_candidate ZORDER BY (sample_id);

-- ---------------------------------------------------------------------
-- 근거 텍스트 분리 저장 (§13.2)
-- rationale이 L1 부피의 대부분을 차지하며 합의 연산에서는 읽히지 않는다.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_label_rationale (
    rationale_id   STRING    NOT NULL COMMENT 'UUIDv7',
    label_id       STRING    NOT NULL,
    run_id         STRING    NOT NULL,
    labeler_id     STRING    NOT NULL,
    kind           STRING    NOT NULL COMMENT 'rule_name | llm_explanation | reviewer_comment',
    text           STRING,
    created_at     TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (created_date DATE GENERATED ALWAYS AS (CAST(created_at AS DATE)))
TBLPROPERTIES (
    'delta.logRetentionDuration'         = 'interval 730 days',
    'delta.deletedFileRetentionDuration' = 'interval 730 days',
    'labelpipe.layer'                    = 'silver',
    'labelpipe.immutable'                = 'true',
    'labelpipe.pii'                      = 'possible'
);

-- ---------------------------------------------------------------------
-- 최신 후보 뷰 (§7.1)
-- append-only 테이블에서 라벨러별 최신 1건만 노출. 실패 파싱 제외.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_l1_latest AS
SELECT * EXCEPT (rn) FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY sample_id, label_task, target_ref_hash, labeler_id
               ORDER BY labeled_at DESC, run_id DESC
           ) AS rn
    FROM silver_label_candidate
    WHERE parse_status <> 'failed'
) WHERE rn = 1;
