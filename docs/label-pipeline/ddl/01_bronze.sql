-- =====================================================================
-- L0 : Bronze - 라벨러 원시 응답 (파싱 전)
-- 설계서 02-design.md §4.1
--
-- 방언: Spark SQL + Delta Lake. Iceberg 사용 시 USING/TBLPROPERTIES만 교체.
-- 원칙 P1: append-only. UPDATE / DELETE 금지.
-- =====================================================================

CREATE TABLE IF NOT EXISTS bronze_label_raw (
    raw_id            STRING    NOT NULL COMMENT 'UUIDv7. L1.raw_response_ref가 참조',
    run_id            STRING    NOT NULL COMMENT '라벨링 실행 식별자',
    labeler_id        STRING    NOT NULL,
    method            STRING    NOT NULL COMMENT 'rule|llm|human|hybrid',
    method_ver        STRING    NOT NULL COMMENT 'LLM은 날짜 고정 ID만 허용 (§5.2)',

    sample_id         STRING    NOT NULL,
    label_task        STRING    NOT NULL,
    target_ref_hash   STRING    NOT NULL,

    -- 전송/수신 원문. PII 마스킹 적용 후 적재 (§14.1)
    request_payload   STRING             COMMENT '렌더링된 프롬프트 또는 규칙 입력 전문',
    response_payload  STRING             COMMENT '응답 전문. 파싱 실패해도 그대로 보존',
    response_meta     MAP<STRING, STRING>
                                         COMMENT 'http status, 모델 ID 에코, finish_reason 등',

    http_status       INT,
    error             STRING             COMMENT '실패 시 예외 메시지. 실패도 관측 대상',

    cost_usd          DOUBLE,
    latency_ms        BIGINT,
    token_usage       MAP<STRING, BIGINT> COMMENT 'prompt/completion/total',

    masking_ver       STRING             COMMENT 'PII 마스킹 규칙 버전',
    created_at        TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (created_date DATE GENERATED ALWAYS AS (CAST(created_at AS DATE)), labeler_id)
TBLPROPERTIES (
    'delta.logRetentionDuration'         = 'interval 730 days',
    'delta.deletedFileRetentionDuration' = 'interval 730 days',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'labelpipe.layer'                    = 'bronze',
    'labelpipe.immutable'                = 'true'
);

-- 보존 정책 (§13.3): 180일 경과분은 cold tier로 이관. 삭제하지 않는다.
