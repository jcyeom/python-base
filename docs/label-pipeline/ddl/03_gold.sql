-- =====================================================================
-- L2 / L3 : Gold - 합의 라벨 및 검증 라벨
-- 설계서 02-design.md §4.3, §4.4, §4.5
-- =====================================================================

-- ---------------------------------------------------------------------
-- L2 합의 라벨
--   개정 A4: pending_review에서도 value를 비우지 않는다. 학습 포함 여부는
--            dataset_manifest.filter_predicate가 결정한다 (P4).
--   개정 A8: agreement 필드는 L1이 아니라 여기에 위치한다.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_label_consensus (
    sample_id            STRING    NOT NULL,
    label_task           STRING    NOT NULL,
    target_ref_hash      STRING    NOT NULL,
    ontology_ver         STRING    NOT NULL,

    value_type           STRING    NOT NULL,
    value_class          STRING,
    value_num            DOUBLE,
    value_json           STRING,
    soft_dist            MAP<STRING, DOUBLE> COMMENT '클래스별 사후 점수',
    margin               DOUBLE             COMMENT '1위 − 2위 점수 차',

    flag                 STRING    NOT NULL
        COMMENT 'agreed | soft_disagreement | pending_review | no_signal | human_final',

    agreement            STRUCT<
                             n_candidates : INT,
                             n_abstain    : INT,
                             unanimous    : BOOLEAN,
                             per_labeler  : ARRAY<STRUCT<
                                                labeler_id : STRING,
                                                value_class: STRING,
                                                conf_raw   : DOUBLE,
                                                conf_cal   : DOUBLE,
                                                weight     : DOUBLE>>,
                             pairwise_agree: MAP<STRING, DOUBLE>
                         >,

    -- ---- 재생 메타 (개정 B11) --------------------------------------
    fusion_policy_ver     STRING   NOT NULL,
    theta                 DOUBLE   NOT NULL,
    calibration_model_ver STRING,
    correlation_model_ver STRING,
    input_l1_run_ids      ARRAY<STRING> NOT NULL,
    input_l1_label_ids    ARRAY<STRING> NOT NULL COMMENT '정밀 재생용. 참조 무결성 검사 대상',

    l2_version            BIGINT   NOT NULL COMMENT 'republish 시 증가. 덮어쓰기 금지 (§10.4)',
    fused_at              TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (label_task, l2_version)
TBLPROPERTIES (
    'delta.logRetentionDuration'         = 'interval 730 days',
    'delta.deletedFileRetentionDuration' = 'interval 730 days',
    'labelpipe.layer'                    = 'gold',
    'labelpipe.pk'                       = 'sample_id,label_task,target_ref_hash,l2_version'
);

-- ---------------------------------------------------------------------
-- L3 검증 라벨
--   개정 A6: l3_purpose로 train / eval_holdout을 사전 분할한다.
--   개정 A5: source로 불일치 기원과 무작위 앵커 기원을 구분한다.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_label_verified (
    verified_id       STRING    NOT NULL COMMENT 'UUIDv7',

    sample_id         STRING    NOT NULL,
    label_task        STRING    NOT NULL,
    target_ref_hash   STRING    NOT NULL,
    ontology_ver      STRING    NOT NULL,

    value_type        STRING    NOT NULL,
    value_class       STRING,
    value_num         DOUBLE,
    value_json        STRING,

    reviewer_id       STRING    NOT NULL COMMENT '가명 ID. 원본 매핑은 별도 제한 테이블 (§14.2)',
    review_round      INT       NOT NULL DEFAULT 1,
    adjudicated       BOOLEAN   NOT NULL DEFAULT false,

    l3_purpose        STRING    NOT NULL
        COMMENT 'train | eval_holdout. hash(sample_id||label_task)로 결정론적 배정 (§4.4)',
    source            STRING    NOT NULL
        COMMENT 'disagreement | random_anchor | escalation',
    anchor_set_id     STRING             COMMENT 'source=random_anchor인 경우',

    review_cost       DOUBLE,
    review_seconds    BIGINT,
    reviewer_comment_ref STRING          COMMENT 'silver_label_rationale 참조',

    l3_version        BIGINT    NOT NULL,
    reviewed_at       TIMESTAMP NOT NULL
)
USING DELTA
PARTITIONED BY (label_task, l3_purpose)
TBLPROPERTIES (
    'delta.logRetentionDuration'         = 'interval 730 days',
    'delta.deletedFileRetentionDuration' = 'interval 730 days',
    'labelpipe.layer'                    = 'gold'
);

-- ---------------------------------------------------------------------
-- 누수 방지 뷰 (§4.5)
--   Fusion Engine과 Calibrator는 v_l3_train만 참조한다.
--   평가·드리프트는 v_l3_holdout만 참조한다.
--   두 뷰를 동시에 참조하는 코드 경로는 CI 정적 검사로 금지 (§14.4).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW v_l3_train AS
SELECT * FROM gold_label_verified WHERE l3_purpose = 'train';

CREATE OR REPLACE VIEW v_l3_holdout AS
SELECT * FROM gold_label_verified WHERE l3_purpose = 'eval_holdout';

-- 보정 학습용 뷰: 불일치 기원 L3는 기본 제외 (§8.3 앵커 편향 보정)
CREATE OR REPLACE VIEW v_l3_calibration AS
SELECT * FROM gold_label_verified
WHERE l3_purpose = 'train' AND source = 'random_anchor';
