-- =====================================================================
-- Meta : 라벨러 레지스트리, 정책, 보정, 앵커, 검수 큐, 드리프트, 매니페스트
-- 설계서 02-design.md §3, §8~§11
-- =====================================================================

-- ---------------------------------------------------------------------
-- 라벨러 레지스트리 (SCD2)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS labeler_registry (
    labeler_id            STRING    NOT NULL,
    method                STRING    NOT NULL COMMENT 'rule | llm | human | hybrid',
    method_ver            STRING    NOT NULL COMMENT 'LLM: 날짜 고정 ID. alias는 CI에서 거부',
    label_task            STRING    NOT NULL,
    ontology_ver          STRING    NOT NULL,
    input_spec            ARRAY<STRING> NOT NULL COMMENT 'inputs_hash 대상 컬럼 (§5.2)',
    prompt_template_hash  STRING             COMMENT 'LLM 프롬프트 템플릿 원문 해시',
    params_hash           STRING,
    masking_ver           STRING,
    data_egress           STRING    NOT NULL COMMENT 'internal | external (§14.3)',
    base_model_family     STRING             COMMENT '상관 보정의 사전 정보 (§7.3)',
    enabled               BOOLEAN   NOT NULL,
    valid_from            TIMESTAMP NOT NULL,
    valid_to              TIMESTAMP           COMMENT 'NULL이면 현재 유효'
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta');

-- ---------------------------------------------------------------------
-- 합의 정책 (설정 파일 해시를 버전에 반영, §16)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fusion_policy (
    fusion_policy_ver  STRING    NOT NULL,
    label_task         STRING    NOT NULL,
    policy_kind        STRING    NOT NULL COMMENT 'majority | weighted | generative',
    theta              DOUBLE    NOT NULL,
    routing_weights    MAP<STRING, DOUBLE> COMMENT 'w1..w4 (§9.3)',
    config_hash        STRING    NOT NULL COMMENT 'canon(설정 전체)의 sha256',
    config_yaml        STRING    NOT NULL COMMENT '적용된 설정 원문 보존',
    created_at         TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta', 'labelpipe.immutable' = 'true');

-- ---------------------------------------------------------------------
-- 신뢰도 보정 모델 (§8)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS calibration_model (
    calibration_model_ver STRING    NOT NULL,
    labeler_id            STRING    NOT NULL,
    label_task            STRING    NOT NULL,
    kind                  STRING    NOT NULL COMMENT 'isotonic | platt | identity',
    n_train               BIGINT    NOT NULL COMMENT 'n_min 미만이면 kind=identity (부트스트랩)',
    knots                 ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>> COMMENT 'isotonic 구간점',
    platt_a               DOUBLE,
    platt_b               DOUBLE,
    fit_source_view       STRING    NOT NULL COMMENT '항상 v_l3_calibration (누수 방지)',
    brier_before          DOUBLE,
    brier_after           DOUBLE,
    fitted_at             TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta', 'labelpipe.immutable' = 'true');

-- 라벨러 상관 모델 (§7.3)
CREATE TABLE IF NOT EXISTS correlation_model (
    correlation_model_ver STRING    NOT NULL,
    label_task            STRING    NOT NULL,
    labeler_a             STRING    NOT NULL,
    labeler_b             STRING    NOT NULL,
    rho                   DOUBLE    NOT NULL COMMENT '조건부 일치율',
    rho_baseline          DOUBLE    NOT NULL COMMENT '클래스 사전분포 기반 우연 일치율',
    n_pairs               BIGINT    NOT NULL,
    window_days           INT       NOT NULL,
    fitted_at             TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta', 'labelpipe.immutable' = 'true');

-- ---------------------------------------------------------------------
-- 앵커 집합 (§9.2) - 개정 A5의 핵심
-- 무작위·층화 추출. 드리프트 재채점 시 동일 표본을 다시 채점한다.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anchor_set (
    anchor_set_id   STRING    NOT NULL,
    label_task      STRING    NOT NULL,
    sample_id       STRING    NOT NULL,
    target_ref_hash STRING    NOT NULL,
    stratum         STRING    NOT NULL COMMENT 'feature 분위수 × 예측 클래스',
    sampling_seed   STRING    NOT NULL COMMENT 'hash(anchor_set_id). 재추출 가능',
    period_start    DATE      NOT NULL,
    period_end      DATE      NOT NULL,
    superseded_by   STRING             COMMENT '갱신된 앵커 집합 ID (중첩 구간 유지)',
    created_at      TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta', 'labelpipe.immutable' = 'true');

-- ---------------------------------------------------------------------
-- 검수 큐 (상태 전이 허용)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS review_queue (
    queue_id        STRING    NOT NULL,
    sample_id       STRING    NOT NULL,
    label_task      STRING    NOT NULL,
    target_ref_hash STRING    NOT NULL,
    route           STRING    NOT NULL COMMENT 'disagreement | random_anchor | escalation',
    reason          STRING    NOT NULL COMMENT 'disagreement | no_signal | ontology_split | ...',
    priority        DOUBLE    NOT NULL COMMENT '§9.3 우선순위 함수 결과',
    margin          DOUBLE,
    anchor_set_id   STRING,
    state           STRING    NOT NULL COMMENT 'queued | assigned | done | expired',
    assigned_to     STRING             COMMENT '가명 reviewer_id',
    enqueued_at     TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta');

-- ---------------------------------------------------------------------
-- 드리프트 지표 (§10)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS drift_metric (
    metric_id        STRING    NOT NULL,
    label_task       STRING    NOT NULL,
    labeler_id       STRING             COMMENT 'NULL이면 태스크 전체',
    feature_bucket   STRING,
    ref_period       STRING    NOT NULL,
    cur_period       STRING    NOT NULL,

    psi              DOUBLE,
    kl_divergence    DOUBLE,
    n_ref            BIGINT,
    n_cur            BIGINT,
    fdr_adjusted_p   DOUBLE             COMMENT 'Benjamini–Hochberg 보정',

    anchor_set_id    STRING,
    anchor_acc_ref   DOUBLE,
    anchor_acc_cur   DOUBLE,
    anchor_acc_ci    STRUCT<lo: DOUBLE, hi: DOUBLE>,

    verdict          STRING    NOT NULL
        COMMENT 'stable | input_drift | labeler_drift | degradation (§10.3 표 3)',
    action_taken     STRING             COMMENT 'none | alert | isolate_labeler | republish',
    computed_at      TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta', 'labelpipe.immutable' = 'true');

-- ---------------------------------------------------------------------
-- 데이터셋 매니페스트 (§11) - 개정 C4
-- 모델 → 데이터셋 → 라벨 → 라벨러 → feature 스냅샷 계보의 연결점
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dataset_manifest (
    dataset_id             STRING    NOT NULL COMMENT 'UUIDv7',
    label_task             STRING    NOT NULL,
    ontology_ver           STRING    NOT NULL,

    feature_table          STRING    NOT NULL,
    feature_snapshot_id    STRING    NOT NULL,
    l2_version             BIGINT    NOT NULL,
    l3_version             BIGINT,

    fusion_policy_ver      STRING    NOT NULL,
    theta                  DOUBLE    NOT NULL,
    calibration_model_ver  STRING,
    correlation_model_ver  STRING,

    filter_predicate       STRING    NOT NULL COMMENT '학습 집합 선택 조건 (SQL)',
    include_pending        BOOLEAN   NOT NULL,
    pending_weight         DOUBLE,

    row_count              BIGINT    NOT NULL,
    class_distribution     MAP<STRING, BIGINT> NOT NULL,
    excluded_count         BIGINT    NOT NULL COMMENT 'P4: 배제는 반드시 기록된다',
    excluded_reason_breakdown MAP<STRING, BIGINT> NOT NULL,

    content_hash           STRING    NOT NULL COMMENT '동일 매니페스트 재빌드 시 일치해야 함',
    created_at             TIMESTAMP NOT NULL,
    created_by             STRING    NOT NULL
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta', 'labelpipe.immutable' = 'true');

-- ---------------------------------------------------------------------
-- 온톨로지 마이그레이션 (§6)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ontology_migration (
    from_ver    STRING NOT NULL,
    to_ver      STRING NOT NULL,
    from_class  STRING NOT NULL,
    to_class    STRING,
    kind        STRING NOT NULL COMMENT 'identity | rename | merge | split | drop',
    confidence  DOUBLE,
    note        STRING
) USING DELTA
TBLPROPERTIES ('labelpipe.layer' = 'meta');
-- kind='split'은 자동 사상 불가 → 해당 표본을 pending_review로 재라우팅
-- kind='drop'은 학습 제외하되 매니페스트 excluded_reason_breakdown에 기록
