# 라벨 일급 객체 파이프라인 — 백엔드 세부 설계

대상: [`02-design.md`](02-design.md) (데이터·알고리즘 설계), [`03-usecases.md`](03-usecases.md) (유스케이스)
버전: 0.1

---

## 1. 이 문서의 범위

[`02-design.md`](02-design.md)는 **무엇을 저장하고 어떻게 계산하는가**를 정한다. 이 문서는 그
계산을 실제로 굴리는 **서버 측 제어 구조**만 정한다: 실행 상태를 어디에 두는가, 누가 무엇을
호출하는가, 동시에 들어오면 어떻게 되는가, 실패하면 무엇이 남는가.

표기: 이 문서 안의 `§N`은 **이 문서의** 절이고, `설계서 §N`은 [`02-design.md`](02-design.md)의
절이다. 두 문서 모두 §4가 스키마, §9가 검수/오류처럼 번호가 겹치므로 구분해 읽는다.

### 1.1 설계하는 것

| 대상 | 이유 |
|------|------|
| 제어 평면 데이터 모델 (run, 검수 과제, 잡 실행 이력) | 레이크 테이블로는 상태 전이·임대·멱등을 감당할 수 없다 |
| 상태 머신 (run, 검수 과제) | UC-02/UC-05의 재개·임대 만료가 전이 규칙 없이는 구현 불가 |
| HTTP API 계약 | 검수 플랫폼·오케스트레이터·ML 엔지니어가 외부 시스템이다(§2 경계) |
| 동시성·멱등성 규칙 | EC-01, EC-02, EC-07, EC-08, EC-10이 전부 여기서 갈린다 |
| 잡 오케스트레이션과 락 | 배치 겹침(S-2)이 상시 발생한다 |
| 에러 카탈로그·인가·관측성 | 운영 인계 가능한 최소선 |

### 1.2 설계하지 않는 것 (과설계 방지선)

[`03-usecases.md`](03-usecases.md) §7의 규모 가정(API < 50 QPS, 검수자 30명, 일 15만 행)에서
불필요한 것들이다. 필요해지면 그때 도입하되, **지금 도입하면 운영 비용만 늘어난다.**

- 서비스 분할(마이크로서비스). 단일 배포 단위 안의 모듈 경계로 충분하다.
- 전용 메시지 브로커(Kafka/RabbitMQ). 큐 규모가 일 수천 건이다. DB 큐로 충분하다.
- 실시간 스트리밍 합의. 설계 전제가 마이크로배치다(설계서 §1.2).
- 자체 검수 UI, 자체 워크플로 엔진, 멀티테넌시, 자동 스케일링 정책.
- 분산 트랜잭션(2PC). 제어 DB 커밋 + 레이크 멱등 적재로 대체한다(§7.3).

---

## 2. 설계안 백엔드 관점 검토 결과

[`02-design.md`](02-design.md)를 유스케이스로 실행해 본 결과, 코드를 쓰려면 반드시 결정해야
하는데 비어 있던 지점이다. 각 항목이 이 문서의 어느 절에서 닫히는지 표시한다.

| # | 비어 있던 결정 | 드러난 유스케이스 | 본 문서 |
|---|----------------|-------------------|---------|
| G1 | run의 생애주기와 상태 전이 (재개 가능 지점이 어디인가) | UC-01, UC-02 | §5.1 |
| G2 | `review_queue.state`의 전이 규칙과 **임대 개념 부재** — `assigned`에서 검수자가 사라지면 영구 점유 | UC-05, EC-07 | §5.3 |
| G3 | 검수 결과 수신 계약 (누가 어떤 형식으로 L3를 넣는가) | UC-05 | §6.3 |
| G4 | 제어 상태의 저장소 — 레이크 테이블에 `UPDATE`를 건다고만 되어 있고 동시성 모델이 없음 | EC-02, EC-07 | §3, §4 |
| G5 | 멱등 키의 전달 경로 (`run_id`는 정의되나 **누가 발급하고 어떻게 재사용하는가**) | EC-01, EC-08 | §7.1 |
| G6 | 제어 DB 커밋과 레이크 적재 사이의 실패 (EC-10의 "L3는 들어갔는데 큐가 안 닫힘") | EC-10 | §7.3 |
| G7 | fusion 동시 실행 방지 — `l2_version` 경쟁 시 매니페스트가 가리키는 버전이 모호 | EC-02 | §7.2 |
| G8 | 인가 모델 — 설계서 §14는 "정책"만 말하고 주체(role)와 강제 지점이 없음 | EC-13, EC-20 | §10 |
| G9 | 드리프트 자동 조치의 승인 경계 (자동 격리 vs republish) | EC-17 | §5.2, §6.6 |
| G10 | 잡 실패 시 재시도 정책과 중복 실행 방지 (LLM 호출 재시도는 정의, 잡 자체는 미정의) | EC-22 | §8.2 |
| G11 | "미산출"과 "안정"의 구분이 스키마에 없음 | EC-16 | §8.3 |
| G12 | holdout 목적 L3가 L2로 전파되는지 — 설계서 §12.4는 "L3 도착 시 `human_final` 갱신"이라고만 쓴다. 그대로 구현하면 설계서 §4.5 누수 방지 계약이 뚫린다 | UC-15, EC-24 | §5.2, §7.3 |
| G13 | 검수자에게 합의 결과를 보여줄지 — 앵커 L3가 평가 기준선인데 L2를 보여주면 확인 절차가 된다 | UC-05, EC-23 | §6.3 |
| G14 | `holdout_pct` 변경 시 과거 분할이 재배정되는 문제 | EC-25 | §14 D5 |

G11은 레이크 스키마 변경이 필요하므로 [`ddl/04_meta.sql`](ddl/04_meta.sql)의 `drift_metric`에
`status STRING` 추가를 권고한다(§14 D2).

---

## 3. 아키텍처

### 3.1 제어 평면과 데이터 평면의 분리

```
 ┌──────────────── 제어 평면 (OLTP, Postgres) ────────────────┐
 │  labeling_run · run_shard · review_task · job_execution     │
 │  outbox_event · idempotency_key · audit_log                 │
 │  → 짧은 트랜잭션, 상태 전이, 임대, 유니크 제약               │
 └───────────────┬────────────────────────────────────────────┘
                 │ (모든 쓰기는 outbox를 거쳐 데이터 평면으로)
 ┌───────────────▼──────── 데이터 평면 (레이크, Delta/Iceberg) ─┐
 │  bronze_label_raw · silver_label_* · gold_label_*           │
 │  meta: labeler_registry · fusion_policy · anchor_set · ...   │
 │  → append-only, 대용량 스캔, 시점 이동                       │
 └─────────────────────────────────────────────────────────────┘
```

**왜 나누는가.** 레이크 테이블 포맷은 초당 수십 건의 짧은 상태 전이(임대 획득, 큐 pop)에
적합하지 않다. 파일 단위 커밋이라 경합 시 재시도가 잦고, 행 수준 잠금이 없으며, small file이
폭증한다. 반대로 제어 DB는 라벨 본체(일 15만 행 × 텍스트)를 담기에 부적합하다. **상태는 OLTP,
사실(fact)은 레이크**로 고정한다.

`review_queue`, `dataset_manifest` 등 [`ddl/04_meta.sql`](ddl/04_meta.sql)에 정의된 meta
테이블 중 **상태 전이가 있는 것(`review_queue`)은 제어 DB가 원본(SoR)** 이 되고, 레이크에는
완료 시점에 불변 이력으로 적재한다. 나머지 meta 테이블(append-only)은 레이크가 원본이다.

| 테이블 | 원본 | 레이크 사본 |
|--------|------|------------|
| `review_queue` | 제어 DB (`review_task`) | 완료 후 append (분석용) |
| run 실행 상태 | 제어 DB (`labeling_run`) | run 요약만 append |
| `labeler_registry`, `fusion_policy`, `anchor_set`, `calibration_model`, `drift_metric`, `dataset_manifest` | 레이크 | — |
| L0~L3 | 레이크 | — |

### 3.2 배포 단위

| 단위 | 역할 | 확장 |
|------|------|------|
| `labelpipe-api` | HTTP API. 무상태. run 생성, 검수 임대/제출, 데이터셋·계보 조회 | 수평 (2 인스턴스로 시작) |
| `labelpipe-worker` | 라벨러 실행·fusion·보정·드리프트·빌드. 큐에서 잡을 집어 실행 | 수평 (잡 종류별 동시성 상한) |
| `labelpipe-scheduler` | 크론 트리거, 임대 만료 수거, outbox 발행 | **단일 인스턴스** (리더 선출은 DB 어드바이저리 락) |

세 단위는 같은 코드베이스([`02-design.md`](02-design.md) §15.1의 `labelpipe` 패키지)의 다른
진입점이다. `api/`와 `jobs/` 두 패키지를 그 레이아웃에 추가한다.

```
labelpipe/
  api/          # FastAPI 앱, 라우터, 스키마(pydantic), 인가
  control/      # 제어 DB 모델, 상태 머신, 임대, outbox
  jobs/         # 잡 정의, 스케줄, 락, 체크포인트
  ... (기존 core/ labelers/ storage/ fusion/ review/ drift/ dataset/)
```

`review/` 모듈은 라우팅 정책(우선순위 계산, 앵커 배분)만 담당하고, 큐의 물리 구현은
`control/`이 담당한다. 정책과 저장을 섞으면 설계서 §9.3 우선순위 함수를 단위 테스트할 수 없다.

---

## 4. 제어 평면 데이터 모델

Postgres 기준. 7개 테이블로 닫는다. 레이크 DDL(`ddl/`)과 달리 제어 평면 DDL은 이 절이
정본이다. 별도 파일로 이중 관리하면 스키마가 갈라진다.

```sql
-- run: 라벨러 배치 1회 (UC-01)
CREATE TABLE labeling_run (
    run_id              TEXT PRIMARY KEY,          -- UUIDv7
    label_task          TEXT NOT NULL,
    ontology_ver        TEXT NOT NULL,
    period_start        DATE NOT NULL,
    period_end          DATE NOT NULL,
    feature_snapshot_id TEXT NOT NULL,             -- 생성 시점에 고정 (EC-03)
    config_hash         TEXT NOT NULL,             -- fusion_policy.config_hash와 동일 규격
    state               TEXT NOT NULL,             -- §5.1
    requested_by        TEXT NOT NULL,
    cost_usd            NUMERIC(12,4) NOT NULL DEFAULT 0,
    max_cost_usd        NUMERIC(12,4) NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    rerun_of            TEXT REFERENCES labeling_run(run_id),  -- 의도적 재실행 (EC-26)
    rerun_reason        TEXT                                   -- rerun_of가 있으면 필수
);
-- EC-01: 진행 중인 run은 (태스크, 기간, 설정)당 하나뿐. 종료된 run과는 충돌하지 않는다.
-- 전역 유니크로 걸면 "같은 설정으로 다시 돌리는 정정 재실행"(P1)이 영구히 막힌다.
CREATE UNIQUE INDEX run_active_natural_key ON labeling_run
    (label_task, period_start, period_end, config_hash)
    WHERE state IN ('created', 'running');

-- run의 라벨러별 샤드. 재개 단위 (UC-02, EC-22)
CREATE TABLE run_shard (
    run_id        TEXT NOT NULL REFERENCES labeling_run(run_id),
    labeler_id    TEXT NOT NULL,
    state         TEXT NOT NULL,                   -- pending|running|succeeded|failed
    cursor        TEXT,                            -- 진행 힌트 (sample_id 오름차순 커밋 지점)
    n_done        BIGINT NOT NULL DEFAULT 0,
    n_failed      BIGINT NOT NULL DEFAULT 0,
    cost_usd      NUMERIC(12,4) NOT NULL DEFAULT 0,
    attempt       INT NOT NULL DEFAULT 0,
    last_error    TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, labeler_id)
);
-- 재개 대상은 "커서 이후"가 아니라 **대상 집합 − 해당 run_id로 이미 적재된 L1 키**의
-- 차집합이다(이 문서 §8.2). 실패는 중간에 흩어져 발생하므로 커서만 쓰면 커서 앞의 실패분이
-- 영구 누락된다. 커서는 차집합 계산의 스캔 범위를 좁히는 힌트일 뿐이다.
-- cost_usd/n_done은 워커가 원자 증분(UPDATE ... SET x = x + $n)으로 갱신한다.
-- 읽고-더하고-쓰면 병렬 배치에서 갱신이 유실되고 비용 상한이 무력화된다.

-- 검수 과제. review_queue의 원본 (UC-05)
CREATE TABLE review_task (
    task_id         TEXT PRIMARY KEY,              -- UUIDv7
    sample_id       TEXT NOT NULL,
    label_task      TEXT NOT NULL,
    target_ref_hash TEXT NOT NULL,
    ontology_ver    TEXT NOT NULL,
    route           TEXT NOT NULL,                 -- disagreement|random_anchor|escalation
    reason          TEXT NOT NULL,
    anchor_set_id   TEXT,
    priority        DOUBLE PRECISION NOT NULL,
    margin          DOUBLE PRECISION,
    round           INT NOT NULL DEFAULT 1,        -- 이중 검수 회차
    state           TEXT NOT NULL,                 -- §5.3
    lease_owner     TEXT,                          -- 가명 reviewer_id
    lease_expires_at TIMESTAMPTZ,
    lease_count     INT NOT NULL DEFAULT 0,        -- EC-07: 반복 만료 시 우선순위 강등
    l2_version      BIGINT NOT NULL,               -- 어느 L2를 보고 만든 과제인가
    enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    blind           BOOLEAN NOT NULL DEFAULT false -- 앵커 경로는 true (§6.3, EC-23)
);
-- 중복 enqueue 방지. **열린 과제에만** 건다. 완료 행까지 포함해 유니크를 걸면
-- UC-14 재발행 후의 재검수와 앵커 재채점이 영구히 막힌다.
CREATE UNIQUE INDEX review_task_open_key ON review_task
    (sample_id, label_task, target_ref_hash, round)
    WHERE state IN ('queued', 'assigned', 'escalated');
CREATE INDEX ON review_task (label_task, state, priority DESC)
    WHERE state = 'queued';
CREATE INDEX ON review_task (lease_expires_at) WHERE state = 'assigned';

-- 잡 실행 이력 + 배타 락 (EC-02, G10)
CREATE TABLE job_execution (
    job_id       TEXT PRIMARY KEY,
    job_type     TEXT NOT NULL,                    -- run_labelers|fuse|calibrate|anchor|drift|build_dataset|migrate_ontology
    scope_key    TEXT NOT NULL,                    -- 배타 단위. 예: 'fuse:disaster_risk_level:ont-2026.03'
    state        TEXT NOT NULL,                    -- queued|running|succeeded|failed|cancelled
    params       JSONB NOT NULL,
    attempt      INT NOT NULL DEFAULT 0,
    heartbeat_at TIMESTAMPTZ,
    error        TEXT,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ
);
-- 같은 scope_key로 동시에 도는 잡은 하나뿐
CREATE UNIQUE INDEX job_scope_active ON job_execution (scope_key)
    WHERE state IN ('queued', 'running');

-- 레이크 적재 outbox (§7.3)
CREATE TABLE outbox_event (
    event_id     BIGSERIAL PRIMARY KEY,
    kind         TEXT NOT NULL,                    -- l3_append|review_done|run_summary|audit
    payload      JSONB NOT NULL,
    dedup_key    TEXT NOT NULL UNIQUE,             -- 레이크 PK와 동일 구성
    state        TEXT NOT NULL DEFAULT 'pending',  -- pending|sent|failed
    attempt      INT NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at      TIMESTAMPTZ
);
CREATE INDEX ON outbox_event (state, event_id) WHERE state <> 'sent';

-- 멱등 키 (§7.1)
CREATE TABLE idempotency_key (
    key          TEXT PRIMARY KEY,
    endpoint     TEXT NOT NULL,
    request_hash TEXT NOT NULL,                    -- canon(body)의 sha256
    response     JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 감사 로그 (§10.3)
CREATE TABLE audit_log (
    audit_id   BIGSERIAL PRIMARY KEY,
    actor      TEXT NOT NULL,
    role       TEXT NOT NULL,
    action     TEXT NOT NULL,                      -- run.create|review.submit|labeler.isolate|...
    subject    TEXT NOT NULL,
    detail     JSONB,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

보존: `idempotency_key` 30일, `outbox_event` 발행 후 7일, `job_execution` 90일,
`review_task` 완료 후 90일(레이크 사본은 무기한), `audit_log` 2년.

---

## 5. 상태 머신

### 5.1 run (UC-01, UC-02)

```
        created ──► running ──┬─► committed
           │          │       │
           │          │       └─► partially_failed ──► running (재개, 같은 run_id)
           │          │                   │
           │          └─► aborted ◄───────┘ (Ops 중단)
           └─► aborted (검증 실패)
```

| 전이 | 조건 | 부수 효과 |
|------|------|-----------|
| `created → running` | 샤드 생성 완료 | feature 스냅샷 ID 고정 |
| `running → committed` | 모든 샤드 `succeeded` | run 요약을 outbox로 레이크에 적재 |
| `running → partially_failed` | 샤드 1개 이상 `failed`, 나머지 종료 | 성공분은 이미 L0/L1에 커밋되어 있음 |
| `partially_failed → running` | UC-02 재개 요청 | 실패 샤드만 `pending`으로 되돌림, `attempt += 1` |
| `* → aborted` | Ops 중단 또는 설정 검증 실패 | 진행 중 호출만 취소. **적재된 L0/L1은 삭제하지 않는다**(P1) |

같은 기간·같은 설정으로 다시 돌려야 하는 경우(입력 정정, 라벨러 버그 수정 후 재라벨)는 이전
run이 종료된 뒤 **새 `run_id` + `rerun_of`** 로 만든다. L1을 덮어쓰지 않고 새 `run_id`로 쌓으면
`v_l1_latest`가 최신본을 노출한다(P1). 자연키 유니크를 진행 중 run에만 걸었기 때문에(§4)
전송 재시도는 EC-01로 흡수되면서도 의도적 재실행은 막히지 않는다.

비용 상한 초과(EC-06)는 별도 상태가 아니라 `partially_failed` + `last_error='cost_cap'`이다.
상태를 늘리지 않고 사유로 구분한다.

### 5.2 L2 flag (참고 — 레이크 측)

`agreed`/`soft_disagreement`/`pending_review`/`no_signal`은 fusion 산출값이고,
`human_final`만 사후 전이다(UC-15). 전이는 값 갱신이 아니라 `l2_version + 1` 발행이므로
제어 평면 상태 머신이 아니다. 백엔드가 보장할 것은 **"L3 도착 → 새 버전 발행"이 정확히 한 번
일어나는 것**뿐이다(§7.3).

**단, `l3_purpose='eval_holdout'`인 L3는 L2로 전파하지 않는다(EC-24).** 전파하면 holdout
정답이 `human_final`로 L2에 실리고, 그 L2로 만든 학습셋이 평가 기준선을 그대로 포함하게 된다
(설계서 §4.5 누수 방지 계약 위반). 증분 fusion 잡은 `v_l3_train`만 입력으로 받는다. holdout 표본의
L2는 fusion 산출값 그대로 남으며, 이것이 "holdout 대비 L2 정확도"(설계서 §18.1)가 성립하는 조건이다.

드리프트 조치(G9)도 같은 규칙을 따른다: 자동 조치는 `labeler_registry.enabled = false`(격리)
까지이고, republish는 Ops 승인 API 호출(§6.6)로만 발생한다.

### 5.3 검수 과제 (UC-05, EC-07~EC-10)

```
  queued ──lease──► assigned ──submit──► done
    ▲                  │
    │  lease 만료       │  판정 불가 / 2인 불일치
    └──────────────────┤
                       └──► escalated ──► (신규 행) queued[route=escalation, round+1]
  queued ──(취소: L2 재발행으로 무효화, disagreement 경로만)──► superseded
```

| 전이 | 규칙 |
|------|------|
| `queued → assigned` | 단일 트랜잭션에서 `SELECT ... FOR UPDATE SKIP LOCKED` + 임대 설정. 같은 키의 다른 회차를 같은 검수자에게 배정하지 않는다(EC-09 이중 검수) |
| `assigned → queued` | 임대 만료 수거(scheduler, 1분 주기). `lease_count += 1`, `lease_count > 3`이면 우선순위를 30% 감쇠 |
| `assigned → done` | 제출 수신. L3 적재는 outbox 경유(§7.3) |
| `assigned → escalated` | 이중 검수 불일치 또는 검수자의 판정 불가. 이 행은 여기서 끝난다 |
| (신규 행) | 중재 과제를 `route='escalation'`, `round + 1`, `state='queued'`로 **새로 만든다.** 상태를 되돌려 재배정하면 한 행에 두 판정 이력이 겹쳐 중재 근거가 사라진다 |
| `queued → superseded` | 해당 키의 L2가 재발행되어 과제 전제가 바뀜(UC-14). **`route='disagreement'`만 해당** — 앵커 과제는 L2와 무관하게 유효하므로 살려둔다. 이미 `assigned`면 전이하지 않고 완료를 기다린다 |

**이중 검수(EC-09)는 회차 행을 미리 만든다.** 앵커 과제 중 `double_review_ratio`만큼은 enqueue
시점에 `round=1`, `round=2` 두 행을 생성해 서로 다른 검수자에게 배정한다. 두 판정이 불일치하면
`round=3` 중재 행을 만든다. 한 행에 검수자 두 명을 배정하는 구조(단일 `lease_owner`)로는 이중
검수가 표현되지 않는다.

임대 기본 30분, 이중 검수 과제는 60분. 설정값이다.

---

## 6. API 계약

REST/JSON. 인증은 서비스 토큰(오케스트레이터·검수 플랫폼)과 사용자 토큰(OIDC) 두 경로.
모든 쓰기 엔드포인트는 `Idempotency-Key` 헤더를 받는다(§7.1).

### 6.0 공통

| 항목 | 규격 |
|------|------|
| 버전 | `/v1` 접두. 하위 호환 깨지는 변경만 `/v2` |
| 오류 본문 | `{"error": {"code": "...", "message": "...", "detail": {...}}}` (§9.1) |
| 페이지네이션 | 커서 방식 `?cursor=&limit=` (기본 100, 최대 1000) |
| 시각 | RFC 3339 UTC |
| 장시간 작업 | 즉시 `202` + `job_id` 반환. 폴링은 `GET /v1/jobs/{job_id}` |

### 6.1 run (UC-01, UC-02)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/v1/runs` | run 생성·시작. 동일 `(task, period, config_hash)`면 기존 run 반환(EC-01) |
| `GET` | `/v1/runs/{run_id}` | 상태, 샤드별 진행률, 누적 비용 |
| `POST` | `/v1/runs/{run_id}/resume` | 실패 샤드 재개(UC-02) |
| `POST` | `/v1/runs/{run_id}/abort` | 중단. 적재분은 유지 |

```http
POST /v1/runs
Idempotency-Key: orch-2026-09-18-disaster_risk_level
{
  "label_task": "disaster_risk_level",
  "period": {"start": "2026-09-17", "end": "2026-09-17"},
  "labelers": ["rule-risk-v2.1", "llm-primary", "llm-secondary"],
  "max_cost_usd": 50.0
}
→ 201 {"run_id": "0192...", "state": "running",
       "feature_snapshot_id": "v1821", "config_hash": "9f3c...",
       "shards": [{"labeler_id": "rule-risk-v2.1", "state": "pending"}, ...]}
```

이미 같은 run이 있으면 `200`과 기존 `run_id`를 돌려준다. `409`가 아니다 — 오케스트레이터의
재시도는 정상 동작이지 오류가 아니다.

### 6.2 합의 (UC-04)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/v1/fusion-jobs` | fusion 실행 요청. `202` + `job_id` |
| `GET` | `/v1/consensus/{label_task}/{sample_id}` | 특정 키의 L2(현재/특정 버전) 조회 |

`POST /v1/fusion-jobs`는 `scope_key = fuse:{task}:{ontology_ver}`로 잠금을 잡는다. 이미
실행 중이면 `409 fusion_in_progress`와 진행 중 `job_id`를 반환한다(EC-02).

### 6.3 검수 (UC-05, UC-12) — 검수 플랫폼 계약

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/v1/review-tasks/lease` | 우선순위 순 N건 임대. 앵커 몫은 항상 선반영(EC-11) |
| `POST` | `/v1/review-tasks/{task_id}/submit` | 판정 제출 → L3 |
| `POST` | `/v1/review-tasks/{task_id}/release` | 자발적 반납 (즉시 `queued`) |
| `POST` | `/v1/review-tasks/{task_id}/escalate` | 판정 불가 → 중재 |
| `GET` | `/v1/review-tasks/{task_id}` | 과제 상세(표본 참조, 후보 라벨 요약) |

```http
POST /v1/review-tasks/lease
{"reviewer_id": "rv-8821", "label_task": "disaster_risk_level", "limit": 20}
→ 200 {"tasks": [
    {"task_id": "0192...", "sample_id": "s-44921", "route": "random_anchor",
     "blind": true, "priority": 0.81,
     "lease_expires_at": "2026-09-18T05:00:00Z"},
    {"task_id": "0193...", "sample_id": "s-44930", "route": "disagreement",
     "blind": false, "priority": 0.77,
     "lease_expires_at": "2026-09-18T04:30:00Z",
     "candidates": [{"labeler_id": "llm-primary", "value_class": "high"},
                    {"labeler_id": "rule-risk-v2.1", "value_class": "medium"}],
     "consensus": {"value_class": "high", "margin": 0.06,
                   "flag": "pending_review"}}]}
```

**앵커 경로 과제는 블라인드로 내보낸다(EC-23).** `route='random_anchor'`인 과제의 응답에는
`candidates`와 `consensus`를 넣지 않는다. 합의 결과를 보여주면 검수자는 그것을 확인하는 방향으로
판정하게 되고, 그 L3로 보정 모델을 학습하고 라벨러 정확도를 재는 순간 기준선이 L2의 반복이 된다
(P5). 불일치 경로는 후보를 보여준다 — 목적이 기준선이 아니라 판정 효율이기 때문이다. 그 경우에도
보정 신뢰도를 노출하거나 신뢰도 순으로 정렬하지 않는다.

`lease_expires_at`은 과제별로 다르다(이중 검수 60분, 그 외 30분). 응답 최상위에 하나만 두면
클라이언트가 짧은 쪽을 놓친다.

```http
POST /v1/review-tasks/0192.../submit
Idempotency-Key: rv-8821:0192...:1
{"reviewer_id": "rv-8821", "value_class": "medium",
 "is_abstain": false, "review_seconds": 42, "note": "..."}
→ 200 {"task_id": "0192...", "state": "done",
       "l3": {"l3_purpose": "train", "source": "random_anchor",
              "l2_propagated": true, "l3_persisted": false}}
```

`l3_purpose='eval_holdout'`이면 `l2_propagated: false`다(§5.2, EC-24). 전파 여부를 응답에
드러내는 이유는, 검수했는데 L2가 그대로인 상황을 운영자가 장애로 오인하지 않게 하기 위함이다.

- 동일 `Idempotency-Key` 재요청: 저장된 응답을 그대로 반환(EC-08).
- 같은 키·다른 본문: `409 idempotency_conflict`.
- 임대 만료 후 제출: 과제가 아직 `queued`면 수용하고 `warning` 필드를 붙인다. 이미 다른
  검수자가 완료했으면 `409 task_already_completed`. **제출을 조용히 버리지 않는다**(EC-07).

### 6.4 데이터셋 (UC-09, UC-10)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `POST` | `/v1/datasets` | 매니페스트 생성 + 빌드. `202` + `job_id` |
| `GET` | `/v1/datasets/{dataset_id}` | 매니페스트 전문 + `content_hash` |
| `POST` | `/v1/datasets/{dataset_id}/rebuild` | 동일 매니페스트 재빌드, 해시 비교 결과 반환 |

`rebuild` 응답은 `{"content_hash_match": true|false, "diff_summary": {...}}`. 불일치는 오류가
아니라 **관측 결과**로 반환한다(스냅샷 소멸, 정책 변경 등 원인을 detail에 담는다).
스냅샷이 소멸한 경우에는 빌드 자체를 `422 snapshot_expired`로 거절한다(EC-18).

### 6.5 라벨러 레지스트리 (UC-03)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/v1/labelers` | 현재 유효 레지스트리(SCD2의 `valid_to IS NULL`) |
| `POST` | `/v1/labelers` | 신규 버전 등록 (SCD2 새 행) |
| `POST` | `/v1/labelers/{labeler_id}/isolate` | 격리 (`enabled=false`) |
| `POST` | `/v1/labelers/{labeler_id}/activate` | 활성화. **앵커 회귀 검증 통과가 전제**(설계서 §9.5) |

`activate`는 `anchor_regression_job_id`를 요구한다. 검증 없이 활성화하려면 `force=true` +
사유가 필요하고, 그 사실은 `audit_log`에 남는다.

### 6.6 드리프트·계보·재생 (UC-08, UC-11, UC-14)

| 메서드 | 경로 | 설명 |
|--------|------|------|
| `GET` | `/v1/drift?label_task=&period=` | 판정 결과(4분면), 미산출 버킷 포함 |
| `POST` | `/v1/republish-jobs` | L2 재발행 승인·실행(Ops 전용, G9) |
| `GET` | `/v1/labels/{label_id}/lineage` | L1 → L0 → 라벨러 버전 → feature 스냅샷 |
| `POST` | `/v1/labels/{label_id}/replay` | 재파싱 후 diff 반환. 외부 호출 0회 |
| `GET` | `/v1/datasets/{dataset_id}/lineage` | 역방향 계보(UC-11) |

---

## 7. 동시성·멱등성

### 7.1 멱등 키 (G5, EC-01, EC-08)

- 모든 쓰기 엔드포인트는 `Idempotency-Key` 헤더를 받는다. 미지정 시 `400`.
- 처리 전에 `idempotency_key`에 `(key, endpoint, request_hash)`를 삽입한다. PK 충돌 시:
  - `request_hash` 동일 → 저장된 응답 반환 (재시도)
  - 다르면 → `409 idempotency_conflict`
- 키 생성 규칙(권고): 오케스트레이터는 `{job_name}:{logical_date}`, 검수 플랫폼은
  `{reviewer_id}:{task_id}:{round}`. **무작위 UUID를 매 재시도마다 새로 만들면 멱등이 깨진다.**
- run의 자연 멱등 키는 `(label_task, period, config_hash)` 유니크 제약이다. 헤더 멱등과
  이중으로 건다(헤더는 전송 계층 재시도, 유니크 제약은 논리 중복을 각각 막는다).

### 7.2 배타 실행 (G7, EC-02)

`job_execution.scope_key`의 부분 유니크 인덱스가 락이다. 별도 락 서비스를 두지 않는다.

| 잡 | scope_key | 동시 실행 |
|----|-----------|-----------|
| `run_labelers` | `run:{run_id}:{labeler_id}` | 라벨러별 병렬 허용 |
| `fuse` | `fuse:{task}` | 태스크당 1개 (온톨로지 버전을 키에 넣지 않는다 — 아래 참조) |
| `calibrate` | `calib:{task}` | 1개 |
| `drift` | `drift:{task}:{cur_period}` | 1개 |
| `build_dataset` | `build:{dataset_id}` | 1개 |
| `migrate_ontology` | `fuse:{task}` | **fuse와 같은 scope를 공유** — 마이그레이션 중 합의 금지 |

배타 단위를 `(task, ontology_ver)`가 아니라 **태스크**로 잡는 이유는 마이그레이션 때문이다.
개정 작업은 `from_ver`와 `to_ver` 양쪽을 건드리는데, 키에 버전이 들어가면 `from_ver` 합의가
마이그레이션과 나란히 돌 수 있다. 태스크 하나에서 두 온톨로지 버전을 동시에 합의할 일은 없으므로
(설계서 §6 혼합 합의 금지) 잃는 병렬성도 없다.

워커는 30초마다 `heartbeat_at`을 갱신한다. scheduler는 하트비트가 3분 이상 끊긴 `running` 잡을
`failed`로 전이해 락을 해제하고, 재시도 정책(§8.2)에 넘긴다.

### 7.3 제어 DB와 레이크 사이의 정확히 한 번 (G6, EC-10)

2PC를 쓰지 않는다. **outbox + 멱등 적재**로 at-least-once를 exactly-once로 만든다.

```
[검수 제출]
  BEGIN (제어 DB)
    review_task: assigned → done
    outbox_event: kind='l3_append', dedup_key=(sample,task,target,round,reviewer)
  COMMIT
        │
        ▼ (scheduler의 outbox 발행기, 최대 5회 지수 백오프)
  레이크 MERGE INTO gold_label_verified ... WHEN NOT MATCHED THEN INSERT
        │
        ▼
  outbox_event: sent
```

- 제어 DB 커밋이 성공하면 검수자에게는 성공을 반환한다. 레이크 적재는 비동기다(수 초 내).
- 발행기가 죽어 재시도해도 `dedup_key`와 레이크 PK가 동일하므로 중복 행이 생기지 않는다(설계서 §12.1).
- 5회 실패한 이벤트는 `failed`로 두고 알림을 띄운다. **자동 폐기하지 않는다.**
- 레이크 적재 지연 중에 `GET /v1/review-tasks/{id}`는 `l3_persisted: false`를 함께 반환해
  비동기임을 숨기지 않는다.

L3 도착 → L2 `human_final` 갱신(UC-15)도 같은 경로다. outbox 이벤트가 증분 fusion 잡을 큐에
넣고, 그 잡이 해당 파티션만 `l2_version + 1`로 재발행한다. 단 **제출 건마다 돌리지 않는다.**
이벤트를 5분 윈도로 묶어 파티션 단위 1회로 실행한다(설정값). 제출마다 버전을 올리면 하루에 수백
개의 `l2_version`이 생기고, 매니페스트가 가리키는 버전이 사실상 임의의 시점이 된다.
`eval_holdout` 목적 L3는 이 경로를 타지 않는다(§5.2).

### 7.4 큐 경합 (EC-11, EC-09)

```sql
-- 임대 획득: 경로별 몫을 각각 잠그고 합친다 (설계서 §9.1 예산 배분)
-- 경로별로 나눈 이유: 한 쿼리에서 윈도 함수로 몫을 자르면 FOR UPDATE를 함께 쓸 수 없다.
WITH esc AS (
  SELECT t.task_id FROM review_task t
   WHERE t.label_task = $1 AND t.state = 'queued' AND t.route = 'escalation'
     AND NOT EXISTS (                       -- EC-09: 같은 키를 맡았던 사람 제외
       SELECT 1 FROM review_task p
        WHERE p.sample_id       = t.sample_id
          AND p.label_task      = t.label_task
          AND p.target_ref_hash = t.target_ref_hash
          AND p.lease_owner     = $reviewer)
   ORDER BY t.priority DESC
   FOR UPDATE SKIP LOCKED
   LIMIT $n_esc                             -- ceil(limit * epsilon)
), anchor AS (
  SELECT t.task_id FROM review_task t
   WHERE t.label_task = $1 AND t.state = 'queued' AND t.route = 'random_anchor'
     AND NOT EXISTS ( ... 동일 조건 ... )    -- 앵커에도 반드시 적용 (이중 검수)
   ORDER BY t.priority DESC
   FOR UPDATE SKIP LOCKED
   LIMIT $n_anchor                          -- ceil(limit * rho), 선점 불가 (EC-11)
), dis AS (
  SELECT t.task_id FROM review_task t
   WHERE t.label_task = $1 AND t.state = 'queued' AND t.route = 'disagreement'
     AND NOT EXISTS ( ... 동일 조건 ... )
   ORDER BY t.priority DESC
   FOR UPDATE SKIP LOCKED
   LIMIT $limit - $n_esc - $n_anchor
)
UPDATE review_task SET state = 'assigned', lease_owner = $reviewer,
       lease_expires_at = now() + CASE WHEN round > 1 THEN $ttl_long ELSE $ttl END,
       lease_count = lease_count + 1
 WHERE task_id IN (SELECT task_id FROM esc
                   UNION ALL SELECT task_id FROM anchor
                   UNION ALL SELECT task_id FROM dis)
RETURNING *;
```

`SKIP LOCKED`가 동시 임대의 경합을 해결한다. 주의할 점 셋.

1. **같은 검수자 배제는 세 경로 모두에 건다.** 이중 검수 대상이 앵커 경로에 있으므로
   (설계서 §9.4), 불일치 경로에만 걸면 정작 필요한 곳에서 빠진다 — 한 사람이 같은 표본의
   1회차와 2회차를 모두 받게 된다.
2. **앵커 몫은 선점되지 않는다**(EC-11). 반대로 앵커가 부족해 몫을 못 채우면, 남은 자리는
   2차 패스로 불일치에서 채우고 `anchor_budget_fill_ratio < 1`을 기록한다. 앵커 고갈은
   UC-06 재추출 신호이므로 조용히 넘기지 않는다.
3. 경로별 몫은 임대 배치 단위의 근사다. 정확한 예산 집행(설계서 §9.1)은 기간 단위 집계로
   감시한다(§11).

---

## 8. 잡 오케스트레이션

### 8.1 트리거

| 잡 | 스케줄 | 트리거 원천 |
|----|--------|------------|
| `run_labelers` | `0 2 * * *` | 외부 오케스트레이터 → `POST /v1/runs` |
| `fuse` | run 커밋 이벤트 | 내부 (outbox) |
| `incremental_fuse` | L3 적재 이벤트 | 내부 (outbox, UC-15) |
| `calibrate` | `0 3 * * 1` + 트리거 조건(설계서 §8.4) | scheduler |
| `anchor_sample` | 주기/수동 | 검수 리드 |
| `drift` | `0 4 * * *` | scheduler |
| `lease_reaper` | `* * * * *` | scheduler |
| `outbox_publisher` | 5초 주기 | scheduler |

외부 오케스트레이터(Airflow 등)는 **run 시작만** 건드린다. 이후 fusion·증분 fusion은 내부
이벤트로 이어지므로, 외부 DAG가 파이프라인 내부 순서를 알 필요가 없다.

### 8.2 재시도와 중복 방지 (G10, EC-22)

| 실패 유형 | 처리 |
|-----------|------|
| 일시적(네트워크, 레이크 커밋 충돌) | 지수 백오프 재시도 최대 3회. `attempt` 증가 |
| 데이터 오류(스키마 위반, 필터 SQL 오류) | 재시도 없음. `failed` + 원인 보고 |
| 워커 소실 | 하트비트 만료 → `failed` → 재시도 정책 적용. 샤드 `cursor`에서 재개 |
| 비용 상한 | 재시도 없음. Ops 개입 필요(UC-02 예외) |

재시도는 항상 **같은 `run_id`/`job_id`** 로 수행한다. 새 ID를 발급하면 레이크에 중복 행이 쌓인다.

### 8.3 잡별 요지

| 잡 | 입력 고정 | 산출 | 주의 |
|----|-----------|------|------|
| `run_labelers` | feature 스냅샷 ID, 레지스트리 스냅샷 | L0, L1 | 샤드 단위 체크포인트 |
| `fuse` | `v_l1_latest` 또는 `run_id` 집합, 정책·보정 버전 | L2, 큐 적재 | 태스크 배타 |
| `incremental_fuse` | 해당 파티션 + `v_l3_train` | `l2_version + 1` | 전체 재계산 금지, 5분 윈도 배치, holdout 비전파(EC-24) |
| `calibrate` (UC-07) | `v_l3_calibration` | `calibration_model` | holdout 접근 불가(§10.2) |
| `anchor_sample` | 모집단 + `seed=hash(anchor_set_id)` | `anchor_set`, 큐 적재 | `ρ=0` 거절 |
| `drift` | 기준/현재 기간 | `drift_metric` | 표본 부족 버킷은 `status='insufficient'`로 기록(G11, EC-16) |
| `build_dataset` | 매니페스트 | 데이터셋 + `content_hash` | `now()` 금지(설계서 §11.2) |
| `migrate_ontology` (UC-13) | 마이그레이션 맵 | 뷰 갱신 + `split` 재라우팅 | fuse와 배타 |

---

## 9. 실패 처리

### 9.1 에러 카탈로그

| HTTP | code | 상황 | 클라이언트 조치 |
|------|------|------|----------------|
| 400 | `missing_idempotency_key` | 헤더 누락 | 키 부여 후 재요청 |
| 409 | `idempotency_conflict` | 같은 키·다른 본문 | 키 재생성 |
| 409 | `fusion_in_progress` | 태스크 배타 위반(EC-02) | 진행 중 `job_id` 폴링 |
| 409 | `task_already_completed` | 만료 임대의 뒤늦은 제출(EC-07) | 결과 폐기, 로그 |
| 409 | `run_already_exists` → 200으로 강등 | 중복 run 요청(EC-01) | — |
| 422 | `snapshot_expired` | 스냅샷 소멸(EC-18) | 재현 불가를 보고 |
| 422 | `invalid_filter_predicate` | 필터가 holdout 참조(EC-13) | 필터 수정 |
| 422 | `ontology_mismatch` | 혼합 온톨로지 합의 시도(EC-19) | 마이그레이션 선행 |
| 422 | `anchor_ratio_zero` | `ρ=0` 설정(EC-12) | 설정 수정 |
| 403 | `egress_not_allowed` | 외부 라벨러를 민감 태스크에(EC-20) | 라벨러 교체 |
| 403 | `holdout_access_denied` | holdout 뷰 접근(EC-13) | — |
| 429 | `rate_limited` | 임대 API 남용 | 백오프 |
| 503 | `cost_cap_reached` | 비용 상한(EC-06) | 상한 조정 후 재개 |

### 9.2 부분 실패의 원칙

1. **적재된 사실은 되돌리지 않는다.** run 중단 시 L0/L1 롤백 없음(P1).
2. **실패도 관측 대상이다.** 파싱 실패·API 실패는 행으로 남는다(설계서 §12.2).
3. **조용한 대체 금지.** 스냅샷 소멸을 최신본으로, 미산출을 안정으로 대체하지 않는다.

---

## 10. 인가·PII·감사 (G8)

### 10.1 역할

| 역할 | 가능 | 불가 |
|------|------|------|
| `ops` | run/재개/중단, 라벨러 격리·활성, republish 승인 | holdout 조회, 검수 제출 |
| `ml` | 매니페스트 생성·빌드·재현, L2 조회 | 라벨러 변경, holdout 조회 |
| `reviewer` | 임대·제출·중재 | 다른 검수자 과제 조회, 라벨러 성능 조회 |
| `lead` | 예산·앵커 관리, 중재 배정, 검수자 지표 조회 | run 변경 |
| `auditor` | 계보·재생·감사 로그 조회(읽기 전용), holdout 지표 조회 | 모든 쓰기 |
| `service` | 오케스트레이터/검수 플랫폼 전용 엔드포인트 | 그 외 |

### 10.2 holdout 격리의 강제 지점 (EC-13)

세 겹으로 건다. 한 겹이라도 빠지면 누수는 조용히 일어난다.

1. **DB 권한**: 워커는 **잡 종류에 따라 다른 역할로 접속한다.** `fuse`/`incremental_fuse`/
   `calibrate`는 `labelpipe_fusion`(holdout 뷰 권한 없음), `drift`/평가 잡만 `labelpipe_eval`을
   쓴다. 한 워커 프로세스가 여러 잡 종류를 돌리므로, 커넥션 풀을 역할별로 분리하고 디스패처가
   `job_type → 역할` 매핑을 강제한다. 프로세스 하나가 단일 계정으로 모든 잡을 돌리면 이 계층은
   있으나 마나다 — 가장 흔한 누수 경로다.
2. **코드 경로 정적 검사**: `fusion/`, `labelpipe/jobs/calibrate.py`가 holdout 뷰 이름을
   참조하면 CI 실패(설계서 §14.4).
3. **API 인가**: `ml`, `ops` 역할에 holdout 조회 엔드포인트를 노출하지 않는다.

### 10.3 감사

`audit_log`에 남기는 최소 집합: run 생성/중단, 라벨러 활성·격리(특히 `force=true`), republish
승인, 매니페스트 생성, holdout 조회 시도(성공·실패 모두), 검수 제출.
검수자 지표는 `lead`/`auditor`만 조회 가능하며, 개인 평가 목적 사용 금지 정책을 응답 헤더에
명시한다(설계서 §14.2).

PII: LLM 전송 경로에서 마스킹이 적용되지 않은 요청은 라벨러 어댑터가 거부한다(`masking_ver`
미설정 + `data_egress=external` 조합은 설정 검증에서 차단).

---

## 11. 관측성

| 지표 | 유형 | 경보 |
|------|------|------|
| `run_duration_seconds{task,labeler}` | 히스토그램 | p95가 SLA(2h) 초과 |
| `run_cost_usd{task}` | 카운터 | 상한의 80% 도달 |
| `l1_rows_total{task,labeler,parse_status}` | 카운터 | `failed` 비율 > 5% |
| `abstain_ratio{labeler}` | 게이지 | 급변(전주 대비 2배) |
| `fusion_flag_ratio{flag}` | 게이지 | `pending_review` > 30% |
| `review_queue_depth{route,state}` | 게이지 | `queued` 적체 > 3일치 |
| `review_lease_expired_total` | 카운터 | 급증(검수 플랫폼 장애 신호) |
| `anchor_budget_fill_ratio` | 게이지 | < 1.0 (앵커 고갈, EC-11) |
| `outbox_pending_age_seconds` | 게이지 | > 5분 (레이크 적재 지연) |
| `outbox_failed_total` | 카운터 | > 0 (즉시 알림) |
| `drift_verdict_total{verdict}` | 카운터 | `labeler_drift` 발생 시 |

대시보드는 두 개면 족하다: **운영**(run/큐/outbox)과 **품질**(라벨러 건강도 설계서 §7.4, 드리프트).

---

## 12. 테스트 (백엔드 한정)

[`02-design.md`](02-design.md) §17에 더해, 제어 평면에 필요한 것만 추가한다.

| 유형 | 대상 | 검증 |
|------|------|------|
| 상태 머신 property | run, review_task | 정의되지 않은 전이가 발생하지 않음 |
| 동시성 | 임대 | 워커 20개가 동시 임대해도 한 과제가 두 번 배정되지 않음(EC-09) |
| 동시성 | fusion | 동시 요청 2건 중 1건만 실행(EC-02) |
| 멱등 | 제출 API | 같은 키 100회 호출 → L3 1행(EC-08) |
| 장애 주입 | outbox | 레이크 적재 중단 후 재개 시 중복 0, 유실 0(EC-10) |
| 장애 주입 | 워커 kill | 샤드 재개 후 L1 행 수 불변(EC-22) |
| 계약 | 검수 플랫폼 API | OpenAPI 스키마 기반 계약 테스트 |
| 인가 | holdout | 각 역할로 holdout 접근 시 403(EC-13) |
| 인가 | 블라인드 | 앵커 과제 임대 응답에 `candidates`/`consensus` 부재(EC-23) |
| 누수 | holdout 전파 | holdout 목적 L3 제출 후 해당 키의 `l2_version`이 증가하지 않음(EC-24) |
| 멱등/재실행 | 정정 재실행 | 종료된 run과 같은 설정으로 새 run 생성 가능, 진행 중이면 기존 run 반환(EC-26) |
| 시나리오 | S-1 ~ S-6 | 통합 테스트로 1:1 대응 |

---

## 13. 케이스 → 메커니즘 추적표

| 케이스 | 메커니즘 | 절 |
|--------|----------|-----|
| EC-01 | `labeling_run` 유니크 제약 + 멱등 키 | §4, §7.1 |
| EC-02 | `job_execution.scope_key` 부분 유니크 인덱스 | §7.2 |
| EC-03 | run 생성 시 `feature_snapshot_id` 고정 | §4, §5.1 |
| EC-04 | L0 선적재 → L1 `parse_status` 기록 | §6.1, §9.2 |
| EC-05 | `no_signal` L2 발행 + 저우선 큐 적재 | §5.2 |
| EC-06 | `cost_usd` 누적 + `partially_failed` + `503 cost_cap_reached` | §5.1, §9.1 |
| EC-07 | 임대 만료 수거 + 뒤늦은 제출 수용 규칙 | §5.3, §6.3 |
| EC-08 | `idempotency_key` 테이블 | §7.1 |
| EC-09 | `round` 증가 + 동일 검수자 배제 쿼리 | §5.3, §7.4 |
| EC-10 | outbox + `dedup_key` | §7.3 |
| EC-11 | 임대 쿼리의 앵커 선반영 + `anchor_budget_fill_ratio` | §7.4, §11 |
| EC-12 | 설정 검증 `422 anchor_ratio_zero` | §9.1 |
| EC-13 | DB 권한 + 정적 검사 + API 인가 3중 | §10.2 |
| EC-14 | 보정 폴백 사실을 L2 메타에 기록 | §8.3 |
| EC-15 | 상관 모델 버전을 L2에 기록 | §8.3 |
| EC-16 | `drift_metric.status='insufficient'` (스키마 추가 필요) | §8.3, §14 D2 |
| EC-17 | 자동 격리까지만, republish는 승인 API | §5.2, §6.6 |
| EC-18 | `422 snapshot_expired` | §6.4, §9.1 |
| EC-19 | `migrate_ontology`가 fuse와 배타, `split` 재라우팅 | §7.2, §8.3 |
| EC-20 | 레지스트리 정책 검증 `403 egress_not_allowed` | §9.1, §10.3 |
| EC-21 | `incremental_fuse` + 매니페스트 불변 | §7.3, §8.1 |
| EC-22 | 적재분 차집합 재개 + 하트비트 만료 회수 | §4, §7.2, §8.2 |
| EC-23 | 앵커 과제 `blind=true`, 후보·합의 미노출 | §6.3 |
| EC-24 | 증분 fusion 입력을 `v_l3_train`으로 고정 | §5.2, §7.3 |
| EC-25 | `holdout_pct`를 태스크 생성 시 고정·버전화 | §14 D5 |
| EC-26 | 자연키 유니크를 진행 중 run으로 한정 + `rerun_of` | §4, §5.1 |

---

## 14. 설계안 대비 변경 제안

백엔드 관점 검토에서 [`02-design.md`](02-design.md)와 [`ddl/`](ddl/) 쪽 수정이 필요해진 항목이다.
이 문서 안에서만 정하고 넘어가면 두 문서가 갈라지므로 여기에 모아 둔다.

| # | 변경 | 대상 | 근거 |
|---|------|------|------|
| D1 | `review_queue`의 원본을 제어 DB(`review_task`)로 이전하고, 레이크에는 완료 이력만 append | 02-design §3 표, `ddl/04_meta.sql` | 임대·상태 전이는 레이크 테이블 포맷이 감당하지 못한다(§3.1) |
| D2 | `drift_metric.status` 추가 (`computed` / `insufficient`) | `ddl/04_meta.sql` | 표본 부족 미산출과 "안정"을 구분해야 한다(EC-16) |
| D3 | holdout L3는 L2로 전파하지 않음을 명문화 | 02-design §12.4, §4.5 | 전파하면 평가 기준선이 학습셋에 실린다(EC-24) |
| D4 | 앵커 과제는 블라인드 배정 | 02-design §9.2, §9.4 | 합의 결과를 보여주면 L3가 L2의 확인 절차가 된다(EC-23) |
| D5 | `holdout_pct`를 태스크 생성 시 고정하고 버전으로 관리 | 02-design §4.4, `config/pipeline.example.yaml` | 결정론적 분할이지만 **`holdout_pct`를 바꾸면 과거 배정이 통째로 뒤바뀐다.** 이미 학습에 쓰인 표본이 holdout으로 넘어오면 평가가 조용히 오염된다(EC-25) |
| D6 | fusion 배타 단위를 `(task, ontology_ver)`에서 태스크로 | 02-design §7.1 | 마이그레이션이 두 버전을 동시에 건드린다(§7.2) |

D2·D5는 스키마·설정 변경이므로 각각 M7, M3 착수 전에 확정한다. D1은 M3 착수 전에 확정해야
한다 — 검수 API가 그 위에 올라간다.

---

## 15. 구현 순서와 미결정

### 15.1 기존 로드맵(§19) 위에 얹기

| 기존 단계 | 추가되는 백엔드 작업 |
|-----------|---------------------|
| M1 | 제어 DB 스키마(§4), `job_execution` 락, 잡 러너 골격 |
| M2 | `POST /v1/runs`, 샤드 체크포인트·재개, 멱등 키 |
| M3 | 검수 API(§6.3), 임대 상태 머신, outbox + L3 적재 |
| M4 | fusion 잡 배타 실행, 큐 enqueue, `incremental_fuse` |
| M5 | 보정 잡 트리거, holdout 권한 분리(§10.2) |
| M6 | 데이터셋 API, 재현 비교 응답 |
| M7 | 드리프트 조회·republish 승인 API |
| M8 | 대시보드 지표(UC-16), 감사 로그 조회 |

M3의 outbox를 M4보다 먼저 세우는 이유는, L3 적재 경로가 이 설계에서 유일하게 **제어 평면과
데이터 평면을 동시에 쓰는 지점**이기 때문이다. 여기서 실패 모델을 확정하지 않으면 이후 모든
쓰기 경로가 임시방편을 답습한다.

### 15.2 미결정

| # | 항목 | 현재 입장 |
|---|------|-----------|
| B1 | 제어 DB 엔진 | Postgres 가정(`SKIP LOCKED`, 부분 유니크 인덱스, JSONB 의존). 사내 표준이 다르면 임대 쿼리 재설계 필요 |
| B2 | `drift_metric.status` 컬럼 추가 | 권고(§2 G11). DDL 변경이므로 M7 착수 전 확정 |
| B3 | 검수 플랫폼 연동 방식 | 폴링(lease) 가정. 플랫폼이 webhook push만 지원하면 §6.3에 콜백 수신 엔드포인트 추가 |
| B4 | 임대 TTL 30분 | 실제 검수 소요 시간 측정 후 조정. `review_seconds` 분포가 근거 |
| B5 | API 인증 | OIDC + 서비스 토큰 가정. 사내 IdP 방식에 종속 |
| B6 | outbox 발행기의 단일 인스턴스 제약 | 어드바이저리 락으로 리더 1개. 처리량 부족 시 `kind`별 샤딩 |
| B7 | `review_task`의 레이크 사본 시점 | 완료 시 append 가정. 큐 상태 이력 전체가 분석에 필요하면 CDC 도입 검토 |
| B8 | 재검수 재enqueue 정책 | 유니크를 열린 과제로 한정했으므로 같은 키가 반복 enqueue될 수 있다. 회차 상한(기본 5)과 "직전 검수 후 N일 이내 재enqueue 금지"를 둘지는 운영 데이터를 본 뒤 결정 |
| B9 | 블라인드 검수의 UI 지원 | 검수 플랫폼이 후보 라벨을 숨긴 화면을 제공해야 D4가 성립한다. 미지원이면 앵커 전용 프로젝트를 분리해 운영 |
