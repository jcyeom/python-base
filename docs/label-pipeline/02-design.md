# 라벨 일급 객체 파이프라인 — 구현 설계서

대상 문서: [`00-paper-revised.md`](00-paper-revised.md)
버전: 0.1 (구현 착수용)

---

## 1. 범위와 목표

### 1.1 이 설계가 만드는 것

메달리온 레이크 위에서 동작하는 라벨 생산·합의·검수·모니터링 파이프라인 `labelpipe`.
라벨은 feature와 동일한 테이블 포맷(Delta 또는 Iceberg)·동일 카탈로그·동일 계보 도구 위에
저장되며, 학습 데이터 구성은 외부 익스포트 없이 SQL 조인으로 수행된다.

### 1.2 비목표

- 라벨링 UI 구현 (기존 Label Studio 등을 검수 프런트로 사용하고 결과만 수집)
- 모델 학습·서빙 (매니페스트를 소비하는 쪽은 이 파이프라인 밖)
- 실시간(초 단위) 라벨링. 본 설계는 마이크로배치(분~시간 단위)를 전제

### 1.3 설계 원칙

| P | 원칙 | 귀결 |
|---|------|------|
| P1 | L0/L1은 불변(immutable) | append-only, UPDATE 금지, 정정은 새 `run_id`로 |
| P2 | 모든 산출물은 입력 집합과 정책 버전으로 재생 가능 | 모든 테이블에 생성 근거 메타 동반 |
| P3 | 판단 유보와 부정 라벨은 다른 것 | `is_abstain`을 값 영역과 분리 |
| P4 | 배제된 데이터는 반드시 기록된다 | 학습 필터는 매니페스트에 선언, 암묵적 NULL 배제 금지 |
| P5 | 평가 기준은 무작위 표본에서만 나온다 | 앵커 집합 강제 배정 |
| P6 | 정책은 코드가 아니라 버전 있는 설정 | `fusion_policy_ver` 등 모든 정책에 버전 |

---

## 2. 아키텍처

```
                    ┌──────────────────────────────────────────┐
   feature (Silver) │  features_v1  @ snapshot_id              │
                    └───────────────┬──────────────────────────┘
                                    │  (input_spec 컬럼만 투영)
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
   ┌────▼─────┐              ┌──────▼──────┐             ┌──────▼──────┐
   │ Rule     │              │ LLM         │             │ Human       │
   │ Labeler  │              │ Labeler     │             │ Labeler     │
   └────┬─────┘              └──────┬──────┘             └──────┬──────┘
        │ raw eval log              │ raw API response          │ UI event
        └───────────────┬───────────┴───────────────────────────┘
                        ▼
              ┌─────────────────────┐
     BRONZE   │ bronze_label_raw    │  L0  원시 응답 (파싱 전)
              └──────────┬──────────┘
                         │ parse + schema validate
                         ▼
              ┌─────────────────────┐   ┌──────────────────────────┐
     SILVER   │ silver_label_cand   │   │ silver_label_rationale   │
              │        (L1)         │   │   (근거 텍스트 분리)       │
              └──────────┬──────────┘   └──────────────────────────┘
                         │
              ┌──────────▼──────────────────────────────┐
              │        Label Fusion Engine              │
              │  calibrate → correlate → vote → route   │
              └────┬───────────────────────────┬────────┘
                   │                           │ pending_review
                   ▼                           ▼
              ┌─────────────┐          ┌────────────────┐
     GOLD     │ gold_label_ │          │ review_queue   │◄── anchor sampler
              │ consensus   │          └───────┬────────┘    (무작위 ρ 강제)
              │    (L2)     │                  │
              └──────┬──────┘                  ▼
                     │                 ┌────────────────┐
                     │                 │ gold_label_    │
                     │                 │ verified (L3)  │
                     │                 └───────┬────────┘
                     │                         │
                     │        ┌────────────────┴──────────────┐
                     │        │ calibration fit / drift anchor │
                     │        └────────────────┬──────────────┘
                     ▼                         ▼
              ┌──────────────────────────────────────────┐
              │ dataset_manifest  (feature snap + L2/L3   │
              │                    + policy ver + filter) │
              └──────────────────────────────────────────┘
```

---

## 3. 계층과 테이블 목록

| 계층 | 테이블 | 쓰기 방식 | 보존 |
|------|--------|-----------|------|
| Bronze | `bronze_label_raw` | append-only | 180일 hot → cold archive |
| Silver | `silver_label_candidate` (L1) | append-only | 무기한 (cold tier 이관) |
| Silver | `silver_label_rationale` | append-only | Bronze와 동일 |
| Gold | `gold_label_consensus` (L2) | 버전 있는 overwrite (스냅샷 유지) | 무기한 |
| Gold | `gold_label_verified` (L3) | append-only + adjudication merge | 무기한 |
| Meta | `labeler_registry` | SCD2 | 무기한 |
| Meta | `fusion_policy` | append-only | 무기한 |
| Meta | `calibration_model` | append-only | 무기한 |
| Meta | `anchor_set` | append-only | 무기한 |
| Meta | `review_queue` | 상태 전이(UPDATE 허용) | 완료 후 90일 |
| Meta | `drift_metric` | append-only | 무기한 |
| Meta | `dataset_manifest` | append-only | 무기한 |

DDL 전문: [`ddl/01_bronze.sql`](ddl/01_bronze.sql), [`ddl/02_silver.sql`](ddl/02_silver.sql),
[`ddl/03_gold.sql`](ddl/03_gold.sql), [`ddl/04_meta.sql`](ddl/04_meta.sql)

---

## 4. 스키마 상세

### 4.1 `bronze_label_raw` (L0)

파싱 이전의 원시 응답. L1 재생의 근거.

| 컬럼 | 타입 | 비고 |
|------|------|------|
| `raw_id` | STRING | UUIDv7. L1의 `raw_response_ref`가 참조 |
| `run_id` | STRING | |
| `labeler_id` | STRING | |
| `sample_id` | STRING | |
| `label_task` | STRING | |
| `request_payload` | STRING | 전송한 프롬프트/입력 전문 (PII 마스킹 후) |
| `response_payload` | STRING | 응답 전문 (JSON). 파싱 실패해도 그대로 적재 |
| `response_meta` | MAP<STRING,STRING> | HTTP 상태, 모델 ID 에코, finish_reason, logprobs 요약 |
| `http_status` | INT | |
| `error` | STRING | 실패 시 예외 메시지 |
| `created_at` | TIMESTAMP | |

파티션: `date(created_at)`, `labeler_id`

### 4.2 `silver_label_candidate` (L1)

논문 표 2와 동일. 추가 운영 컬럼:

| 컬럼 | 타입 | 비고 |
|------|------|------|
| `label_id` | STRING | UUIDv7, 표면 키 |
| `parse_status` | STRING | `ok` / `repaired` / `failed` |
| `schema_ver` | STRING | L1 스키마 자체의 버전 |

**PK:** `(sample_id, label_task, target_ref_hash, labeler_id, run_id)`
**파티션:** `date(labeled_at)`, `method`
**클러스터링:** `sample_id` (조인 패턴이 sample_id 기준)

`target_ref`는 STRUCT로 저장하되, PK에는 그 정규화 해시(`target_ref_hash`)를 쓴다.

```
target_ref: STRUCT<
  kind: STRING,        -- 'whole' | 'span' | 'bbox' | 'cell'
  start: BIGINT, end: BIGINT,          -- span
  x: DOUBLE, y: DOUBLE, w: DOUBLE, h: DOUBLE,  -- bbox
  key: STRING          -- cell/필드 경로
>
```

### 4.3 `gold_label_consensus` (L2)

| 컬럼 | 타입 | 비고 |
|------|------|------|
| `sample_id`, `label_task`, `target_ref_hash`, `ontology_ver` | | 식별 |
| `value_type`, `value_class`, `value_num`, `value_json` | | 합의 값 |
| `soft_dist` | MAP<STRING,DOUBLE> | 클래스별 사후 점수 |
| `margin` | DOUBLE | 1위−2위 점수 차 |
| `flag` | STRING | `agreed` / `soft_disagreement` / `pending_review` / `no_signal` / `human_final` |
| `agreement` | STRUCT | **초고에서 L1→L2로 이동**. 아래 참조 |
| `fusion_policy_ver` | STRING | |
| `theta` | DOUBLE | 적용된 임계값 |
| `calibration_model_ver` | STRING | |
| `input_l1_run_ids` | ARRAY<STRING> | 재생용 |
| `input_l1_label_ids` | ARRAY<STRING> | 정밀 재생용 |
| `l2_version` | BIGINT | 재발행 시 증가 |
| `fused_at` | TIMESTAMP | |

```
agreement: STRUCT<
  n_candidates: INT,
  n_abstain: INT,
  unanimous: BOOLEAN,
  per_labeler: ARRAY<STRUCT<labeler_id: STRING,
                            value_class: STRING,
                            conf_raw: DOUBLE,
                            conf_cal: DOUBLE,
                            weight: DOUBLE>>,
  pairwise_agree: MAP<STRING, DOUBLE>   -- 'A|B' -> 일치 여부/정도
>
```

### 4.4 `gold_label_verified` (L3)

| 컬럼 | 타입 | 비고 |
|------|------|------|
| 식별 컬럼 | | L2와 동일 |
| `value_*` | | 검수 확정 값 |
| `reviewer_id` | STRING | **가명화된 ID.** 원본은 별도 매핑 테이블 |
| `review_round` | INT | 이중 검수 회차 |
| `adjudicated` | BOOLEAN | 검수자 간 불일치를 중재했는지 |
| `l3_purpose` | STRING | `train` / `eval_holdout` — **Fusion Engine은 `train`만 조회** |
| `source` | STRING | `disagreement` / `random_anchor` / `escalation` |
| `anchor_set_id` | STRING | `random_anchor`인 경우 |
| `review_cost`, `review_seconds` | | 비용 측정 |
| `reviewed_at` | TIMESTAMP | |

`l3_purpose` 배정은 `hash(sample_id || label_task) mod 100 < holdout_pct` 로 결정론적으로
수행한다. 무작위 난수를 쓰면 재실행 시 분할이 달라져 누수가 발생한다.

### 4.5 누수 방지 계약

- Fusion Engine이 읽는 L3 뷰: `v_l3_train` (`l3_purpose = 'train'` 필터 고정)
- 보정 모델 학습이 읽는 뷰: `v_l3_train`
- 평가·드리프트가 읽는 뷰: `v_l3_holdout`
- 두 뷰를 동시에 참조하는 코드 경로는 CI에서 정적 검사로 금지

---

## 5. 재생(replay) 규격

### 5.1 정규화 직렬화 (`canon`)

모든 해시의 전제. 구현은 `labelpipe.core.hashing`.

1. 문자열은 UTF-8, 유니코드 **NFC** 정규화
2. 객체 키는 코드포인트 기준 오름차순 정렬
3. JSON 직렬화 시 구분자 `(',', ':')` — 공백 없음
4. 부동소수: 유효숫자 12자리에서 banker's rounding 후 `repr`. `-0.0`은 `0.0`으로
5. 결측/NaN은 JSON `null`
6. 배열은 순서 보존 (순서가 의미 없으면 호출부에서 정렬 후 전달)
7. 해시는 `sha256(canon(x).encode('utf-8')).hexdigest()`

### 5.2 해시 정의

| 해시 | 대상 |
|------|------|
| `inputs_hash` | `{k: sample[k] for k in labeler.input_spec}` — **선언된 입력만** |
| `prompt_hash` | 최종 렌더링된 프롬프트 전문 |
| `prompt_template_hash` | 템플릿 원문 (프롬프트 변경 추적용, `labeler_registry`에 보관) |
| `params_hash` | `{temperature, top_p, seed, max_tokens, stop, response_format}` |
| `retrieval_ctx_hash` | `[{doc_id, doc_ver, chunk_id}, ...]` (검색 순서 보존) |
| `target_ref_hash` | `target_ref` STRUCT |

`input_spec`을 좁게 선언하는 것이 중요하다. 전체 행을 해시하면 무관한 컬럼 추가가 모든 라벨의
해시를 무효화한다.

### 5.3 시점 이동 보존 설정

라벨이 참조하는 feature 테이블과 라벨 테이블 전체에 적용:

```sql
-- Delta
ALTER TABLE <t> SET TBLPROPERTIES (
  'delta.logRetentionDuration'          = 'interval 730 days',
  'delta.deletedFileRetentionDuration'  = 'interval 730 days'
);
-- Iceberg
ALTER TABLE <t> SET TBLPROPERTIES (
  'history.expire.max-snapshot-age-ms'  = '63072000000',   -- 730d
  'history.expire.min-snapshots-to-keep' = '100'
);
```

**기본값(Delta 7일/30일)으로 운용하면 VACUUM이 재현성을 조용히 파괴한다.** 이 설정은 선택이
아니라 설계 전제이며, CI에서 테이블 속성을 검증한다(§14.4).

### 5.4 재생 절차

```
replay(label_id):
  l1   ← silver_label_candidate[label_id]
  raw  ← bronze_label_raw[l1.raw_response_ref]
  feat ← features @ l1.feature_snapshot_id  WHERE sample_id = l1.sample_id
  assert hash_inputs(project(feat, labeler.input_spec)) == l1.inputs_hash
  l1'  ← parser[labeler.schema_ver].parse(raw.response_payload)
  return diff(l1, l1')     -- 파서 변경의 영향만 분리해 관측
```

외부 API 재호출 없이 파싱 로직 변경의 영향을 전수 평가할 수 있다. 이것이 L1/L0 영구 보존의
직접적 효용이며 평가 축 2(§15)의 측정 대상이다.

---

## 6. 온톨로지 버전 관리

클래스 체계는 반드시 바뀐다. `ontology_ver`와 별도로 마이그레이션 맵을 둔다.

```
ontology_migration(from_ver, to_ver, from_class, to_class, kind, confidence)
  kind ∈ {identity, rename, merge, split, drop}
```

- `identity` / `rename` / `merge`: 구 라벨을 자동 사상. L1은 불변이므로 **읽기 시점 뷰에서
  사상**하고 원본은 보존한다
- `split`: 자동 사상 불가. 해당 표본은 `pending_review`로 재라우팅
- `drop`: 학습 집합에서 제외하되 매니페스트에 제외 건수 기록 (P4)

혼합 온톨로지 합의는 금지한다. Fusion Engine은 `ontology_ver`가 동일한 후보끼리만 합의하며,
다른 버전의 후보는 뷰 레벨에서 목표 버전으로 사상된 뒤 투입된다.

---

## 7. Label Fusion Engine

### 7.1 실행 단위

`(label_task, ontology_ver)` 별로 독립 실행. 입력은 "해당 태스크의 최신 유효 L1 집합"이다.
L1은 append-only이므로 최신본 뷰가 필요하다.

```sql
CREATE OR REPLACE VIEW v_l1_latest AS
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY sample_id, label_task, target_ref_hash, labeler_id
    ORDER BY labeled_at DESC, run_id DESC
  ) AS rn
  FROM silver_label_candidate
  WHERE parse_status <> 'failed'
) WHERE rn = 1;
```

`run_id`를 명시하면 "그때의 L1 집합"으로 과거 L2를 재생할 수 있다.

### 7.2 알고리즘 (논문 알고리즘 1의 구현 형태)

```python
def fuse(candidates, ctx) -> ConsensusLabel:
    active = [c for c in candidates if not c.is_abstain]
    if not active:
        enqueue(ctx.sample_id, ctx.label_task, priority=Priority.LOW,
                reason="no_signal")
        return ConsensusLabel(value=None, flag="no_signal",
                              agreement=summarize(candidates), **ctx.meta)

    for c in active:
        c.conf = ctx.calibration[c.labeler_id](c.confidence_raw)

    w = correlation_adjusted_weights(active, ctx.corr_matrix)
    scores = defaultdict(float)
    for c, wc in zip(active, w):
        scores[c.value_key] += wc * c.conf
    p = normalize(scores)

    top, second = top_two(p)
    margin = p[top] - (p[second] if second else 0.0)

    if all_same(active):
        flag = "agreed"
    elif margin >= ctx.theta:
        flag = "soft_disagreement"
    else:
        flag = "pending_review"
        enqueue(ctx.sample_id, ctx.label_task,
                priority=route_priority(margin, ctx.class_rarity[top],
                                        ctx.review_cost),
                reason="disagreement")

    return ConsensusLabel(value=top, soft_dist=p, margin=margin, flag=flag,
                          agreement=summarize(candidates), **ctx.meta)
```

핵심: **`pending_review`에서도 `value`를 비우지 않는다.** 학습 집합 포함 여부는 매니페스트의
필터 조건이 결정한다(P4).

### 7.3 상관 보정

동일 기반 모델을 공유하는 라벨러가 중복 투표권을 갖는 문제를 완화한다.

1. `v_l1_latest`에서 라벨러 쌍 `(a, b)`의 조건부 일치율 `ρ_ab`를 최근 N일 윈도우로 추정
2. 유사도 행렬 `S = [ρ_ab]`에서 유효 표본 수 개념으로 가중치 축소
   `w_a = 1 / (1 + Σ_{b≠a} max(0, ρ_ab − ρ_baseline))`
   여기서 `ρ_baseline`은 클래스 사전분포에서 기대되는 우연 일치율
3. 가중치는 `calibration_model`과 함께 버전 고정되어 L2에 기록

더 정교한 대안(Snorkel 계열 생성 모델)은 `FusionPolicy` 플러그인으로 교체 가능하게 둔다.
초기 구현은 위 휴리스틱으로 충분하며, 평가에서 생성 모델과 비교한다(§18).

### 7.4 모니터링 지표 (합의 정책과 분리)

일치도 계수는 표본 단위 판정에 쓰지 않고 라벨러 건강도 대시보드에만 사용한다.

| 지표 | 용도 |
|------|------|
| Krippendorff's α | 전체 라벨러 집합의 일치도. 3인 이상 + 기권(결측) 허용 |
| Fleiss' κ | 기권이 없는 부분집합의 보조 지표 |
| 라벨러별 커버리지 | `1 − (기권 수 / 전체)` |
| 라벨러별 충돌률 | 다른 라벨러 다수와 불일치한 비율 |
| holdout L3 대비 정확도 | 라벨러 개별 성능 |

---

## 8. 신뢰도 보정 (Calibration)

### 8.1 목적

`confidence_raw`는 라벨러마다 척도가 달라 직접 비교가 불가능하다(개정 A3). 앵커 집합을 기준으로
라벨러별 단조 사상 `g_m: raw → P(correct)`를 학습한다.

### 8.2 학습

- 데이터: `v_l3_train` ⋈ `v_l1_latest` (동일 sample/task/target)
- 라벨러별로 `(confidence_raw, is_correct)` 쌍 구성
- 모델: isotonic regression (단조 제약). 표본 부족 시 Platt scaling으로 폴백
- **최소 표본 수 `n_min`(기본 200) 미만이면 항등 사상 + 균등 가중으로 폴백** (부트스트랩 경로)
- 산출물은 `calibration_model`에 버전과 함께 적재. L2가 `calibration_model_ver`를 참조

### 8.3 앵커 편향 보정

앵커 집합은 무작위 표본이므로 보정 학습에 그대로 사용 가능하다. 반면 불일치 기원 L3
(`source='disagreement'`)를 보정 학습에 섞으면 난이도 상위 구간에 과적합되므로 **기본적으로
제외**하고, 포함할 경우 역확률 가중(inverse propensity weighting)을 적용한다.

### 8.4 재학습 주기

- 정기: 주 1회
- 트리거: 라벨러 `method_ver` 변경, 앵커 집합 확장 20% 이상, 드리프트 경보

---

## 9. 검수 라우팅과 앵커 집합

### 9.1 예산 배분

주기별 검수 예산 `B` (건수 또는 비용)를 다음으로 나눈다.

| 경로 | 비율 | 목적 |
|------|------|------|
| `disagreement` | `1 − ρ − ε` | 학습 데이터 품질 향상 |
| `random_anchor` | `ρ` (기본 0.15) | 보정·평가·드리프트 기준선 |
| `escalation` | `ε` (기본 0.05) | 검수자 간 불일치 중재, 이의 제기 |

`ρ`는 설정값이며 0으로 설정할 수 없도록 검증한다(P5).

### 9.2 앵커 표본 추출

```
anchor_sample(period):
  frame ← 해당 기간의 전체 대상 모집단 (불일치 여부 무관)
  strata ← feature 분위수 × 예측 클래스   -- 층화로 희소 클래스 확보
  seed  ← hash(anchor_set_id)             -- 결정론적, 재추출 가능
  return stratified_sample(frame, strata, n = B * ρ, seed = seed)
```

앵커는 `anchor_set` 테이블에 고정 기록되며, 드리프트 재채점 시 **동일 표본**을 다시 채점한다.
앵커 집합은 6개월 주기로 갱신하되 직전 집합과 중첩 구간을 두어 비교 가능성을 유지한다.

### 9.3 우선순위 함수

```
priority = w1 * (1 − margin)            # 불확실성
         + w2 * class_rarity_bonus      # 희소 클래스 가중
         + w3 * downstream_impact       # 해당 표본의 학습 영향도(옵션)
         − w4 * normalized_review_cost  # 검수 난이도/비용
```

가중치는 `fusion_policy`에 버전과 함께 저장한다.

### 9.4 이중 검수와 중재

- 앵커 표본의 일정 비율(기본 20%)은 2인 검수 → 검수자 간 일치도 측정
- 불일치 시 `escalation` 경로로 3인째 중재자에게. 결과는 `adjudicated=true`
- 검수자별 정확도(중재 결과 대비)를 추적하여 검수자 신뢰도 관리

### 9.5 폐루프 안전장치

검수 결과를 규칙 추가·프롬프트 수정에 재투입할 때, **변경 후 라벨러는 반드시 기존 앵커
집합에서 회귀 검증을 통과해야 한다**. 통과 기준은 앵커 정확도 비열화(신뢰구간 기준). 이 게이트가
없으면 국소 개선이 전역 열화를 낳아도 감지되지 않는다.

---

## 10. 드리프트 감지

### 10.1 신호 1 — 분포 변화

```
for each (labeler_id, label_task, feature_bucket):
    p_ref  ← 기준 기간의 L1 클래스 분포
    p_cur  ← 현재 기간의 L1 클래스 분포
    PSI    ← Σ_i (p_cur_i − p_ref_i) * ln(p_cur_i / p_ref_i)
```

- 0 표본 클래스에 대해 `ε = 1e-6` 스무딩 (무한대 방지)
- 버킷별 최소 표본 수 `n_bucket_min`(기본 200) 미만이면 산출하지 않음
- 다중 비교: 라벨러×태스크×버킷 조합 수가 크므로 Benjamini–Hochberg FDR 보정 적용
- 임계: PSI < 0.1 안정, 0.1–0.25 주의, > 0.25 경보

### 10.2 신호 2 — 앵커 재채점

고정 앵커 집합을 현재 `method_ver`로 재채점하여 정확도 변화를 추적한다.
비용이 크므로 앵커 부분표본(기본 30%)을 주 1회 순환 채점한다.

### 10.3 식별 규칙 (논문 표 3)

| PSI | 앵커 정확도 | 판정 | 자동 조치 |
|-----|------------|------|-----------|
| 상승 | 유지 | 입력 드리프트 | 알림만. feature 모니터로 에스컬레이션 |
| 상승 | 하락 | 라벨러 드리프트 | 라벨러 격리 + republish 후보 등록 |
| 유지 | 하락 | 점진 열화 / 앵커 노후화 | 앵커 갱신 후 재평가 |
| 유지 | 유지 | 안정 | 없음 |

### 10.4 republish

- 덮어쓰기가 아니라 `l2_version + 1` 생성
- 기존 매니페스트는 이전 버전을 계속 참조하므로 진행 중인 실험에 영향 없음
- republish 이벤트는 `drift_metric`과 `dataset_manifest` 양쪽에 기록

---

## 11. 데이터셋 매니페스트

### 11.1 스키마

| 컬럼 | 설명 |
|------|------|
| `dataset_id` | UUIDv7 |
| `label_task`, `ontology_ver` | |
| `feature_snapshot_id` | feature 테이블 버전 |
| `l2_version`, `l3_version` | 라벨 버전 |
| `fusion_policy_ver`, `theta`, `calibration_model_ver` | 정책 버전 |
| `filter_predicate` | 학습 집합 선택 조건 (SQL 문자열) |
| `include_pending` | 보류 표본 포함 여부 |
| `pending_weight` | 포함 시 샘플 가중치 |
| `row_count`, `class_distribution` | |
| `excluded_count`, `excluded_reason_breakdown` | **P4. 배제 사유별 건수** |
| `content_hash` | 구성 결과의 결정론 검증용 |
| `created_at`, `created_by` | |

### 11.2 빌더 계약

```python
def build(manifest: DatasetManifest) -> DataFrame:
    """동일 manifest → 동일 content_hash. 이 불변식은 CI에서 검증한다."""
```

빌더는 시각 기반 함수(`now()`)나 비결정적 셔플을 사용하지 않는다. 셔플이 필요하면 `dataset_id`를
seed로 사용한다.

### 11.3 보류 표본 정책 (선택지를 명시적으로)

| 정책 | 설정 | 사용처 |
|------|------|--------|
| 배제 | `include_pending=false` | 보수적 baseline. 배제 건수는 반드시 기록 |
| 소프트 라벨 포함 | `include_pending=true, pending_weight<1` | 선택 편향 완화. 권장 기본값 |
| 전량 포함 | `pending_weight=1` | 검수 지연이 큰 초기 운영 |

정책 간 다운스트림 성능 차이는 평가 항목이다(§18).

---

## 12. 멱등성·재실행·지연 도착

### 12.1 멱등 키

라벨러 실행은 `(run_id, sample_id, label_task, target_ref_hash, labeler_id)`로 멱등하다.
재시도가 같은 `run_id`를 유지하면 중복 적재는 dedup으로 흡수된다.

```sql
MERGE INTO silver_label_candidate t
USING staged s
ON  t.run_id = s.run_id AND t.sample_id = s.sample_id
AND t.label_task = s.label_task AND t.target_ref_hash = s.target_ref_hash
AND t.labeler_id = s.labeler_id
WHEN NOT MATCHED THEN INSERT *;
```

`WHEN MATCHED THEN UPDATE`는 두지 않는다 (P1).

### 12.2 부분 실패

- 라벨러 단위 체크포인트. 실패한 라벨러만 동일 `run_id`로 재개
- Bronze에는 실패 응답도 적재(`error` 컬럼) — 실패 패턴 분석이 라벨러 개선의 입력
- `parse_status='failed'`인 L1은 적재하되 `v_l1_latest`에서 제외

### 12.3 LLM 호출 운영

| 항목 | 정책 |
|------|------|
| 동시성 | 라벨러별 세마포어. 전역 토큰/분 상한 |
| 재시도 | 지수 백오프 (2s→4s→8s→16s), 최대 4회. 429/5xx만 |
| 비용 상한 | run 단위 `max_cost_usd`. 초과 시 circuit break 후 부분 커밋 |
| 타임아웃 | 요청별 + run 전체 |
| 캐시 | `(inputs_hash, method_ver, prompt_hash, params_hash)` 적중 시 재호출 생략 (설정으로 on/off) |

캐시는 비용을 크게 줄이지만 드리프트 관측을 가리므로, 앵커 재채점 경로에서는 **항상 비활성화**한다.

### 12.4 지연 도착 라벨

사람 검수 L3는 며칠 뒤 도착한다.

- L3 도착 시 해당 `(sample_id, label_task)`의 L2를 `human_final`로 갱신 (새 `l2_version`)
- 갱신은 해당 파티션에 한정된 증분 fusion
- 이미 발행된 매니페스트는 갱신되지 않음. 신규 매니페스트부터 반영

---

## 13. 저장·성능·비용

### 13.1 부피 추정

`|L1| ≈ N × K × R` (표본 수 × 라벨러 수 × 재실행 횟수). 부피의 대부분은 `rationale`
(LLM 설명 텍스트)과 Bronze 원시 응답이다.

### 13.2 대응

| 조치 | 효과 |
|------|------|
| `rationale`을 `silver_label_rationale`로 분리, L1은 `rationale_ref`만 보유 | 합의 연산이 rationale을 읽지 않으므로 스캔량 대폭 감소 |
| Bronze `response_payload`에 zstd 압축 | 텍스트 압축률 높음 |
| 파티션 `date(labeled_at) × method` | 시간 범위 질의 프루닝 |
| `sample_id` 클러스터링(Z-order / sort order) | 조인 성능 |
| compaction 일 1회 | 라벨러 병렬 실행이 유발하는 small file 문제 |
| 90/180일 경과분 cold tier 이관 | 비용. 메타(L1 본체)는 hot 유지, rationale/Bronze만 이관 |

### 13.3 보존 정책

| 대상 | hot | cold | 삭제 |
|------|-----|------|------|
| Bronze 원시 응답 | 180일 | 이후 | 없음 (법적 요건 시 별도) |
| L1 본체 | 무기한 | — | 없음 |
| rationale | 90일 | 이후 | 없음 |
| L2/L3 | 무기한 | — | 없음 |
| 테이블 스냅샷 | 730일 | — | §5.3 |

**"폐기하지 않는다"의 비용은 평가 축 2에서 정량화한다(§18).**

---

## 14. 거버넌스

### 14.1 PII

- `rationale`과 Bronze `request_payload`에 원문 인용이 유입된다. L1은 feature보다 접근 통제가
  느슨해지기 쉬우므로 **feature와 동일한 컬럼 레벨 ACL을 적용**(조건 C6)
- LLM 전송 전 PII 마스킹 파이프라인을 라벨러 어댑터에 내장. 마스킹 규칙 버전도 `method_ver`에 포함

### 14.2 검수자 정보

- `reviewer_id`는 가명. 원본 매핑은 별도 테이블 + 제한 권한
- 검수자별 성능 지표는 개인 평가 목적 사용을 금지하는 접근 정책과 함께 운영

### 14.3 외부 전송

- LLM 라벨러는 데이터를 외부로 내보낸다. 도메인(재난 안전)에 따라 온프레미스 모델 옵션 필요
- 라벨러 레지스트리에 `data_egress: internal | external` 플래그. 외부 라벨러는 민감 태스크에
  배정 불가하도록 정책 검증

### 14.4 CI 검증 항목

| 검사 | 내용 |
|------|------|
| 테이블 속성 | 스냅샷 보존 기간이 §5.3 기준 이상인지 |
| 누수 | `v_l3_holdout`을 fusion/calibration 코드 경로가 참조하지 않는지 |
| 앵커 비율 | `rho > 0` 인지 |
| `method_ver` 형식 | LLM 라벨러가 alias가 아닌 날짜 고정 ID를 쓰는지 |
| 스키마 호환성 | L1 schema evolution이 하위 호환인지 |

---

## 15. 모듈 구조와 인터페이스 계약

### 15.1 패키지 레이아웃

```
labelpipe/
  core/
    schema.py         # dataclass/enum 정의, 스키마 상수
    hashing.py        # canon() 및 해시 함수 (§5.1–5.2)
    ids.py            # UUIDv7, run_id 생성
    errors.py
  labelers/
    base.py           # Labeler 프로토콜, CandidateLabel
    rule.py           # 규칙 엔진 어댑터
    llm.py            # LLM 어댑터 (재시도/비용/캐시 포함)
    human.py          # 검수 플랫폼 결과 수집기
    registry.py       # labeler_registry 로딩·검증
    masking.py        # PII 마스킹 (§14.1)
  storage/
    base.py           # LabelStore 프로토콜
    delta.py          # Delta 구현
    iceberg.py        # Iceberg 구현
    views.py          # v_l1_latest, v_l3_train, v_l3_holdout DDL 생성
  fusion/
    engine.py         # §7.2
    correlation.py    # §7.3
    calibration.py    # §8
    policies/
      base.py         # FusionPolicy 프로토콜
      majority.py
      weighted.py
      generative.py   # Snorkel 계열 (선택)
  review/
    queue.py
    routing.py        # 우선순위 + 앵커 배분 (§9)
    anchor.py         # 층화 표본 추출
    adjudication.py
  drift/
    psi.py
    anchor_rescore.py
    detector.py       # §10.3 식별 규칙
  dataset/
    manifest.py
    builder.py        # §11.2
  metrics/
    agreement.py      # Krippendorff α, Fleiss κ
    labeler_health.py
  cli.py
  config.py
```

### 15.2 핵심 인터페이스

```python
# labelers/base.py
class Labeler(Protocol):
    labeler_id: str
    method: Method                 # rule | llm | human | hybrid
    method_ver: str                # LLM은 날짜 고정 ID만 허용
    input_spec: tuple[str, ...]    # 해시 대상 컬럼 (§5.2)
    ontology_ver: str
    label_task: str
    data_egress: Egress            # internal | external

    def label(self, batch: Sequence[Sample],
              run: RunContext) -> Iterator[LabelOutput]:
        """LabelOutput = (RawRecord, CandidateLabel | None).

        - 파싱 실패 시 CandidateLabel은 None이 아니라
          parse_status='failed'인 레코드로 반환한다 (실패도 관측 대상).
        - 판단 유보는 is_abstain=True로 표현한다. None 반환 금지.
        """

    def fingerprint(self) -> LabelerFingerprint:
        """method_ver, prompt_template_hash, params_hash 등 재생 메타."""
```

```python
# storage/base.py
class LabelStore(Protocol):
    def append_raw(self, records: Iterable[RawRecord]) -> None: ...
    def append_candidates(self, labels: Iterable[CandidateLabel]) -> int:
        """멱등. 반환값은 실제 삽입 행 수 (§12.1)."""
    def read_candidates(self, task: str, *, run_id: str | None = None,
                        window: TimeWindow | None = None
                        ) -> Iterator[CandidateLabel]: ...
    def write_consensus(self, labels: Iterable[ConsensusLabel],
                        l2_version: int) -> None: ...
    def read_verified(self, task: str, purpose: Purpose
                      ) -> Iterator[VerifiedLabel]:
        """purpose는 필수 인자. 기본값을 두지 않는다 (§4.5 누수 방지)."""
    def feature_snapshot_id(self, table: str) -> str: ...
```

```python
# fusion/policies/base.py
class FusionPolicy(Protocol):
    policy_ver: str
    def fuse(self, candidates: Sequence[CandidateLabel],
             ctx: FusionContext) -> ConsensusLabel: ...

# fusion/calibration.py
class Calibrator(Protocol):
    model_ver: str
    def fit(self, pairs: Sequence[tuple[float, bool]]) -> None: ...
    def transform(self, conf_raw: float) -> float:
        """단조 비감소. 표본 부족 시 항등 사상 (§8.2)."""
```

### 15.3 CLI

```
labelpipe run-labelers   --task T --run-id R [--labeler L]...
labelpipe fuse           --task T [--run-id R] [--policy P]
labelpipe sample-anchor  --task T --period P
labelpipe calibrate      --task T
labelpipe detect-drift   --task T --ref-period A --cur-period B
labelpipe build-dataset  --manifest M.yaml
labelpipe replay         --label-id ID
labelpipe verify-tables  # §14.4 CI 검사
```

---

## 16. 설정

예시: [`config/pipeline.example.yaml`](config/pipeline.example.yaml)

정책성 값은 모두 설정으로 노출하되, 설정 파일 자체를 해시하여 `fusion_policy_ver`에 반영한다.
"코드는 같은데 동작이 다른" 상황을 만들지 않기 위함이다.

---

## 17. 테스트 전략

### 17.1 단위·속성 테스트

| 대상 | 검증 |
|------|------|
| `canon()` | 키 순서·공백·유니코드 표기가 달라도 동일 해시 (property test) |
| `canon()` 부동소수 | `0.1+0.2`와 `0.3`이 12자리 규칙에서 동일 처리되는지 |
| `is_abstain` | 기권이 합의에서 제외되고 `no_signal`과 불일치가 구분되는지 |
| `Calibrator` | 출력이 단조 비감소, 표본 부족 시 항등 |
| 상관 보정 | 완전 상관 라벨러 2개 = 독립 라벨러 1개에 수렴 |
| PSI | 알려진 분포 쌍에 대한 해석해 일치, 0 표본 스무딩 |
| 우선순위 함수 | margin 감소 시 우선순위 단조 증가 |

### 17.2 골든 테스트

`fusion/testdata/`에 후보 집합 → 기대 L2 픽스처를 둔다. 정책 변경 시 골든 파일 갱신이
강제되므로 의도치 않은 동작 변화가 드러난다.

### 17.3 통합 테스트

| 시나리오 | 기대 |
|----------|------|
| 동일 `run_id`로 2회 실행 | L1 행 수 불변 (멱등, §12.1) |
| `run_id` 변경 후 실행 | 새 행 추가, `v_l1_latest`는 최신만 노출 |
| 동일 매니페스트로 2회 빌드 | `content_hash` 동일 (§11.2) |
| 파서 변경 후 replay | 외부 호출 0회, diff만 산출 (§5.4) |
| holdout 참조 | fusion/calibration이 `v_l3_holdout`을 읽으면 테스트 실패 |
| 온톨로지 `split` 마이그레이션 | 해당 표본이 `pending_review`로 재라우팅 |

### 17.4 데이터 품질 게이트 (조건 C5)

L1/L2 적재 시 실행되는 expectation:

- `is_abstain=true`이면 `value_*`는 모두 NULL
- `value_class`는 `ontology_ver`의 허용 클래스 집합에 속함
- `confidence_raw ∈ [0,1]`
- LLM 라벨러 행은 `prompt_hash`, `params_hash`가 NOT NULL
- L2의 `input_l1_label_ids`가 실제 존재하는 L1을 가리킴 (참조 무결성)

---

## 18. 평가 실험 설계

### 18.1 축 1 — 합의 품질과 검수 비용

| 조건 | 설명 |
|------|------|
| B0 | 단일 LLM 라벨러 |
| B1 | 단순 다수결 (보정·상관 보정 없음) |
| B2 | Snorkel 계열 생성 모델 |
| B3 | 전수 사람 검수 (품질 상한) |
| **P** | 제안 (보정 + 상관 보정 + 불일치 라우팅) |

지표: holdout L3 대비 L2 정확도/F1, Krippendorff's α, 동일 다운스트림 성능 도달까지의 검수
건수, 다운스트림 모델 F1.

**ablation:** 보정 제거, 상관 보정 제거, 앵커 비율 ρ ∈ {0, 0.05, 0.15, 0.30},
보류 표본 정책 3종(§11.3).
ρ=0 조건은 앵커 없는 운영이 라벨러 정확도 추정을 얼마나 낙관 편향시키는지 보이는 데 사용한다.

### 18.2 축 2 — 후보 라벨 영구 보존의 효용 (고유 주장)

| 실험 | 측정 |
|------|------|
| E1 라벨러 회귀 검증 | LLM `method_ver` 교체 시, 보존 L1 기반 회귀 분석 비용 vs 전량 재호출 비용 (USD, 소요 시간) |
| E2 사후 ablation | "라벨러 X를 제외했다면 L2가 어떻게 달라지는가"를 재실행 없이 산출. 보존 없을 때는 전량 재라벨링 필요 → 비용 대비 |
| E3 파서 변경 replay | Bronze 보존으로 외부 호출 0회 달성 여부와 소요 시간 (§5.4) |
| E4 손익분기 | 저장 오버헤드(GB, USD/월) vs E1–E3 절감액의 손익분기 시점 |

축 2가 이 설계의 차별점을 직접 증명하는 부분이므로, 축 1보다 우선 보고한다.

---

## 19. 구현 로드맵

| 단계 | 산출물 | 완료 기준 |
|------|--------|-----------|
| M1 | `core.schema`, `core.hashing`, DDL 적용, 테이블 속성 검증 | §17.1 해시 속성 테스트 통과, `verify-tables` 통과 |
| M2 | rule 라벨러 1 + LLM 라벨러 1, Bronze/L1 적재, 멱등성 | 동일 `run_id` 2회 실행 시 행 수 불변 |
| M3 | 앵커 표본 추출 + 검수 연동 + L3 적재, `l3_purpose` 분할 | 앵커 집합이 층화 조건을 만족, holdout 격리 테스트 통과 |
| M4 | Fusion Engine (다수결) + L2 적재 + `v_l1_latest` | 골든 테스트 통과, `pending_review`에 value 보존 |
| M5 | 보정 + 상관 보정 | 보정 단조성·부트스트랩 폴백 테스트 통과 |
| M6 | 데이터셋 매니페스트 + 빌더 | 동일 매니페스트 재빌드 시 `content_hash` 일치 |
| M7 | 드리프트 2채널 + 식별 규칙 + republish | 합성 드리프트 주입 시 표 3의 4분면이 올바르게 분류됨 |
| M8 | 계보 도구 연동, 대시보드, 평가 실험 | 조건 C4/C7 충족 입증, §18 결과 산출 |

M3을 M4보다 앞에 두는 것이 중요하다. 앵커가 없으면 M5의 보정이 학습할 데이터가 없고, M7의
드리프트 기준선도 없다.

---

## 20. 열린 이슈

| # | 이슈 | 현재 입장 |
|---|------|-----------|
| O1 | 테이블 포맷 선택 (Delta vs Iceberg) | `LabelStore` 추상으로 분리. MERGE 성능과 기존 레이크 표준에 따라 결정 |
| O2 | 상관 보정 휴리스틱 vs 생성 모델 | 휴리스틱으로 시작, §18.1에서 B2와 비교 후 결정 |
| O3 | 앵커 비율 ρ의 적정값 | 15% 가정. ablation으로 도메인별 결정 |
| O4 | 보류 표본 기본 정책 | 소프트 라벨 포함(가중치 < 1) 권장. 실증 필요 |
| O5 | rationale 장기 보존 가치 | 90일 hot 이후 cold. 실제 사후 분석 빈도를 측정해 재조정 |
| O6 | 멀티모달 `target_ref` | bbox까지만 정의. 비디오 시공간 구간은 후속 |
| O7 | 온톨로지 `split` 자동화 | 현재 전량 재검수. LLM 보조 분할은 후속 |
