# HANDOFF GUIDE (프로그램 인수인계 지침)

## 1) 목표
- 목적: 차량 판매가/낙찰가를 분석해서 **수익이 남는 최적 입찰가** 도출.
- 데이터 소스:
  - 헤이딜러 딜러용 `auction_type=customer_zero` (제로 경매만)
  - 엔카 비교매물

## 2) 절대 원칙
- 수입차는 앞으로 신규 수집하지 않음 (국산차만).
- 중복 저장 금지.
- 트림/세부 스펙 혼합 금지:
  - 같은 모델 + 같은 트림 계열
  - 배기량(예: 1.6 vs 2.0) 분리
  - 연료/구동/터보/변속 분리
- 표본 부족 시 억지 추정 금지, `없음`/사유 표시.

## 3) 현재 반영된 핵심 로직
- 파일: `app.py`
- 로직 버전 문자열: `LOGIC_VERSION = "2026-03-05-encar-strict-v4"`

### 엔카(fetch_market)
- 연식: 입력 연식 **고정**.
- 주행거리 범위: `기준 - 20,000 ~ 기준 + 30,000` (최소 0).
- 국산차 타입 기준(`CarType=Y`).
- 일반등록 기준(`SellType`에 `일반` 포함).
- 옵션은 “필터”가 아니라 “매칭 점수”로 반영.
- 중복 제거: `vehicle_id` 기준 1건.
- 디버그 출력에 `raw_count`, `strict_count`, `dedup_vehicle_count`, `scanned_pages`, `logic_version`, `fetch_errors` 포함.

### 제로경매 입찰가(estimate_optimal_bid)
- `source_url LIKE 'heydealer://%'` 데이터만 사용.
- 같은 모델/트림 + 스펙 분리(배기량/연료/구동/터보/변속) 강제.
- 연식 고정 + 주행거리 범위(`-2만 ~ +3만`) 적용.
- 표본 부족 시 추천입찰가 `None` + 사유 반환.

## 4) 옵션 매칭
- 입력 텍스트의 신차 추가옵션을 키워드로 정규화(`style`, `navigation`, `drivewise` 등).
- 엔카 상세의 `options.choice` 코드/본문 텍스트와 매칭.
- 비교표에 `옵션매칭 (x/y)` 표시.

## 5) 데이터/수집 운영
- DB: `data.db`
- 진행 파일: `data/sync/all_zero_progress.json`
- 수집 스크립트:
  - `collect_all_zero.py` (제로 전체 수집, 중단/재개 가능)
  - `import_heydealer_by_url.py` (URL 단위 수집)
- 수집 정책:
  - 제로 기준만 수집
  - 중복 저장 금지
  - 수입차 신규 수집 금지

## 6) 실행 방법
- 앱 실행:
  ```powershell
  cd C:\Users\Administrator\Desktop\파이썬\프로그램
  py -3 app.py
  ```
- 브라우저:
  - `http://127.0.0.1:8080`

## 7) 반영 확인 체크리스트
- 화면 `조회근거`에 아래가 보여야 최신 로직 반영 상태:
  - `ver: 2026-03-05-encar-strict-v4`
  - `pages: ...`
  - 필요 시 `fetch_err: ...`
- `ver`가 다르거나 없으면 서버 재시작 후 재검증.

## 8) 다음 우선 작업
- 헤이딜러 `car_spec.description` 파싱 보강:
  - `신차정가(옵션 포함)` 저장
  - `신차 추가옵션명 + 옵션가` 구조 저장
- 보강 모드 실행으로 기존 수집분 업데이트.
