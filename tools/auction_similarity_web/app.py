from __future__ import annotations

import re
import sqlite3
import threading
import webbrowser
import json
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request, render_template_string

DB_PATH = Path(__file__).resolve().parents[2] / "data.db"
OPTION_SYNONYMS_PATH = Path(__file__).resolve().parents[2] / "data" / "sync" / "option_synonyms.json"
_SHARED_SYNONYM_CACHE = None
_SHARED_SYNONYM_MTIME = None

app = Flask(__name__)

HTML = """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>과거 경매 유사매물 조회</title>
  <style>
    body { font-family: "Segoe UI", "Malgun Gothic", sans-serif; margin: 16px; background:#f4f7fb; color:#132538; }
    .card { background:#fff; border:1px solid #d9e3ec; border-radius:12px; padding:12px; margin-bottom:10px; }
    .row { display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:8px; }
    .field { display:flex; flex-direction:column; gap:4px; }
    label { font-size:12px; color:#5f7388; }
    input, textarea, button { border:1px solid #d9e3ec; border-radius:8px; padding:8px; font-size:13px; }
    textarea { min-height:90px; resize:vertical; }
    button { cursor:pointer; font-weight:700; }
    .primary { background:#0f766e; color:#fff; border:0; }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th, td { border-bottom:1px solid #e6edf4; padding:7px; text-align:left; }
    th { color:#607487; font-size:12px; }
    .muted { color:#6b7f93; font-size:12px; }
    .score-high { background:#fff4d6; font-weight:700; color:#9a5b00; }
  </style>
</head>
<body>
  <div class="card">
    <h2 style="margin:0 0 8px">과거 경매 유사매물 조회 (읽기 전용)</h2>
    <div class="muted">기존 분석 프로그램과 분리된 안전 모드입니다. DB 쓰기 없이 조회만 합니다.</div>
    <div class="field" style="margin-top:8px;">
      <label>매물 원문(붙여넣기)</label>
      <textarea id="raw" placeholder="엔카/헤이딜러 매물정보를 그대로 붙여넣으세요. 비워두면 수동 입력값으로 조회합니다."></textarea>
    </div>
    <div class="row" style="margin-top:8px;">
      <div class="field"><label>브랜드</label><input id="brand" placeholder="예: 기아" /></div>
      <div class="field"><label>모델</label><input id="model" placeholder="예: 더 뉴 기아 레이" /></div>
      <div class="field"><label>트림</label><input id="trim" placeholder="예: 시그니처" /></div>
      <div class="field"><label>연식</label><input id="year" type="number" placeholder="예: 2023" /></div>
    </div>
    <div class="row" style="margin-top:8px;">
      <div class="field"><label>주행거리(km)</label><input id="km" type="number" placeholder="예: 21649" /></div>
      <div class="field" style="grid-column: span 3;"><label>옵션(줄바꿈 또는 쉼표)</label><textarea id="opts" placeholder="내비게이션\n열선핸들\n후측방 충돌방지보조"></textarea></div>
    </div>
    <div style="margin-top:8px; display:flex; gap:8px;">
      <button class="primary" id="run">유사매물 찾기</button>
      <button id="clear">초기화</button>
    </div>
  </div>

  <div class="card">
    <div id="summary" class="muted">-</div>
    <table>
      <thead>
        <tr>
          <th>유사도</th><th>브랜드/모델/트림</th><th>연식</th><th>주행</th><th>최고가</th><th>내입찰</th><th>차이(내-최고)</th><th>옵션매칭</th><th>종료일</th>
        </tr>
      </thead>
      <tbody id="rows"><tr><td colspan="9" class="muted">조회 전</td></tr></tbody>
    </table>
  </div>

<script>
const $ = (id) => document.getElementById(id);
function n(v){ return (v===null||v===undefined)?'-':String(v); }

async function run(){
  const payload = {
    raw_text: $('raw').value,
    brand: $('brand').value.trim(),
    model: $('model').value.trim(),
    trim: $('trim').value.trim(),
    year: $('year').value.trim(),
    mileage_km: $('km').value.trim(),
    options_text: $('opts').value,
  };
  const res = await fetch('/api/similar', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
  const data = await res.json();
  if(!res.ok){ $('summary').textContent = data.error || '오류'; return; }
  const p = data.parsed || {};
  if (p.brand) $('brand').value = p.brand;
  if (p.model) $('model').value = p.model;
  if (p.trim) $('trim').value = p.trim;
  if (p.year) $('year').value = p.year;
  if (p.mileage_km) $('km').value = p.mileage_km;
  if ((p.options || []).length && !$('opts').value.trim()) $('opts').value = (p.options || []).join('\\n');
  $('summary').textContent = `표본 ${data.count}건 / 평균 최고가 ${n(data.avg_top_bid)} / 평균 내입찰 ${n(data.avg_my_bid)} / 평균 차이 ${n(data.avg_gap)}`;
  const rows = data.items || [];
  $('rows').innerHTML = rows.length ? rows.map(r => {
    const s = Number(r.score100 || 0);
    const cls = s >= 90 ? 'score-high' : '';
    const star = s >= 90 ? ' ★' : '';
    return `<tr><td class="${cls}">${n(r.score100)}${star}</td><td>${n(r.brand)} / ${n(r.model)} / ${n(r.trim)}</td><td>${n(r.registration_year)}</td><td>${n(r.mileage_km)}</td><td>${n(r.auction_top_bid)}</td><td>${n(r.auction_my_bid)}</td><td>${n(r.my_minus_top)}</td><td title="일치:${(r.option_matched_names||[]).join(', ')} / 미일치:${(r.option_missing_names||[]).join(', ')}">${n(r.option_match_count)}/${n(r.option_target_count)}</td><td>${n(r.auction_end_date)}</td></tr>`;
  }).join('') : '<tr><td colspan="9" class="muted">일치 표본 없음</td></tr>';
}

$('run').onclick = run;
$('clear').onclick = () => { ['raw','brand','model','trim','year','km','opts'].forEach(id => $(id).value=''); $('summary').textContent='-'; $('rows').innerHTML='<tr><td colspan="9" class="muted">조회 전</td></tr>'; };
</script>
</body>
</html>
"""


def normalize_text_key(text: str) -> str:
    s = str(text or "").lower().strip()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^0-9a-z가-힣]+", " ", s)
    return " ".join(s.split())


def parse_options(options_text: str) -> list[str]:
    raw = str(options_text or "")
    parts = re.split(r"[\n,]+", raw)
    out = []
    for p in parts:
        t = p.strip()
        if t:
            out.append(t)
    return list(dict.fromkeys(out))


def _syn_norm(text: str) -> str:
    s = str(text or "").strip().lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"[^0-9a-z가-힣]+", "", s)
    return s


def load_shared_option_synonyms() -> dict[str, str]:
    global _SHARED_SYNONYM_CACHE, _SHARED_SYNONYM_MTIME
    try:
        mtime = OPTION_SYNONYMS_PATH.stat().st_mtime
    except Exception:
        _SHARED_SYNONYM_CACHE = {}
        _SHARED_SYNONYM_MTIME = None
        return _SHARED_SYNONYM_CACHE

    if (_SHARED_SYNONYM_CACHE is not None) and (_SHARED_SYNONYM_MTIME == mtime):
        return _SHARED_SYNONYM_CACHE

    try:
        data = json.loads(OPTION_SYNONYMS_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}

    merged = {}
    for src, dst in (data.get("exact") or {}).items():
        sk = _syn_norm(src)
        dk = _syn_norm(dst)
        if sk and dk:
            merged[sk] = dk
    for src, dst in (data.get("contains") or {}).items():
        sk = _syn_norm(src)
        dk = _syn_norm(dst)
        if sk and dk:
            merged[sk] = dk

    _SHARED_SYNONYM_CACHE = merged
    _SHARED_SYNONYM_MTIME = mtime
    return _SHARED_SYNONYM_CACHE


def normalize_option_key(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    roman = {
        "Ⅰ": "1", "Ⅱ": "2", "Ⅲ": "3", "Ⅳ": "4", "Ⅴ": "5",
        "Ⅵ": "6", "Ⅶ": "7", "Ⅷ": "8", "Ⅸ": "9", "Ⅹ": "10",
        "ⅰ": "1", "ⅱ": "2", "ⅲ": "3", "ⅳ": "4", "ⅴ": "5",
        "ⅵ": "6", "ⅶ": "7", "ⅷ": "8", "ⅸ": "9", "ⅹ": "10",
    }
    for k, v in roman.items():
        s = s.replace(k, v)
    s = re.sub(r"^\d+\)\s*", "", s)
    s = re.sub(r"\([\d,\s]*만원[^)]*\)", " ", s)
    s = re.sub(r"[^0-9a-zA-Z가-힣]+", "", s).lower()

    # Alias normalization (same intent, different label style)
    alias_map = {
        "bsd사각지대감지시스템": "후측방충돌방지보조",
        "후측방충돌방지보조bca": "후측방충돌방지보조",
        "후측방충돌방지보조": "후측방충돌방지보조",
        "후측방충돌방지보조시스템": "후측방충돌방지보조",
        "후측방충돌경고": "후측방충돌방지보조",
        "사각지대경고시스템sbza": "후측방충돌방지보조",
        "사각지대감지": "후측방충돌방지보조",
        "사각지대경고": "후측방충돌방지보조",
        "후측방충돌회피지원absd": "후측방충돌방지보조",
        "내비게이션사제": "내비게이션",
        "내비게이션정품": "내비게이션",
        "순정내비게이션": "내비게이션",
        "8인치내비게이션정품": "내비게이션",
        "10.25인치내비게이션정품": "내비게이션",
        "인포테인먼트내비": "내비게이션",
        "통풍시트앞좌석": "통풍시트",
        "통풍시트운전석": "통풍시트",
        "통풍시트조수석": "통풍시트",
        "열선시트앞좌석": "열선시트",
        "열선시트뒷좌석": "열선시트",
        "스마트크루즈컨트롤scc": "스마트크루즈컨트롤",
        "어댑티브크루즈컨트롤acc": "스마트크루즈컨트롤",
        "어드밴스스마트크루즈컨트롤ascc": "스마트크루즈컨트롤",
        "차선유지보조lkas": "차선유지보조",
        "서라운드뷰모니터svm": "서라운드뷰모니터",
        "헤드업디스플레이hud": "헤드업디스플레이",
        "전자식파킹브레이크epb": "전자식파킹브레이크",
        "led헤드램프": "led헤드램프",
    }
    for src, dst in alias_map.items():
        if src in s:
            s = s.replace(src, dst)
    shared = load_shared_option_synonyms()
    if s in shared:
        s = shared[s]
    for src, dst in shared.items():
        if src in s:
            s = s.replace(src, dst)
    return s.strip()


def option_group_key(norm_key: str) -> str:
    k = norm_key or ""
    if not k:
        return ""
    groups = [
        ("nav", ["내비게이션", "uvo", "인포테인먼트", "네비"]),
        ("blindspot", ["후측방충돌방지보조", "사각지대"]),
        ("cruise", ["스마트크루즈컨트롤", "크루즈컨트롤"]),
        ("lane", ["차선유지보조", "차선이탈", "lkas"]),
        ("sunroof", ["파노라마선루프", "선루프"]),
        ("vent", ["통풍시트"]),
        ("heatedseat", ["열선시트"]),
        ("heatedwheel", ["열선핸들"]),
        ("ledlamp", ["led헤드램프", "led램프"]),
        ("svm", ["서라운드뷰모니터"]),
        ("hud", ["헤드업디스플레이"]),
        ("epb", ["전자식파킹브레이크"]),
        ("powergate", ["파워테일게이트", "전동식트렁크"]),
        ("camera", ["후방카메라", "어라운드뷰"]),
        ("aircon", ["전자동에어컨", "풀오토에어컨"]),
    ]
    for g, toks in groups:
        if any(t in k for t in toks):
            return g
    # package-like labels
    if any(t in k for t in ["드라이브와이즈", "스마트센스"]):
        return "blindspot"
    if any(t in k for t in ["스타일", "컴포트", "컨비니언스"]):
        return "pkg_misc"
    return ""


def option_match_names(target_opts: list[str], item_opts: list[str]) -> tuple[list[str], list[str]]:
    item_keys = [normalize_option_key(x) for x in item_opts if normalize_option_key(x)]
    item_groups = [option_group_key(k) for k in item_keys]
    matched, missing = [], []
    for opt in target_opts:
        k = normalize_option_key(opt)
        if not k:
            continue
        g = option_group_key(k)
        ok = any((k in ik) or (ik in k) for ik in item_keys)
        if (not ok) and g:
            ok = g in item_groups
        if ok:
            matched.append(opt)
        else:
            missing.append(opt)
    return matched, missing


def parse_raw_vehicle_text(raw_text: str) -> dict[str, Any]:
    raw = str(raw_text or "")
    lines = [ln.strip() for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln]
    raw_lines = [ln.strip() for ln in raw.splitlines()]

    brand = ""
    bm = re.search(r"(현대|기아|제네시스|쉐보레(?:\(GM대우\))?|르노코리아(?:\(삼성\))?|KG모빌리티(?:\(쌍용\))?|쌍용)", raw)
    if bm:
        brand = bm.group(1)

    model = lines[0] if lines else ""
    # If first line is not car model, fallback to known model pattern lines.
    if model in {"평균 재고", "제원", "옵션"}:
        model = ""
    if not model:
        for ln in lines:
            if any(k in ln for k in ["아반떼", "레이", "티볼리", "K5", "스파크", "셀토스", "모닝", "쏘나타", "그랜저", "트레일 블레이저", "투싼", "스포티지"]):
                model = ln
                break
    trim = ""
    if len(lines) >= 2:
        l2 = lines[1]
        if l2 not in {"평균 재고", "제원", "옵션"}:
            trim = l2

    ym = re.search(r"(20\d{2})\s*년", raw)
    year = int(ym.group(1)) if ym else None

    km = None
    mm = re.search(r"([\d,]{1,9})\s*km", raw, flags=re.IGNORECASE)
    if mm:
        try:
            km = int(mm.group(1).replace(",", ""))
        except Exception:
            km = None

    parsed_opts: list[str] = []
    parsed_main_opts: list[str] = []
    if raw_lines:
        # Main installed option section (used as fallback for matching).
        try:
            i0 = next(i for i, ln in enumerate(raw_lines) if ln == "옵션")
            stop0 = ["출고 정보", "* 신차 추가옵션", "완전무사고", "평가사 진단결과", "차량 프레임"]
            for ln in raw_lines[i0 + 1 :]:
                ln = ln.strip()
                if not ln:
                    continue
                if any(k in ln for k in stop0):
                    break
                if ln:
                    parsed_main_opts.append(ln)
        except Exception:
            parsed_main_opts = []

        # Prefer only factory add-on section:
        # '... 출고 정보' -> numbered lines -> '* 신차 추가옵션'
        start_idx = None
        end_idx = None
        for i, ln in enumerate(lines):
            if ("출고 정보" in ln) and ("신차정가" not in ln):
                start_idx = i
                break
        if start_idx is not None:
            for j in range(start_idx + 1, len(raw_lines)):
                if "* 신차 추가옵션" in raw_lines[j]:
                    end_idx = j
                    break
            if end_idx is not None:
                for ln in raw_lines[start_idx + 1 : end_idx]:
                    ln = ln.strip()
                    if not ln:
                        continue
                    m = re.match(r"^\d+\)\s*(.+)$", ln)
                    if not m:
                        continue
                    opt = m.group(1).strip()
                    if opt:
                        parsed_opts.append(opt)

    return {
        "brand": brand,
        "model": model,
        "trim": trim,
        "year": year,
        "mileage_km": km,
        "options": list(dict.fromkeys(parsed_opts)),
        "main_options": list(dict.fromkeys(parsed_main_opts)),
    }


def load_candidates(brand: str, model: str) -> list[dict[str, Any]]:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    where = [
        "source_url LIKE 'heydealer://%'",
        "auction_end_date IS NOT NULL",
        "auction_top_bid IS NOT NULL",
        "auction_my_bid IS NOT NULL",
    ]
    params: list[Any] = []

    sql = f"""
      SELECT id, brand, model, trim, registration_year, mileage_km,
             auction_top_bid, auction_my_bid, auction_end_date
      FROM cars
      WHERE {' AND '.join(where)}
      ORDER BY id DESC
      LIMIT 5000
    """
    rows = [dict(r) for r in cur.execute(sql, params).fetchall()]
    if not rows:
        conn.close()
        return []

    ids = [r["id"] for r in rows]
    q = ",".join(["?"] * len(ids))
    opt_rows = cur.execute(f"SELECT car_id, option_name FROM car_options WHERE car_id IN ({q})", ids).fetchall()
    by_opts: dict[int, list[str]] = {}
    for car_id, name in opt_rows:
        by_opts.setdefault(int(car_id), []).append(str(name))
    for r in rows:
        r["options"] = by_opts.get(int(r["id"]), [])

    conn.close()
    return rows


def score_item(item: dict[str, Any], q: dict[str, Any]) -> dict[str, Any]:
    score = 0.0

    brand_q = normalize_text_key(q.get("brand"))
    model_q = normalize_text_key(q.get("model"))
    trim_q = normalize_text_key(q.get("trim"))

    brand_i = normalize_text_key(item.get("brand"))
    model_i = normalize_text_key(item.get("model"))
    trim_i = normalize_text_key(item.get("trim"))

    if brand_q and brand_q in brand_i:
        score += 1.5
    if model_q and model_q in model_i:
        score += 3.0
    if trim_q:
        if trim_q == trim_i:
            score += 5.0
        elif trim_q in trim_i or trim_i in trim_q:
            score += 2.5

    yq = q.get("year")
    yi = item.get("registration_year")
    if isinstance(yq, int) and isinstance(yi, int):
        score += max(0.0, 2.0 - abs(yq - yi) * 0.6)

    kmq = q.get("mileage_km")
    kmi = item.get("mileage_km")
    if isinstance(kmq, int) and isinstance(kmi, int):
        diff = abs(kmq - kmi)
        score += max(0.0, 2.0 - (diff / 10000.0) * 0.4)

    target_opts = q.get("options") or []
    fallback_opts = q.get("options_fallback") or []
    item_opts = item.get("options") or []
    matched, missing = option_match_names(target_opts, item_opts)

    # Fallback: package-option names may not match installed-option labels.
    # If primary is all-miss, retry with main installed-option section.
    option_basis = "add_on"
    if target_opts and (len(matched) == 0) and fallback_opts:
        fm, fmiss = option_match_names(fallback_opts, item_opts)
        if fm:
            matched, missing = fm, fmiss
            target_opts = fallback_opts
            option_basis = "main_fallback"

    if target_opts:
        ratio = len(matched) / max(1, len(target_opts))
        score += ratio * 6.0
        if len(matched) == 0:
            score -= 1.2

    out = dict(item)
    out["score"] = round(score, 3)
    out["score100"] = int(round(max(0.0, min(100.0, (score / 16.5) * 100.0))))
    out["my_minus_top"] = None
    try:
        out["my_minus_top"] = int(item.get("auction_my_bid") or 0) - int(item.get("auction_top_bid") or 0)
    except Exception:
        out["my_minus_top"] = None
    out["option_match_count"] = len(matched)
    out["option_target_count"] = len(target_opts)
    out["option_matched_names"] = matched
    out["option_missing_names"] = missing
    out["option_basis"] = option_basis
    return out


def same_model_guard(item: dict[str, Any], q: dict[str, Any]) -> bool:
    mq = normalize_text_key(q.get("model"))
    mi = normalize_text_key(item.get("model"))
    if not mq:
        return True
    if (mq in mi) or (mi in mq):
        return True
    q_tokens = [t for t in mq.split() if len(t) >= 2]
    i_tokens = [t for t in mi.split() if len(t) >= 2]
    if not q_tokens or not i_tokens:
        return False
    overlap = len(set(q_tokens) & set(i_tokens))
    # Require stronger overlap to block unrelated models.
    return overlap >= max(2, min(len(q_tokens), len(i_tokens)))


@app.get("/")
def home():
    return render_template_string(HTML)


@app.post("/api/similar")
def api_similar():
    data = request.get_json(force=True, silent=True) or {}
    parsed_raw = parse_raw_vehicle_text(str(data.get("raw_text") or ""))
    typed_opts = parse_options(str(data.get("options_text") or ""))
    parsed_addon_opts = parsed_raw.get("options") or []
    parsed_main_opts = parsed_raw.get("main_options") or []

    # Option source priority (strict selected-option mode):
    # 1) manual typed options
    # 2) parsed add-on options (출고 신차 추가옵션)
    # 3) parsed main installed options (옵션 섹션) only when selected options are unavailable
    primary_opts = typed_opts or parsed_addon_opts or parsed_main_opts
    # If selected options are available, do not fallback to main installed-option section.
    fallback_opts = []
    q = {
        "brand": str(data.get("brand") or "").strip() or (parsed_raw.get("brand") or ""),
        "model": str(data.get("model") or "").strip() or (parsed_raw.get("model") or ""),
        "trim": str(data.get("trim") or "").strip() or (parsed_raw.get("trim") or ""),
        "year": int(data.get("year")) if str(data.get("year") or "").strip().isdigit() else parsed_raw.get("year"),
        "mileage_km": int(data.get("mileage_km")) if str(data.get("mileage_km") or "").strip().isdigit() else parsed_raw.get("mileage_km"),
        "options": primary_opts,
        "options_fallback": fallback_opts,
    }
    if not q["model"]:
        return jsonify({"error": "모델은 필수입니다."}), 400

    rows = load_candidates(q["brand"], q["model"])
    same_model_rows = [r for r in rows if same_model_guard(r, q)]
    scored = [score_item(r, q) for r in same_model_rows]
    scored.sort(
        key=lambda x: (
            x.get("score100") or 0,
            x.get("option_match_count") or 0,
            x.get("score") or 0,
        ),
        reverse=True,
    )
    top = scored[:20]

    def avg(vals: list[int | float | None]) -> int | None:
        arr = [float(v) for v in vals if v is not None]
        return int(round(sum(arr) / len(arr))) if arr else None

    return jsonify(
        {
            "count": len(top),
            "avg_top_bid": avg([x.get("auction_top_bid") for x in top]),
            "avg_my_bid": avg([x.get("auction_my_bid") for x in top]),
            "avg_gap": avg([x.get("my_minus_top") for x in top]),
            "parsed": q,
            "items": top,
        }
    )


if __name__ == "__main__":
    url = "http://127.0.0.1:8092"
    print(url)
    threading.Timer(0.7, lambda: webbrowser.open(url)).start()
    app.run(host="127.0.0.1", port=8092, debug=False)
