from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request, render_template_string
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_WEB = "https://dealer.heydealer.com"
API_BASE = "https://api.heydealer.com/v2/dealers/web"
DB_PATH = Path(__file__).resolve().parents[2] / "data.db"

app = Flask(__name__)

STATE: dict[str, Any] = {
    "running": False,
    "last": None,
    "log": [],
    "progress": {
        "phase": "idle",
        "page": 0,
        "meta_total": None,
        "fetched_rows": 0,
        "dedup_rows": 0,
        "inserted": 0,
        "skipped": 0,
    },
}


HTML = """
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>헤이딜러 내입찰 종료 수집기</title>
  <style>
    body { font-family: "Segoe UI", "Malgun Gothic", sans-serif; margin: 18px; background: #f5f7fa; color: #132434; }
    .card { background: #fff; border: 1px solid #d9e2eb; border-radius: 12px; padding: 14px; margin-bottom: 12px; }
    .row { display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 8px; }
    .field { display: flex; flex-direction: column; gap: 4px; }
    label { font-size: 12px; color: #556b81; }
    input, button, select { padding: 8px; border: 1px solid #d9e2eb; border-radius: 8px; }
    button { cursor: pointer; font-weight: 700; }
    .primary { background: #0f766e; color: #fff; border: 0; }
    pre { background: #0b1d2d; color: #d6e4f0; padding: 10px; border-radius: 8px; white-space: pre-wrap; }
  </style>
</head>
<body>
  <div class="card">
    <h2 style="margin-top:0">헤이딜러 내입찰 종료(expired) 수집기</h2>
    <div class="row">
      <div class="field"><label>아이디</label><input id="u" /></div>
      <div class="field"><label>비밀번호</label><input id="p" type="password" /></div>
      <div class="field"><label>최대 페이지(0=전체)</label><input id="pages" type="number" value="0" /></div>
      <div class="field"><label>페이지당</label><input id="size" type="number" value="20" /></div>
    </div>
    <div style="margin-top:10px; display:flex; gap:8px;">
      <button class="primary" id="run">수집 시작</button>
      <button id="refresh">상태 새로고침</button>
    </div>
  </div>

  <div class="card">
    <div id="status">-</div>
    <pre id="log">-</pre>
  </div>

<script>
async function refresh() {
  const r = await fetch('/status');
  const d = await r.json();
  const p = d.progress || {};
  const lines = [
    `running: ${d.running ? 'Y' : 'N'}`,
    `phase: ${p.phase || '-'}`,
    `page: ${p.page ?? 0}`,
    `meta_total: ${p.meta_total ?? '-'}`,
    `fetched_rows: ${p.fetched_rows ?? 0}`,
    `dedup_rows: ${p.dedup_rows ?? 0}`,
    `inserted: ${p.inserted ?? 0}`,
    `skipped: ${p.skipped ?? 0}`,
    `last: ${JSON.stringify(d.last || {}, null, 2)}`,
  ];
  document.getElementById('status').textContent = lines.join('\\n');
  document.getElementById('log').textContent = (d.log || []).slice(-30).join('\\n') || '-';
}

document.getElementById('run').onclick = async () => {
  const payload = {
    username: document.getElementById('u').value.trim(),
    password: document.getElementById('p').value,
    max_pages: Number(document.getElementById('pages').value || 0),
    page_size: Number(document.getElementById('size').value || 20),
  };
  const r = await fetch('/collect', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload) });
  const d = await r.json();
  alert(d.message || d.error || '요청 완료');
  refresh();
};
document.getElementById('refresh').onclick = refresh;
refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""


def log(msg: str) -> None:
    t = datetime.now().strftime("%H:%M:%S")
    STATE["log"].append(f"[{t}] {msg}")
    if len(STATE["log"]) > 400:
        STATE["log"] = STATE["log"][-400:]


def set_progress(**kwargs: Any) -> None:
    p = STATE.get("progress") or {}
    p.update(kwargs)
    STATE["progress"] = p


def slugify_name(text: str) -> str:
    import re

    s = str(text or "").strip()
    s = re.sub(r"[^\w가-힣]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def stable_source_url(model_hash: str, detail: dict[str, Any], auction: dict[str, Any]) -> str:
    key = {
        "model_hash": model_hash,
        "grade_part_name": detail.get("grade_part_name"),
        "year": detail.get("year"),
        "mileage": detail.get("mileage"),
        "ended_at": auction.get("ended_at") or auction.get("expired_at"),
        "top_bid": (auction.get("highest_bid") or {}).get("price"),
        "my_bid": auction.get("my_bid_price"),
        "bids_count": auction.get("bids_count"),
        "car_number": detail.get("car_number"),
    }
    digest = hashlib.sha1(json.dumps(key, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:18]
    return f"heydealer://{model_hash}/{digest}"


def collect_loaded_options(detail: dict[str, Any]) -> list[str]:
    out: list[str] = []
    adv = detail.get("advanced_options")
    if isinstance(adv, list):
        for it in adv:
            if not isinstance(it, dict):
                continue
            # Keep selected add-ons only; drop trim-default options.
            choice = str(it.get("choice") or "").strip().lower()
            availability = str(it.get("availability") or "").strip().lower()
            if choice != "loaded":
                continue
            if availability == "default":
                continue
            nm = str(it.get("name") or it.get("label") or "").strip()
            if nm:
                out.append(nm)
    return list(dict.fromkeys(out))


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def export_car_record(conn: sqlite3.Connection, car_id: int) -> None:
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    car = cur.execute("SELECT * FROM cars WHERE id = ?", (car_id,)).fetchone()
    if not car:
        return
    car = dict(car)
    options = [r[0] for r in cur.execute("SELECT option_name FROM car_options WHERE car_id = ? ORDER BY id", (car_id,)).fetchall()]
    auctions = [dict(r) for r in cur.execute("SELECT * FROM auction_results WHERE car_id = ? ORDER BY id DESC", (car_id,)).fetchall()]

    base = Path(__file__).resolve().parents[2] / "data" / "by_model"
    out_dir = base / slugify_name(car.get("model")) / slugify_name(car.get("trim"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"car_{car_id:06d}.json"
    out_path.write_text(json.dumps({"car": car, "options": options, "auction_results": auctions}, ensure_ascii=False, indent=2), encoding="utf-8")


def save_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    conn = get_conn()
    cur = conn.cursor()
    inserted = 0
    skipped = 0

    for row in rows:
        detail = row.get("detail") or {}
        auction = row.get("auction") or {}
        model_hash = str(detail.get("model_hash_id") or "unknown")
        source_url = stable_source_url(model_hash, detail, auction)

        exists = cur.execute("SELECT id FROM cars WHERE source_url = ? LIMIT 1", (source_url,)).fetchone()
        if exists:
            skipped += 1
            car_id = int(exists[0])
            # Refresh option rows for existing cars to keep selection-option quality.
            cur.execute("DELETE FROM car_options WHERE car_id = ?", (car_id,))
            for opt in collect_loaded_options(detail):
                cur.execute("INSERT INTO car_options(car_id, option_name) VALUES (?, ?)", (car_id, opt))
            export_car_record(conn, car_id)
            continue

        trim = str(detail.get("grade_part_name") or "미확인")
        brand = str(detail.get("brand_name") or "미확인")
        model = str(detail.get("model_part_name") or "미확인")
        year = detail.get("year")
        mileage = detail.get("mileage")
        fuel = detail.get("fuel_display")
        trans = detail.get("transmission_display")

        carhistory = detail.get("carhistory") or {}
        owner_changes = carhistory.get("owner_changed_count")

        highest = (auction.get("highest_bid") or {}).get("price")
        my_bid = auction.get("my_bid_price")
        bids_count = auction.get("bids_count")
        ended_at = auction.get("ended_at") or auction.get("expired_at")
        ended_date = str(ended_at)[:10] if ended_at else None

        tags = [t.get("short_text") for t in (auction.get("tags") or []) if isinstance(t, dict) and t.get("short_text")]
        evaluator_memo = "heydealer_bid_import"
        now = datetime.now().isoformat(timespec="seconds")

        cur.execute(
            """
            INSERT INTO cars(
              source_url, brand, model, trim, year_model, registration_year, fuel, transmission,
              mileage_km, owner_changes, evaluator_memo, customer_points, auction_end_date,
              auction_participants, auction_top_bid, auction_my_bid, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_url, brand, model, trim,
                year, year, fuel, trans,
                mileage, owner_changes, evaluator_memo, "|".join(tags), ended_date,
                bids_count, highest, my_bid, now, now,
            ),
        )
        car_id = int(cur.lastrowid)

        for opt in collect_loaded_options(detail):
            cur.execute("INSERT INTO car_options(car_id, option_name) VALUES (?, ?)", (car_id, opt))

        cur.execute(
            """
            INSERT INTO auction_results(
              car_id, ended_at, participants, top_bid, selected_price, my_bid, note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                car_id, ended_date, bids_count, highest, None, my_bid,
                "source=heydealer_bid; auction_status=expired", now,
            ),
        )

        export_car_record(conn, car_id)
        inserted += 1

    conn.commit()
    conn.close()
    return {"inserted": inserted, "skipped": skipped, "total": len(rows)}


def fetch_xhr_json(driver: webdriver.Chrome, path_qs: str) -> Any:
    txt = driver.execute_script(
        """
        var xhr = new XMLHttpRequest();
        xhr.open('GET', 'https://api.heydealer.com/v2/dealers/web/' + arguments[0], false);
        xhr.withCredentials = true;
        xhr.send(null);
        return xhr.responseText || 'null';
        """,
        path_qs,
    )
    return json.loads(txt)


def run_collect(username: str, password: str, max_pages: int, page_size: int) -> dict[str, Any]:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1600,2200")

    d = webdriver.Chrome(options=opts)
    try:
        d.set_page_load_timeout(45)
        d.set_script_timeout(45)
        set_progress(phase="login", page=0, fetched_rows=0, dedup_rows=0, inserted=0, skipped=0)
        log("로그인 시도")

        login_ok = False
        login_err = None
        for attempt in range(1, 4):
            try:
                log(f"로그인 페이지 이동 (시도 {attempt}/3)")
                d.get(f"{BASE_WEB}/login")
                WebDriverWait(d, 20).until(EC.presence_of_element_located((By.NAME, "username")))
                log("아이디/비밀번호 입력")
                u = d.find_element(By.NAME, "username")
                p = d.find_element(By.CSS_SELECTOR, "input[type='password']")
                u.clear()
                p.clear()
                u.send_keys(username)
                p.send_keys(password)

                log("로그인 버튼 클릭")
                clicked = False
                for b in d.find_elements(By.CSS_SELECTOR, "button[type='submit'], button"):
                    txt = (b.text or "").strip()
                    if ("로그인" in txt) and ("가입" not in txt):
                        b.click()
                        clicked = True
                        break
                if not clicked:
                    d.execute_script(
                        "const f=document.querySelector('form'); f && (f.requestSubmit ? f.requestSubmit() : f.submit());"
                    )

                time.sleep(2.0)
                log(f"로그인 직후 URL: {d.current_url}")

                # 보호 페이지 접근 가능 여부로 로그인 성공 검증
                d.get(f"{BASE_WEB}/dashboard")
                time.sleep(2.0)
                log(f"대시보드 이동 URL: {d.current_url}")
                if "redirect_url" in (d.current_url or ""):
                    raise RuntimeError("세션 미인증(redirect_url)")

                # API 접근 확인(로그인 후 사용해주세요 응답 방지)
                chk = fetch_xhr_json(
                    d,
                    "cars/?type=bid&auction_status=expired&is_subscribed=false&is_retried=false&is_previously_bid=false&envelope=true&meta_only=true",
                )
                if isinstance(chk, dict) and (chk.get("toast") or {}).get("message") == "로그인 후 사용해주세요.":
                    raise RuntimeError("API 인증 쿠키 미적용")

                login_ok = True
                break
            except Exception as e:
                login_err = e
                log(f"로그인 재시도 사유: {e}")
                time.sleep(1.5)

        if not login_ok:
            raise RuntimeError(
                f"로그인 실패: {login_err or '인증 실패'} (계정 상태/추가 인증/동시 로그인 여부 확인 필요)"
            )

        d.get(f"{BASE_WEB}/bid?type=bid")
        time.sleep(3)
        log(f"내입찰 페이지 URL: {d.current_url}")

        meta_total = None
        try:
            meta_obj = fetch_xhr_json(
                d,
                "cars/?type=bid&auction_status=expired&is_subscribed=false&is_retried=false&is_previously_bid=false&envelope=true&meta_only=true",
            )
            if isinstance(meta_obj, dict):
                meta_total = (meta_obj.get("meta") or {}).get("count")
        except Exception:
            meta_total = None
        set_progress(phase="fetch_list", meta_total=meta_total)

        all_rows: list[dict[str, Any]] = []
        page = 1
        while True:
            set_progress(page=page)
            qs = f"cars/?page={page}&type=bid&auction_status=expired&is_subscribed=false&is_retried=false&is_previously_bid=false&page_size={page_size}"
            rows = fetch_xhr_json(d, qs)
            if not isinstance(rows, list) or not rows:
                break

            for it in rows:
                h = str(it.get("hash_id") or "").strip()
                if not h:
                    continue
                det = fetch_xhr_json(d, f"cars/{h}/?referrer=bid")
                if not isinstance(det, dict):
                    continue
                all_rows.append({"detail": det.get("detail") or {}, "auction": det.get("auction") or {}})
                if len(all_rows) % 10 == 0:
                    set_progress(fetched_rows=len(all_rows))

            log(f"페이지 {page}: {len(rows)}건 수집")
            page += 1
            if max_pages > 0 and page > max_pages:
                break

        # dedup by source key
        uniq: dict[str, dict[str, Any]] = {}
        for r in all_rows:
            detail = r.get("detail") or {}
            auction = r.get("auction") or {}
            model_hash = str(detail.get("model_hash_id") or "unknown")
            key = stable_source_url(model_hash, detail, auction)
            uniq[key] = r

        rows = list(uniq.values())
        set_progress(phase="dedup", dedup_rows=len(rows), fetched_rows=len(all_rows))
        log(f"중복 제거 후 {len(rows)}건")
        set_progress(phase="save")
        saved = save_rows(rows)
        set_progress(phase="done", inserted=saved["inserted"], skipped=saved["skipped"])
        log(f"DB 저장 완료 inserted={saved['inserted']} skipped={saved['skipped']}")
        return {"fetched": len(all_rows), "dedup": len(rows), **saved}
    finally:
        d.quit()


@app.get("/")
def home():
    return render_template_string(HTML)


@app.get("/status")
def status():
    return jsonify(
        {
            "running": STATE["running"],
            "last": STATE["last"],
            "progress": STATE.get("progress") or {},
            "log": STATE["log"][-80:],
        }
    )


@app.post("/collect")
def collect():
    if STATE["running"]:
        return jsonify({"error": "이미 실행 중입니다."}), 409

    data = request.get_json(force=True, silent=True) or {}
    username = str(data.get("username") or "").strip()
    password = str(data.get("password") or "")
    max_pages = int(data.get("max_pages") or 0)
    page_size = int(data.get("page_size") or 20)

    if not username or not password:
        return jsonify({"error": "아이디/비밀번호가 필요합니다."}), 400

    def worker() -> None:
        STATE["running"] = True
        try:
            res = run_collect(username, password, max_pages=max_pages, page_size=page_size)
            STATE["last"] = {"ok": True, **res, "at": datetime.now().isoformat(timespec="seconds")}
        except Exception as e:
            STATE["last"] = {"ok": False, "error": str(e), "at": datetime.now().isoformat(timespec="seconds")}
            set_progress(phase="error")
            log(f"오류: {e}")
        finally:
            STATE["running"] = False

    threading.Thread(target=worker, daemon=True).start()
    return jsonify({"message": "수집 시작됨"})


if __name__ == "__main__":
    url = "http://127.0.0.1:8091"
    print(url)
    threading.Timer(0.7, lambda: webbrowser.open(url)).start()
    app.run(host="127.0.0.1", port=8091, debug=False)
