import hashlib
import json
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.sync_api import sync_playwright


DB_PATH = "data.db"
DATA_ROOT = Path("data") / "by_model"
LOGIN_URL = "https://dealer.heydealer.com/login"
TARGET_URL = (
    "https://dealer.heydealer.com/price"
    "?brand=xoKegB&model_group=XyQ1gy&model=x4JNl4&grade=67Kdy6&auction_type=customer_zero"
)
PRICE_API = (
    "https://api.heydealer.com/v2/dealers/web/price/cars/"
    "?page={page}&model=x4JNl4&grade=67Kdy6&auction_type=customer_zero&period=c&order=recent"
)
LIMIT = 30

FRAME_PART_KEYS = {
    "side_member",
    "pillar",
    "wheel_house",
    "dash_panel",
    "cowl",
    "roof_panel",
    "panel_back",
    "floor",
    "frame",
    "cross_member",
}


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def slugify_name(text):
    s = str(text or "").strip()
    s = re.sub(r"[^\w가-힣]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def parse_trim(grade_part_name):
    text = (grade_part_name or "").strip()
    text = re.sub(r"^\s*1\.6\s*가솔린\s*", "", text)
    return text or (grade_part_name or "")


def parse_end_date(ended_at_display):
    text = str(ended_at_display or "").strip()
    today = date.today()
    if not text:
        return None
    if text == "오늘":
        return today.isoformat()
    if text == "어제":
        return (today - timedelta(days=1)).isoformat()
    m = re.search(r"(\d+)\s*일\s*전", text)
    if m:
        return (today - timedelta(days=int(m.group(1)))).isoformat()
    return None


def classify_accident(accident_repairs):
    repairs = accident_repairs or []
    exchange_count = 0
    painted_count = 0
    weld_count = 0
    frame_parts = []

    for rep in repairs:
        part = str(rep.get("part") or "")
        kind = str(rep.get("repair") or "")
        if kind == "exchange":
            exchange_count += 1
        elif kind == "painted":
            painted_count += 1
        elif kind == "weld":
            weld_count += 1

        if any(k in part for k in FRAME_PART_KEYS):
            frame_parts.append(part)

    if not repairs:
        status = "무사고"
    elif frame_parts:
        status = "유사고(프레임)"
    else:
        status = "유사고(비프레임)"

    return {
        "status": status,
        "exchange_count": exchange_count,
        "painted_count": painted_count,
        "weld_count": weld_count,
        "frame_parts": sorted(set(frame_parts)),
    }


def stable_source_url(detail, auction):
    end_date = parse_end_date(auction.get("ended_at_display"))
    images = detail.get("images") or []
    first_image = images[0] if images else None
    key = {
        "grade_part_name": detail.get("grade_part_name"),
        "year": detail.get("year"),
        "mileage": detail.get("mileage"),
        "ended_at": end_date,
        "top_bid": (auction.get("highest_bid") or {}).get("price"),
        "my_bid": auction.get("my_bid_price"),
        "bids_count": auction.get("bids_count"),
        "my_car_accident_summary": (detail.get("carhistory") or {}).get("my_car_accident_summary"),
        "first_image": first_image,
    }
    digest = hashlib.sha1(
        json.dumps(key, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return f"heydealer://cn7-1.6-gasoline/{digest}"


def fetch_rows(username, password, limit=30):
    rows = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        page = ctx.new_page()

        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=120000)
        page.locator(
            'input[name="username"], input[placeholder*="아이디"], input[type="text"]'
        ).first.fill(username)
        page.locator('input[type="password"]').first.fill(password)
        page.locator('button:has-text("로그인"), button[type="submit"]').first.click()
        page.wait_for_timeout(5000)
        page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(2000)

        page_no = 1
        while len(rows) < limit:
            result = page.evaluate(
                """
                async (url) => {
                  const r = await fetch(url, { credentials: "include" });
                  let data = null;
                  try { data = await r.json(); } catch (e) {}
                  return { status: r.status, data };
                }
                """,
                PRICE_API.format(page=page_no),
            )
            if result.get("status") != 200:
                break
            items = result.get("data")
            if not isinstance(items, list) or not items:
                break

            for item in items:
                detail = item.get("detail") or {}
                auction = item.get("auction") or {}
                top_bid = (auction.get("highest_bid") or {}).get("price")
                if top_bid is None:
                    continue
                rows.append({"detail": detail, "auction": auction})
                if len(rows) >= limit:
                    break
            page_no += 1

        browser.close()
    return rows


def export_car_record(conn, car_id):
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    car = cur.execute("SELECT * FROM cars WHERE id = ?", (car_id,)).fetchone()
    if not car:
        return None
    car = dict(car)
    options = [
        r["option_name"]
        for r in cur.execute(
            "SELECT option_name FROM car_options WHERE car_id = ? ORDER BY id",
            (car_id,),
        ).fetchall()
    ]
    auction_results = [
        dict(r)
        for r in cur.execute(
            "SELECT * FROM auction_results WHERE car_id = ? ORDER BY id DESC",
            (car_id,),
        ).fetchall()
    ]

    model_folder = slugify_name(car.get("model"))
    trim_folder = slugify_name(car.get("trim"))
    out_dir = DATA_ROOT / model_folder / trim_folder
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"car_{car_id:06d}.json"
    payload = {"car": car, "options": options, "auction_results": auction_results}
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(out_path.as_posix())


def save_to_db(rows):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    by_trim = {}
    exported_files = []

    for row in rows:
        detail = row["detail"]
        auction = row["auction"]
        source_url = stable_source_url(detail, auction)
        exists = cur.execute(
            "SELECT id FROM cars WHERE source_url = ? LIMIT 1",
            (source_url,),
        ).fetchone()
        if exists:
            skipped += 1
            exported = export_car_record(conn, int(exists[0]))
            if exported:
                exported_files.append(exported)
            continue

        grade_part_name = detail.get("grade_part_name") or ""
        trim = parse_trim(grade_part_name)
        end_date = parse_end_date(auction.get("ended_at_display"))
        accident = classify_accident(detail.get("accident_repairs") or [])
        carhistory = detail.get("carhistory") or {}
        loaded_options = [o.get("name") for o in (detail.get("loaded_options") or []) if o.get("name")]
        tags = [t.get("short_text") for t in (auction.get("tags") or []) if t.get("short_text")]

        my_bid = auction.get("my_bid_price")
        top_bid = (auction.get("highest_bid") or {}).get("price")
        participants = auction.get("bids_count")
        now = now_iso()

        evaluator_memo = (
            f"heydealer_import; accident={accident['status']}; "
            f"exchange={accident['exchange_count']}; painted={accident['painted_count']}; "
            f"weld={accident['weld_count']}; frame_parts={','.join(accident['frame_parts'])}"
        )

        cur.execute(
            """
            INSERT INTO cars(
                source_url, brand, model, trim, year_model, registration_year, fuel, transmission,
                mileage_km, owner_changes, evaluator_memo, customer_points, auction_end_date,
                auction_participants, auction_top_bid, auction_my_bid, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                source_url,
                "현대",
                "올 뉴 아반떼 (CN7)",
                trim,
                detail.get("year"),
                detail.get("year"),
                detail.get("fuel_display"),
                detail.get("transmission_display"),
                detail.get("mileage"),
                carhistory.get("owner_changed_count"),
                evaluator_memo,
                "|".join(tags),
                end_date,
                participants,
                top_bid,
                my_bid,
                now,
                now,
            ),
        )
        car_id = cur.lastrowid

        for opt_name in loaded_options:
            cur.execute(
                "INSERT INTO car_options(car_id, option_name) VALUES (?, ?)",
                (car_id, opt_name),
            )

        cur.execute(
            """
            INSERT INTO auction_results(
                car_id, ended_at, participants, top_bid, selected_price, my_bid, note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                car_id,
                end_date,
                participants,
                top_bid,
                None,
                my_bid,
                f"source=heydealer; trim_full={grade_part_name}; ended_display={auction.get('ended_at_display')}",
                now,
            ),
        )

        inserted += 1
        by_trim[trim] = by_trim.get(trim, 0) + 1
        exported = export_car_record(conn, car_id)
        if exported:
            exported_files.append(exported)

    conn.commit()
    conn.close()
    return {
        "inserted": inserted,
        "skipped": skipped,
        "by_trim": by_trim,
        "exported_count": len(set(exported_files)),
    }


def main():
    username = os.getenv("HEYDEALER_USER")
    password = os.getenv("HEYDEALER_PASS")
    if not username or not password:
        raise RuntimeError("Set HEYDEALER_USER and HEYDEALER_PASS environment variables.")

    rows = fetch_rows(username, password, limit=LIMIT)
    result = save_to_db(rows)

    print("fetched:", len(rows))
    print("inserted:", result["inserted"])
    print("skipped:", result["skipped"])
    print("by_trim:", json.dumps(result["by_trim"], ensure_ascii=False, sort_keys=True))
    print("exported_files:", result["exported_count"])


if __name__ == "__main__":
    main()
