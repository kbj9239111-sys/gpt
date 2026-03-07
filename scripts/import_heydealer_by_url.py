import argparse
import hashlib
import json
import os
import re
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import sync_playwright


DB_PATH = "data.db"
DATA_ROOT = Path("data") / "by_model"
LOGIN_URL = "https://dealer.heydealer.com/login"
API_BASE = "https://api.heydealer.com/v2/dealers/web"

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

DOMESTIC_BRAND_KEYWORDS = [
    "현대",
    "기아",
    "제네시스",
    "쉐보레",
    "르노",
    "쌍용",
    "kgm",
    "kg 모빌리티",
    "한국gm",
    "gm대우",
    "대우",
    "hyundai",
    "kia",
    "genesis",
    "chevrolet",
    "renault",
    "ssangyong",
]


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def slugify_name(text):
    s = str(text or "").strip()
    s = re.sub(r"[^\w가-힣]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


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


def is_domestic_brand(brand_name):
    text = str(brand_name or "").strip().lower()
    if not text:
        return False
    return any(keyword in text for keyword in DOMESTIC_BRAND_KEYWORDS)


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


def stable_source_url(model_hash, detail, auction):
    end_date = parse_end_date(auction.get("ended_at_display"))
    images = detail.get("images") or []
    first_image = images[0] if images else None
    key = {
        "model_hash": model_hash,
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
    ).hexdigest()[:18]
    return f"heydealer://{model_hash}/{digest}"


def parse_price_url(price_url):
    q = parse_qs(urlparse(price_url).query)
    brand = (q.get("brand") or [None])[0]
    model_group = (q.get("model_group") or [None])[0]
    model = (q.get("model") or [None])[0]
    grade = (q.get("grade") or [None])[0]
    auction_type = (q.get("auction_type") or ["customer_zero"])[0]
    if not brand or not model_group or not model:
        raise ValueError("URL must include brand, model_group, model query params.")
    return {
        "brand": brand,
        "model_group": model_group,
        "model": model,
        "grade": grade,
        "auction_type": auction_type,
    }


def meta_brand(page, brand_hash, auction_type):
    url = f"{API_BASE}/price/car_meta/brands/{brand_hash}/?auction_type={auction_type}"
    result = page.evaluate(
        """
        async (url) => {
          const r = await fetch(url, { credentials: "include" });
          let data = null;
          try { data = await r.json(); } catch (e) {}
          return { status: r.status, data };
        }
        """,
        url,
    )
    if result.get("status") != 200 or not isinstance(result.get("data"), dict):
        return {}
    return result["data"]


def meta_model(page, model_hash, auction_type):
    url = f"{API_BASE}/price/car_meta/models/{model_hash}/?auction_type={auction_type}"
    result = page.evaluate(
        """
        async (url) => {
          const r = await fetch(url, { credentials: "include" });
          let data = null;
          try { data = await r.json(); } catch (e) {}
          return { status: r.status, data };
        }
        """,
        url,
    )
    if result.get("status") != 200 or not isinstance(result.get("data"), dict):
        return {}
    return result["data"]


def fetch_rows(page, params, limit=None):
    rows = []
    page_no = 1
    while True:
        query = {
            "page": page_no,
            "model": params["model"],
            "auction_type": params["auction_type"],
            "period": "c",
            "order": "recent",
        }
        if params.get("grade"):
            query["grade"] = params["grade"]
        qs = "&".join([f"{k}={v}" for k, v in query.items()])
        url = f"{API_BASE}/price/cars/?{qs}"

        result = page.evaluate(
            """
            async (url) => {
              const r = await fetch(url, { credentials: "include" });
              let data = null;
              try { data = await r.json(); } catch (e) {}
              return { status: r.status, data };
            }
            """,
            url,
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
            if limit and len(rows) >= limit:
                return rows
        page_no += 1
    return rows


def fetch_rows_with_extra(page, params, extra_qs, limit=None):
    rows = []
    page_no = 1
    while True:
        query = {
            "page": page_no,
            "model": params["model"],
            "auction_type": params["auction_type"],
            "period": "c",
            "order": "recent",
        }
        if params.get("grade"):
            query["grade"] = params["grade"]
        query.update(extra_qs or {})
        qs = "&".join([f"{k}={v}" for k, v in query.items()])
        url = f"{API_BASE}/price/cars/?{qs}"

        result = page.evaluate(
            """
            async (url) => {
              const r = await fetch(url, { credentials: "include" });
              let data = null;
              try { data = await r.json(); } catch (e) {}
              return { status: r.status, data };
            }
            """,
            url,
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
            if limit and len(rows) >= limit:
                return rows
        page_no += 1
    return rows


def dedup_rows(rows, model_hash):
    out = []
    seen = set()
    for row in rows:
        detail = row.get("detail") or {}
        auction = row.get("auction") or {}
        key = stable_source_url(model_hash, detail, auction)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def fetch_partition_recursive(
    page,
    base_params,
    extra_qs,
    start_year,
    end_year,
    depth=0,
    max_depth=8,
):
    rows = fetch_rows_with_extra(page, base_params, extra_qs, limit=None)
    rows = dedup_rows(rows, base_params["model"])

    if len(rows) < 120:
        return rows
    if depth >= max_depth:
        return rows

    # Step 1: split by exact year if not already constrained.
    if "year" not in extra_qs:
        y0 = int(start_year) if start_year else date.today().year - 6
        y1 = int(end_year) if end_year else date.today().year
        if y1 < y0:
            y1 = y0
        union = {}
        for y in range(y0, y1 + 1):
            e = dict(extra_qs)
            e["year"] = y
            part = fetch_partition_recursive(
                page,
                base_params,
                e,
                start_year,
                end_year,
                depth=depth + 1,
                max_depth=max_depth,
            )
            for r in part:
                k = stable_source_url(base_params["model"], r["detail"], r["auction"])
                union[k] = r
        if union:
            return list(union.values())

    # Step 2: split mileage range (binary split).
    lo = int(extra_qs.get("min_mileage", 0))
    hi = int(extra_qs.get("max_mileage", 300000))
    if hi <= lo:
        return rows
    if (hi - lo) < 1200:
        return rows

    mid = (lo + hi) // 2
    left_qs = dict(extra_qs)
    left_qs["min_mileage"] = lo
    left_qs["max_mileage"] = mid
    right_qs = dict(extra_qs)
    right_qs["min_mileage"] = mid + 1
    right_qs["max_mileage"] = hi

    union = {}
    for part in [
        fetch_partition_recursive(
            page,
            base_params,
            left_qs,
            start_year,
            end_year,
            depth=depth + 1,
            max_depth=max_depth,
        ),
        fetch_partition_recursive(
            page,
            base_params,
            right_qs,
            start_year,
            end_year,
            depth=depth + 1,
            max_depth=max_depth,
        ),
    ]:
        for r in part:
            k = stable_source_url(base_params["model"], r["detail"], r["auction"])
            union[k] = r
    return list(union.values())


def fetch_grade_rows_adaptive(page, base_params, grade_count_hint=None, start_year=None, end_year=None):
    rows = fetch_partition_recursive(
        page,
        base_params,
        {},
        start_year=start_year,
        end_year=end_year,
        depth=0,
        max_depth=9,
    )
    rows = dedup_rows(rows, base_params["model"])
    if grade_count_hint and len(rows) > grade_count_hint:
        rows = rows[:grade_count_hint]
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


def save_to_db(rows, params, brand_name, model_name):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    inserted = 0
    skipped = 0
    by_trim = {}
    exported_files = []

    for row in rows:
        detail = row["detail"]
        auction = row["auction"]
        source_url = stable_source_url(params["model"], detail, auction)
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

        trim = (detail.get("grade_part_name") or "").strip() or "미확인"
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
                brand_name or "미확인",
                model_name or "미확인",
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
                f"source=heydealer; ended_display={auction.get('ended_at_display')}",
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
    parser = argparse.ArgumentParser(description="Import heydealer price cars by URL.")
    parser.add_argument("--url", required=True, help="Heydealer dealer price URL")
    parser.add_argument("--limit", type=int, default=0, help="0 means all pages")
    args = parser.parse_args()

    username = os.getenv("HEYDEALER_USER")
    password = os.getenv("HEYDEALER_PASS")
    if not username or not password:
        raise RuntimeError("Set HEYDEALER_USER and HEYDEALER_PASS environment variables.")

    params = parse_price_url(args.url)

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
        page.goto(args.url, wait_until="domcontentloaded", timeout=120000)
        page.wait_for_timeout(2000)

        brand_meta = meta_brand(page, params["brand"], params["auction_type"])
        model_meta = meta_model(page, params["model"], params["auction_type"])
        brand_name = brand_meta.get("name")
        model_name = model_meta.get("name")
        if not is_domestic_brand(brand_name):
            print("skipped_non_domestic_brand:", brand_name or params["brand"])
            browser.close()
            return

        expected_count = model_meta.get("count")
        if params.get("grade"):
            grade_count = None
            for g in model_meta.get("grades") or []:
                if g.get("hash_id") == params.get("grade"):
                    grade_count = g.get("count")
                    break
            expected_count = grade_count if grade_count is not None else expected_count
            rows = fetch_grade_rows_adaptive(
                page,
                params,
                grade_count_hint=grade_count,
                start_year=model_meta.get("start_year"),
                end_year=model_meta.get("end_year"),
            )
            if args.limit > 0:
                rows = rows[: args.limit]
        else:
            rows_all = []
            grades = model_meta.get("grades") or []
            for g in grades:
                gp = dict(params)
                gp["grade"] = g.get("hash_id")
                rows_part = fetch_grade_rows_adaptive(
                    page,
                    gp,
                    grade_count_hint=g.get("count"),
                    start_year=model_meta.get("start_year"),
                    end_year=model_meta.get("end_year"),
                )
                rows_all.extend(rows_part)
            rows = dedup_rows(rows_all, params["model"])
            if args.limit > 0:
                rows = rows[: args.limit]
        browser.close()

    result = save_to_db(rows, params, brand_name, model_name)

    print("brand:", brand_name or params["brand"])
    print("model:", model_name or params["model"])
    print("expected_count:", expected_count)
    print("fetched:", len(rows))
    print("inserted:", result["inserted"])
    print("skipped:", result["skipped"])
    print("by_trim:", json.dumps(result["by_trim"], ensure_ascii=False, sort_keys=True))
    print("exported_files:", result["exported_count"])


if __name__ == "__main__":
    main()
