import json
import os
from pathlib import Path

from playwright.sync_api import sync_playwright

from scripts.import_heydealer_by_url import (
    API_BASE,
    LOGIN_URL,
    dedup_rows,
    fetch_grade_rows_adaptive,
    is_domestic_brand,
    save_to_db,
)


PROGRESS_PATH = Path("data") / "sync" / "all_zero_progress.json"


def load_progress():
    if PROGRESS_PATH.exists():
        try:
            return json.loads(PROGRESS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"done_models": [], "stats": {}}


def save_progress(progress):
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_PATH.write_text(
        json.dumps(progress, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def fetch_json(page, url):
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
        return None
    return result.get("data")


def main():
    username = os.getenv("HEYDEALER_USER")
    password = os.getenv("HEYDEALER_PASS")
    if not username or not password:
        raise RuntimeError("Set HEYDEALER_USER and HEYDEALER_PASS environment variables.")

    progress = load_progress()
    done_models = set(progress.get("done_models") or [])

    total_inserted = 0
    total_skipped = 0
    total_fetched = 0
    model_count = 0

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

        brands = fetch_json(page, f"{API_BASE}/price/car_meta/brands/?auction_type=customer_zero") or []

        for b in brands:
            brand_hash = b.get("hash_id")
            if not brand_hash:
                continue
            brand_meta = fetch_json(
                page,
                f"{API_BASE}/price/car_meta/brands/{brand_hash}/?auction_type=customer_zero",
            ) or {}
            brand_name = brand_meta.get("name") or b.get("name") or "미확인"
            if not is_domestic_brand(brand_name):
                continue
            model_groups = brand_meta.get("model_groups") or []

            for mg in model_groups:
                mg_hash = mg.get("hash_id")
                if not mg_hash:
                    continue
                mg_meta = fetch_json(
                    page,
                    f"{API_BASE}/price/car_meta/model_groups/{mg_hash}/?auction_type=customer_zero",
                ) or {}
                models = mg_meta.get("models") or []

                for m in models:
                    model_hash = m.get("hash_id")
                    if not model_hash or model_hash in done_models:
                        continue

                    model_meta = fetch_json(
                        page,
                        f"{API_BASE}/price/car_meta/models/{model_hash}/?auction_type=customer_zero",
                    ) or {}
                    model_name = model_meta.get("name") or m.get("name") or "미확인"
                    grades = model_meta.get("grades") or []

                    rows_all = []
                    for g in grades:
                        gp = {
                            "brand": brand_hash,
                            "model_group": mg_hash,
                            "model": model_hash,
                            "grade": g.get("hash_id"),
                            "auction_type": "customer_zero",
                        }
                        rows_part = fetch_grade_rows_adaptive(
                            page,
                            gp,
                            grade_count_hint=g.get("count"),
                            start_year=model_meta.get("start_year"),
                            end_year=model_meta.get("end_year"),
                        )
                        rows_all.extend(rows_part)

                    rows = dedup_rows(rows_all, model_hash)
                    total_fetched += len(rows)

                    result = save_to_db(
                        rows,
                        {
                            "brand": brand_hash,
                            "model_group": mg_hash,
                            "model": model_hash,
                            "auction_type": "customer_zero",
                        },
                        brand_name,
                        model_name,
                    )
                    total_inserted += result["inserted"]
                    total_skipped += result["skipped"]
                    model_count += 1

                    done_models.add(model_hash)
                    progress["done_models"] = sorted(done_models)
                    progress["stats"] = {
                        "processed_models": model_count,
                        "total_fetched": total_fetched,
                        "total_inserted": total_inserted,
                        "total_skipped": total_skipped,
                    }
                    save_progress(progress)
                    print(
                        f"[{model_count}] {brand_name} / {model_name} "
                        f"fetched={len(rows)} inserted={result['inserted']} skipped={result['skipped']}"
                    )

        browser.close()

    print("done_models:", model_count)
    print("total_fetched:", total_fetched)
    print("total_inserted:", total_inserted)
    print("total_skipped:", total_skipped)
    print("progress_file:", PROGRESS_PATH.resolve())


if __name__ == "__main__":
    main()
