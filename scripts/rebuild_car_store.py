import json
import re
import sqlite3
from pathlib import Path


DB_PATH = "data.db"
DATA_ROOT = Path("data") / "by_model"


def slugify_name(text):
    s = str(text or "").strip()
    s = re.sub(r"[^\w가-힣]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "unknown"


def export_all():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = cur.execute("SELECT id, model, trim FROM cars ORDER BY id").fetchall()
    count = 0
    for r in rows:
        car_id = int(r["id"])
        car = dict(cur.execute("SELECT * FROM cars WHERE id = ?", (car_id,)).fetchone())
        options = [
            x["option_name"]
            for x in cur.execute(
                "SELECT option_name FROM car_options WHERE car_id = ? ORDER BY id",
                (car_id,),
            ).fetchall()
        ]
        auctions = [
            dict(x)
            for x in cur.execute(
                "SELECT * FROM auction_results WHERE car_id = ? ORDER BY id DESC",
                (car_id,),
            ).fetchall()
        ]

        model_folder = slugify_name(r["model"])
        trim_folder = slugify_name(r["trim"])
        out_dir = DATA_ROOT / model_folder / trim_folder
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"car_{car_id:06d}.json"
        out_file.write_text(
            json.dumps(
                {"car": car, "options": options, "auction_results": auctions},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        count += 1

    conn.close()
    print("exported:", count)
    print("root:", DATA_ROOT.resolve())


if __name__ == "__main__":
    export_all()
