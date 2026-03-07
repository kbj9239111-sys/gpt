import json
import os
import re
import sqlite3
import hashlib
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
from urllib.parse import urlparse
import urllib.parse
import urllib.error
import urllib.request
import webbrowser

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "data.db"
STATIC_DIR = ROOT / "static"
LOGIC_VERSION = "2026-03-07-encar-manual-v41"
PARSER_PROFILE_VERSION = "2026-03-06-source-profile-v1"
ENABLE_SOURCE_PROFILES = os.getenv("ENABLE_SOURCE_PROFILES", "1") != "0"
MAX_RECENT_ANALYSES = 10
MIN_MARKET_COMPS = 3
DETAILED_COMPARE_LIMIT = 20
DETAILED_FETCH_POOL_TARGET = 28
DETAILED_FETCH_MAX_ROWS = 60
COARSE_SCAN_TARGET_HITS = 1200
COARSE_SCAN_MIN_START = 6600
COARSE_SCAN_MAX_ROWS = 3000
COARSE_PREFILTER_MIN_ROWS = 120
DIRECT_ACTION_MAX_CANDIDATES = 60
DIRECT_ACTION_MAX_START = 4000
TOP_CANDIDATE_POOL = 120
OPTION_SCOPE_MIN_RATE = 0.15
OPTION_SCOPE_EXTRA_MIN_RATE = 0.35
REVIEW_MILEAGE_EXTRA_KM = 20000
OPTION_CATALOG_LEARN_SOURCE = str(os.getenv("OPTION_CATALOG_LEARN_SOURCE", "parsed")).strip().lower()
if OPTION_CATALOG_LEARN_SOURCE not in {"parsed", "encar", "both", "off"}:
    OPTION_CATALOG_LEARN_SOURCE = "parsed"
BUSINESS_DAY_CUTOFF_HOUR = 7
ENABLE_DAILY_MARKET_CACHE = os.getenv("ENABLE_DAILY_MARKET_CACHE", "0") != "0"
ENABLE_COARSE_PREFILTER = os.getenv("ENABLE_COARSE_PREFILTER", "0") == "1"
ENABLE_ENCAR_DETAILED_FETCH = os.getenv("ENABLE_ENCAR_DETAILED_FETCH", "0") == "1"
DAILY_MARKET_CACHE_DIR = ROOT / "data" / "sync" / "daily_cache"
MAX_DAILY_MARKET_CACHE_FILES = 5
OPTION_SYNONYMS_PATH = ROOT / "data" / "sync" / "option_synonyms.json"
_OPTION_SYNONYM_CACHE = None
_OPTION_SYNONYM_MTIME = None

TRIM_ANCHOR_KEYWORDS = [
    "모던", "프리미엄", "프레스티지", "노블레스", "시그니처", "인스퍼레이션",
    "스마트", "트렌디", "익스클루시브", "르블랑", "캘리그래피", "X-Line",
    "N라인", "N Line", "플래티넘", "럭셔리", "V1", "V3", "V5", "노블레스 스페셜", "그래비티",
]
SUPPORTED_SOURCE_SITES = {"generic", "heydealer", "autoinside", "carauction", "gulliver"}
MODEL_BRAND_NOISE_TOKENS = {
    "기아", "현대", "제네시스", "쉐보레", "르노", "르노코리아",
    "kg모빌리티", "kgm", "쌍용", "gm대우", "gm",
    "kia", "hyundai", "genesis", "chevrolet", "renault", "ssangyong",
}


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS analyses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_text TEXT NOT NULL,
            source_site TEXT,
            parser_profile_version TEXT,
            brand TEXT,
            model TEXT,
            trim TEXT,
            registration_year INTEGER,
            registration_month INTEGER,
            fuel TEXT,
            transmission TEXT,
            mileage_km INTEGER,
            new_price INTEGER,
            options_json TEXT,
            market_json TEXT,
            estimated_sale_price INTEGER,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS encar_option_catalog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            brand TEXT NOT NULL,
            model_key TEXT NOT NULL,
            trim_anchor_key TEXT NOT NULL DEFAULT '',
            power_key TEXT NOT NULL DEFAULT '',
            displacement_key TEXT NOT NULL DEFAULT '',
            drive_key TEXT NOT NULL DEFAULT '',
            transmission_key TEXT NOT NULL DEFAULT '',
            option_code TEXT NOT NULL,
            option_name TEXT NOT NULL,
            seen_count INTEGER NOT NULL DEFAULT 1,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_encar_option_catalog_scope_code
        ON encar_option_catalog(
            brand, model_key, trim_anchor_key, power_key, displacement_key, drive_key, transmission_key, option_code
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_encar_option_catalog_lookup
        ON encar_option_catalog(brand, model_key, power_key, displacement_key, trim_anchor_key, seen_count)
        """
    )
    normalize_option_catalog_model_keys(conn)
    ensure_runtime_indexes(conn)
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(analyses)").fetchall()}
    if "source_site" not in cols:
        conn.execute("ALTER TABLE analyses ADD COLUMN source_site TEXT")
    if "parser_profile_version" not in cols:
        conn.execute("ALTER TABLE analyses ADD COLUMN parser_profile_version TEXT")
    # Keep storage small and recent-list behavior deterministic.
    conn.execute(
        "DELETE FROM analyses WHERE id NOT IN (SELECT id FROM analyses ORDER BY id DESC LIMIT ?)",
        (MAX_RECENT_ANALYSES,),
    )
    conn.commit()
    conn.close()


def ensure_runtime_indexes(conn):
    try:
        table_rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    except Exception:
        return
    table_set = {str(r[0]) for r in table_rows}

    # Keep index creation idempotent and optional per existing schema.
    if "cars" in table_set:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_cars_market_lookup
            ON cars(source_url, brand, model, trim, registration_year, mileage_km, auction_end_date)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_cars_market_bids
            ON cars(auction_top_bid, auction_my_bid, auction_end_date)
            """
        )
    if "car_options" in table_set:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_car_options_car_id_name
            ON car_options(car_id, option_name)
            """
        )
    if "auction_results" in table_set:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_auction_results_car_ended
            ON auction_results(car_id, ended_at)
            """
        )


def normalize_option_catalog_model_keys(conn):
    try:
        rows = conn.execute(
            """
            SELECT brand, model_key, trim_anchor_key, power_key, displacement_key, drive_key, transmission_key,
                   option_code, option_name, seen_count, first_seen_at, last_seen_at
            FROM encar_option_catalog
            """
        ).fetchall()
    except Exception:
        return
    if not rows:
        return

    merged = {}
    for r in rows:
        mk = normalize_scope_model_key(r["model_key"])
        key = (
            str(r["brand"] or "").strip(),
            mk,
            str(r["trim_anchor_key"] or "").strip(),
            str(r["power_key"] or "").strip(),
            str(r["displacement_key"] or "").strip(),
            str(r["drive_key"] or "").strip(),
            str(r["transmission_key"] or "").strip(),
            str(r["option_code"] or "").strip(),
        )
        rec = merged.get(key)
        seen = to_int(r["seen_count"]) or 1
        first_seen = str(r["first_seen_at"] or "") or now_iso()
        last_seen = str(r["last_seen_at"] or "") or now_iso()
        name = str(r["option_name"] or "").strip()
        if rec is None:
            merged[key] = {
                "option_name": name,
                "seen_count": seen,
                "first_seen_at": first_seen,
                "last_seen_at": last_seen,
            }
        else:
            rec["seen_count"] += seen
            if first_seen and ((not rec["first_seen_at"]) or (first_seen < rec["first_seen_at"])):
                rec["first_seen_at"] = first_seen
            if last_seen and ((not rec["last_seen_at"]) or (last_seen > rec["last_seen_at"])):
                rec["last_seen_at"] = last_seen
                if name:
                    rec["option_name"] = name

    conn.execute("DELETE FROM encar_option_catalog")
    rows_out = []
    for key, rec in merged.items():
        rows_out.append(
            (
                key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                rec["option_name"], rec["seen_count"], rec["first_seen_at"], rec["last_seen_at"],
            )
        )
    conn.executemany(
        """
        INSERT INTO encar_option_catalog(
            brand, model_key, trim_anchor_key, power_key, displacement_key, drive_key, transmission_key,
            option_code, option_name, seen_count, first_seen_at, last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows_out,
    )


def to_int(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def is_repeated_digit_4_price(price):
    p = to_int(price)
    if p is None:
        return False
    s = str(abs(p))
    return len(s) == 4 and len(set(s)) == 1


def is_abnormal_market_price(price):
    p = to_int(price)
    if p is None or p <= 0:
        return True
    # Encar placeholders like 1111/2222/.../9999
    if is_repeated_digit_4_price(p):
        return True
    return False


def fetch_json_url(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as res:
        return json.loads(res.read().decode("utf-8"))


def fetch_json_url_retry(url, timeout=10, attempts=2):
    last_err = None
    for _ in range(max(1, attempts)):
        try:
            return fetch_json_url(url, timeout=timeout)
        except Exception as e:
            last_err = e
            continue
    if last_err is not None:
        raise last_err
    raise RuntimeError("fetch_json_url_retry failed")


def build_encar_list_url(action_query, sr, count="true", inav="|Metadata|Sort"):
    # Use %20 for spaces in q (not '+') to keep Encar action parser stable.
    return "https://api.encar.com/search/car/list/mobile?" + urllib.parse.urlencode(
        {"count": count, "q": action_query, "sr": sr, "inav": inav},
        quote_via=urllib.parse.quote,
    )


def build_encar_web_search_url(action_query, sort="Mileage", page=1, limit=20):
    state = {
        "action": action_query,
        "toggle": {},
        "layer": "",
        "sort": sort,
        "page": page,
        "limit": limit,
        "searchKey": "",
        "loginCheck": False,
    }
    return (
        "https://www.encar.com/dc/dc_carsearchlist.do?carType=kor&searchType=model&TG.R=A#!"
        + urllib.parse.quote(json.dumps(state, ensure_ascii=False), safe="")
    )


def business_date_str(now=None):
    dt = now or datetime.now()
    if dt.hour < BUSINESS_DAY_CUTOFF_HOUR:
        dt = dt - timedelta(days=1)
    return dt.strftime("%Y%m%d")


def market_cache_file_path(day_key):
    return DAILY_MARKET_CACHE_DIR / f"market_cache_{day_key}.json"


def market_cache_key(parsed):
    key_obj = {
        "v": LOGIC_VERSION,
        "brand": str(parsed.get("brand") or "").strip(),
        "model": str(parsed.get("model") or "").strip(),
        "trim": str(parsed.get("trim") or "").strip(),
        "year": to_int(parsed.get("registration_year")),
        "fuel": str(parsed.get("fuel") or "").strip(),
        "trans": str(parsed.get("transmission") or "").strip(),
        "km": to_int(parsed.get("mileage_km")),
        "opts": sorted([str(x).strip() for x in (parsed.get("options") or []) if str(x).strip()]),
    }
    return json.dumps(key_obj, ensure_ascii=False, sort_keys=True)


def prune_old_market_cache_files():
    if not DAILY_MARKET_CACHE_DIR.exists():
        return
    files = sorted(DAILY_MARKET_CACHE_DIR.glob("market_cache_*.json"), key=lambda p: p.name, reverse=True)
    for old in files[MAX_DAILY_MARKET_CACHE_FILES:]:
        try:
            old.unlink(missing_ok=True)
        except Exception:
            pass


def load_market_cache(parsed):
    if not ENABLE_DAILY_MARKET_CACHE:
        return None
    day_key = business_date_str()
    fpath = market_cache_file_path(day_key)
    if not fpath.exists():
        return None
    try:
        payload = json.loads(fpath.read_text(encoding="utf-8"))
        item = (payload.get("items") or {}).get(market_cache_key(parsed))
        if not isinstance(item, dict):
            return None
        market = item.get("market")
        if not isinstance(market, dict):
            return None
        # Cache-compatibility guard: ignore old cached results that include basic rows.
        comps = market.get("comps") or []
        if any(str((c or {}).get("analysis_level") or "").strip().lower() == "basic" for c in comps):
            return None
        market_out = json.loads(json.dumps(market, ensure_ascii=False))
        dbg = market_out.setdefault("debug", {})
        dbg["cache_hit"] = True
        dbg["cache_day"] = day_key
        return market_out
    except Exception:
        return None


def save_market_cache(parsed, market):
    if not ENABLE_DAILY_MARKET_CACHE:
        return
    if not isinstance(market, dict):
        return
    day_key = business_date_str()
    fpath = market_cache_file_path(day_key)
    DAILY_MARKET_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        payload = {}
        if fpath.exists():
            payload = json.loads(fpath.read_text(encoding="utf-8"))
        items = payload.get("items")
        if not isinstance(items, dict):
            items = {}
            payload["items"] = items
        market_to_save = json.loads(json.dumps(market, ensure_ascii=False))
        dbg = market_to_save.setdefault("debug", {})
        dbg["cache_hit"] = False
        dbg["cache_day"] = day_key
        items[market_cache_key(parsed)] = {
            "saved_at": now_iso(),
            "market": market_to_save,
        }
        fpath.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        prune_old_market_cache_files()
    except Exception:
        pass


def finalize_market_result(parsed, market):
    if isinstance(market, dict):
        save_market_cache(parsed, market)
    return market


def safe_median(vals):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    n = len(vals)
    if n % 2:
        return vals[n // 2]
    return int(round((vals[n // 2 - 1] + vals[n // 2]) / 2))


def safe_quantile(vals, q):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    if q <= 0:
        return vals[0]
    if q >= 1:
        return vals[-1]
    pos = (len(vals) - 1) * q
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    if lo == hi:
        return vals[lo]
    w = pos - lo
    return int(round(vals[lo] * (1 - w) + vals[hi] * w))


def fuel_key(v):
    s = str(v or "")
    if ("전기" in s) and (("가솔린" in s) or ("휘발유" in s) or ("디젤" in s) or ("경유" in s)):
        return "hybrid"
    if ("하이브리드" in s) or ("HEV" in s.upper()) or ("PHEV" in s.upper()):
        return "hybrid"
    if ("휘발유" in s) or ("가솔린" in s):
        return "gasoline"
    if ("디젤" in s) or ("경유" in s):
        return "diesel"
    if "LPG" in s.upper():
        return "lpg"
    if "전기" in s:
        return "ev"
    if "하이브리드" in s:
        return "hybrid"
    return s.strip().lower()


def extract_year(v):
    if v is None:
        return None
    s = re.sub(r"\D", "", str(v))
    if len(s) >= 6:
        y = to_int(s[:4])
    elif len(s) == 4:
        y = to_int(s)
    else:
        y = None
    if y is None:
        return None
    if 1980 <= y <= 2100:
        return y
    return None


def normalize_source_site(v):
    s = str(v or "").strip().lower()
    if s in SUPPORTED_SOURCE_SITES:
        return s
    return "generic"


def normalize_scope_model_key(text):
    s = canonicalize_model_text(text).lower().replace("\ufeff", "")
    s = re.sub(r"[^0-9a-z가-힣()]+", " ", s)
    s = re.sub(r"\s+", "", s).strip()
    return s


def relaxed_scope_model_key(scope_key):
    s = str(scope_key or "")
    if not s:
        return s
    s = s.replace("디올뉴", "")
    s = s.replace("올뉴", "")
    s = s.replace("더뉴", "")
    s = s.replace("페이스리프트", "")
    s = s.replace("신형", "")
    s = re.sub(r"\s+", "", s).strip()
    return s


def strip_scope_parenthesis(scope_key):
    s = str(scope_key or "")
    if not s:
        return s
    s = re.sub(r"\([^)]*\)", "", s)
    s = re.sub(r"\s+", "", s).strip()
    return s


def model_scope_match(target_scope, row_scope):
    t = str(target_scope or "").strip()
    r = str(row_scope or "").strip()
    if (not t) or (not r):
        return False
    if t == r:
        return True
    if relaxed_scope_model_key(t) == relaxed_scope_model_key(r):
        return True

    # Handle model strings where Encar appends generation code in parentheses,
    # e.g. "모닝 어반" vs "모닝 어반 (JA)".
    tb = strip_scope_parenthesis(t)
    rb = strip_scope_parenthesis(r)
    if tb and rb and (tb == rb):
        return True
    if tb and rb and (relaxed_scope_model_key(tb) == relaxed_scope_model_key(rb)):
        return True
    return False


def escape_action_term(term):
    s = str(term or "").strip()
    if not s:
        return ""
    # Encar action grammar: "." and ")" inside node values should be escaped.
    s = re.sub(r"(?<!_)\.", "_.", s)
    s = re.sub(r"(?<!_)\)", "_)", s)
    return s


def build_direct_model_variants(model, model_toks, model_group_name, rows, limit=8):
    out = []
    model_raw = str(model or "").strip()
    model_clean = re.sub(r"\(.*?\)", " ", model_raw)
    model_clean = re.sub(r"(더\s*뉴|올\s*뉴|디\s*올\s*뉴|신형|페이스리프트)", " ", model_clean)
    model_clean = re.sub(r"\s+", " ", model_clean).strip()
    for mv in [model_raw, model_clean]:
        if mv and (mv not in out):
            out.append(mv)

    toks = [normalize_text_key(t) for t in (model_toks or []) if normalize_text_key(t)]
    group_key = normalize_text_key(model_group_name or "")
    target_scope = normalize_scope_model_key(model)
    scored = {}

    for r in (rows or []):
        mname = str(r.get("Model") or "").strip()
        if not mname:
            continue
        gname = str(r.get("ModelGroup") or "")
        mg_key = normalize_text_key(mname + " " + gname)
        if group_key and (group_key not in mg_key):
            continue
        if toks:
            if len(toks) >= 2:
                if not all(t in mg_key for t in toks):
                    continue
            elif not any(t in mg_key for t in toks):
                continue

        row_scope = normalize_scope_model_key(mname)
        score = 0
        if target_scope and row_scope:
            if target_scope == row_scope:
                score += 100
            elif model_scope_match(target_scope, row_scope):
                score += 70
        mk = normalize_text_key(mname)
        for t in toks:
            if t in mk:
                score += 6
        if "(" in mname and ")" in mname:
            score += 2

        prev = scored.get(mname)
        if (prev is None) or (score > prev):
            scored[mname] = score

    for name, _ in sorted(scored.items(), key=lambda kv: (-kv[1], len(kv[0]))):
        if name not in out:
            out.append(name)
        if len(out) >= limit:
            break
    return out[:limit]


def action_specificity_score(action_text):
    s = str(action_text or "")
    score = 0
    if "(C.Model." in s:
        score += 2
    if "C.BadgeGroup." in s:
        score += 2
    if "BadgeDetail." in s:
        score += 1
    if "C.Badge." in s:
        score += 1
    return score


def infer_badge_group_candidates_from_rows(
    rows,
    model_toks,
    model_group_name,
    anchor_badge,
    target_year=None,
    target_fuel_key=None,
    max_samples=6,
):
    if not ENABLE_ENCAR_DETAILED_FETCH:
        return []
    out = {}
    badge_key = normalize_text_key(anchor_badge or "")
    group_key = normalize_text_key(model_group_name or "")
    tok_keys = [normalize_text_key(t) for t in (model_toks or []) if normalize_text_key(t)]
    samples = []
    seen_ids = set()

    for r in (rows or []):
        ad_id = str(r.get("Id") or "").strip()
        if (not ad_id) or (ad_id in seen_ids):
            continue
        mname = str(r.get("Model") or "")
        gname = str(r.get("ModelGroup") or "")
        mg_key = normalize_text_key(mname + " " + gname)
        if group_key and (group_key not in mg_key):
            continue
        if tok_keys:
            # For badge-group inference, keep model-group strict but token check relaxed
            # so facelifts/renames can still contribute displacement evidence.
            if not any(t in mg_key for t in tok_keys):
                continue

        badge_text = normalize_text_key(str(r.get("Badge") or "") + " " + str(r.get("BadgeDetail") or ""))
        if badge_key and (badge_key not in badge_text):
            continue

        if target_year:
            yr = extract_year(r.get("Year"))
            if yr is None or yr != target_year:
                continue

        if target_fuel_key:
            row_fuel_key = fuel_key(str(r.get("FuelType") or ""))
            if row_fuel_key and (row_fuel_key != target_fuel_key):
                continue

        seen_ids.add(ad_id)
        samples.append((ad_id, str(r.get("FuelType") or "")))
        if len(samples) >= max_samples:
            break

    for ad_id, row_fuel in samples:
        try:
            detail = fetch_json_url(f"https://api.encar.com/v1/readside/vehicle/{ad_id}", timeout=8)
        except Exception:
            continue
        spec = detail.get("spec") or {}
        disp = to_int(spec.get("displacement"))
        if not disp:
            continue
        fuel_name = str(spec.get("fuelName") or row_fuel or "")
        fuel_label = _fuel_label_for_badge(fuel_name)
        if not fuel_label:
            continue
        cc = int(round(disp / 100.0) * 100)
        if cc <= 0:
            continue
        key = f"{fuel_label} {cc}cc"
        out[key] = (to_int(out.get(key)) or 0) + 1

    return [k for k, _ in sorted(out.items(), key=lambda kv: (-kv[1], kv[0]))]


def canonicalize_model_text(model_text):
    s = " ".join(str(model_text or "").replace("\ufeff", "").split())
    if not s:
        return s
    toks = [t for t in re.split(r"\s+", s) if t]
    # Drop only leading manufacturer tokens injected by some sources.
    def _norm_tok(x):
        return re.sub(r"[^0-9a-z가-힣]+", "", str(x or "").lower())
    while toks and _norm_tok(toks[0]) in MODEL_BRAND_NOISE_TOKENS:
        toks = toks[1:]
    out = []
    for t in toks:
        if out and out[-1] == t:
            continue
        out.append(t)
    if out:
        return " ".join(out).strip()
    return s


def parse_bool(v, default=False):
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in {"1", "y", "yes", "true", "on"}:
        return True
    if s in {"0", "n", "no", "false", "off"}:
        return False
    return default


def split_model_trim_title_line(line):
    s = " ".join(str(line or "").strip().split())
    if not s:
        return None, None

    # Typical combined title:
    # "현대 아반떼(CN7) 1.6 가솔린 모던"
    m = re.search(r"\s(\d\.\d(?:\s*터보)?\s*(?:가솔린|휘발유|디젤|경유|LPG|전기|하이브리드).+)$", s, flags=re.IGNORECASE)
    if m:
        model = s[:m.start(1)].strip()
        trim = m.group(1).strip()
        if model and trim:
            return model, trim

    anchors = sorted(set(TRIM_ANCHOR_KEYWORDS), key=len, reverse=True)
    a_pat = "|".join(re.escape(a) for a in anchors)
    m2 = re.search(rf"\s(\d\.\d(?:\s*터보)?(?:\s*(?:2WD|4WD|AWD|FF|FR))?\s*(?:{a_pat}).*)$", s, flags=re.IGNORECASE)
    if m2:
        model = s[:m2.start(1)].strip()
        trim = m2.group(1).strip()
        if model and trim:
            return model, trim

    # Fallback: split at first trim anchor if it appears after engine/displacement chunk.
    for a in anchors:
        pos = s.find(" " + a)
        if pos <= 0:
            continue
        left = s[:pos].strip()
        if re.search(r"\d\.\d|\d{3,4}\s*cc", left, flags=re.IGNORECASE):
            if " " in left:
                model = left.rsplit(" ", 1)[0].strip()
                trim = left.rsplit(" ", 1)[1].strip() + s[pos:]
                trim = " ".join(trim.split())
                if model and trim:
                    return model, trim

    return None, None


def prepare_source_text(raw_text, source_site="generic", model_override=None, trim_override=None):
    text = (raw_text or "").replace("\ufeff", "").replace("\r", "")
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    src = normalize_source_site(source_site)
    mo = str(model_override or "").strip()
    to = str(trim_override or "").strip()

    if mo and to:
        rest = lines[2:] if len(lines) >= 2 else lines
        return "\n".join([mo, to] + rest)

    if src in {"autoinside", "carauction", "gulliver"} and lines:
        m, t = split_model_trim_title_line(lines[0])
        if m and t:
            return "\n".join([m, t] + lines[1:])

    return text


def parse_text(raw_text, source_site="generic", model_override=None, trim_override=None):
    text = prepare_source_text(raw_text, source_site=source_site, model_override=model_override, trim_override=trim_override)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if len(lines) < 2:
        raise ValueError("첫 줄(모델), 둘째 줄(트림)을 포함해서 입력해 주세요.")

    model = canonicalize_model_text(lines[0].replace("\ufeff", "").strip())
    trim = lines[1].replace("\ufeff", "").strip()

    model_hints = {
        "모닝": "기아", "K3": "기아", "K5": "기아", "K8": "기아", "K9": "기아", "레이": "기아", "쏘렌토": "기아", "카니발": "기아",
        "아반떼": "현대", "쏘나타": "현대", "그랜저": "현대", "투싼": "현대", "싼타페": "현대", "캐스퍼": "현대",
        "GV70": "제네시스", "GV80": "제네시스", "G80": "제네시스",
        "티볼리": "KG모빌리티", "코란도": "KG모빌리티", "토레스": "KG모빌리티", "렉스턴": "KG모빌리티",
    }

    def hint_match(line_text, hint):
        line_norm = normalize_text_key(line_text)
        hint_norm = normalize_text_key(hint)
        if (not line_norm) or (not hint_norm):
            return False
        toks = line_norm.split()
        # Short/model-code hints should be exact token to avoid substring false positives.
        if (len(hint_norm) <= 2) or re.fullmatch(r"[a-z0-9]+", hint_norm):
            return hint_norm in toks
        return hint_norm in line_norm

    # Heuristic: if first line looks like trim and second line looks like spec,
    # treat this as missing model input and fail fast with a clear message.
    model_line_norm = normalize_text_key(model)
    trim_line_norm = normalize_text_key(trim)
    model_like_line = any(hint_match(model, k) for k in model_hints.keys())
    trim_like_line = (
        bool(re.search(r"\b\d\.\d\b", model_line_norm))
        or any(k in model for k in ["가솔린", "휘발유", "디젤", "LPG", "전기", "하이브리드", "인스퍼레이션", "모던", "프리미엄", "스마트"])
    )
    spec_like_line = bool(re.search(r"(\d{4}\s*년형|\d+\s*만\s*km|[\d,]+\s*km)", trim_line_norm, flags=re.IGNORECASE))
    if (not model_like_line) and trim_like_line and spec_like_line:
        raise ValueError("모델명이 없습니다. 첫 줄에 모델명을 넣어 주세요. 예: 올 뉴 아반떼 (CN7)")

    brand = ""
    for k in ["기아", "현대", "제네시스", "BMW", "벤츠", "아우디", "쉐보레", "르노", "KG", "쌍용", "KG모빌리티"]:
        if k in text:
            if k in ["KG", "쌍용", "KG모빌리티"]:
                brand = "KG모빌리티"
            else:
                brand = k
            break
    if not brand:
        for k, v in model_hints.items():
            if hint_match(model, k):
                brand = v
                break

    ym = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월", text)
    reg_year = to_int(ym.group(1)) if ym else None
    reg_month = to_int(ym.group(2)) if ym else None
    if reg_year is None:
        y2 = re.search(r"(\d{4})\s*년형", text)
        if y2:
            reg_year = to_int(y2.group(1))

    ft = re.search(r"(휘발유|가솔린|디젤|LPG|전기)\s*[ㆍ·]\s*(오토|자동|수동|CVT)", text)
    fuel = ft.group(1) if ft else None
    transmission = ft.group(2) if ft else None
    if fuel is None:
        f2 = re.search(r"(휘발유|가솔린|디젤|경유|LPG|전기|하이브리드)", text)
        if f2:
            fuel = f2.group(1)
    if transmission is None:
        t2 = re.search(r"(오토|자동|수동|CVT)", text)
        if t2:
            transmission = t2.group(1)

    mileage = None
    m = re.search(r"([\d,]+)km", text)
    if m:
        mileage = to_int(m.group(1))
    if mileage is None:
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*만\s*km", text, flags=re.IGNORECASE)
        if m2:
            mileage = int(round(float(m2.group(1)) * 10000))

    new_price = None
    np = re.search(r"신차정가\(옵션[^)]*\)\s*:\s*(?:약\s*)?([\d,]+)\s*만원", text)
    if np:
        new_price = to_int(np.group(1))

    options = []
    sec = re.search(r"신차정가\(옵션[^)]*\)[\s\S]*?\n([\s\S]*?)\n\s*\*\s*신차 추가옵션", text)
    if sec:
        for ln in sec.group(1).split("\n"):
            mo = re.match(r"\d+\)\s*(.+)$", ln.strip())
            if mo:
                options.append(mo.group(1).strip())

    owner_changes = None
    if "소유자 변경" in text and "없음" in text:
        owner_changes = 0
    if owner_changes is None and (("1인소유" in text) or ("1인 신조" in text)):
        owner_changes = 0
    oc = re.search(r"소유자 변경\s*\((\d+)회", text)
    if oc:
        owner_changes = to_int(oc.group(1))

    my_accident_count = None
    md = re.search(r"내차 피해\s*\((\d+)건\)", text)
    if md:
        my_accident_count = to_int(md.group(1))
    elif "내차 피해" in text and "없음" in text:
        my_accident_count = 0
    elif "완무" in text:
        i0 = re.search(r"보험\s*(\d+)\s*건", text)
        if i0:
            my_accident_count = to_int(i0.group(1))

    panel_count = None
    p4 = re.search(r"외판\s*\n\s*(\d+)판", text)
    if p4:
        panel_count = to_int(p4.group(1))

    return {
        "source_site": normalize_source_site(source_site),
        "brand": brand,
        "model": model,
        "trim": trim,
        "registration_year": reg_year,
        "registration_month": reg_month,
        "fuel": fuel,
        "transmission": transmission,
        "mileage_km": mileage,
        "new_price": new_price,
        "options": dedupe_option_texts(options, similarity=0.90),
        "accident_free": ("무사고" in text),
        "simple_exchange": ("단순교환" in text),
        "my_accident_count": my_accident_count,
        "panel_count": panel_count,
        "owner_changes": owner_changes,
    }


def tokenize_model(text):
    s = (text or "").replace("\ufeff", "")
    for n in ["세대", "더 뉴", "올 뉴", "디 올 뉴", "페이스리프트", "신형"]:
        s = s.replace(n, " ")
    s = re.sub(r"[()]", " ", s)
    toks = [t for t in re.split(r"\s+", s) if t]
    out = []
    for t in toks:
        tk = normalize_scope_model_key(t)
        if tk in MODEL_BRAND_NOISE_TOKENS:
            continue
        if len(t) >= 2 or any(ch.isdigit() for ch in t):
            out.append(t)
    return out


def trim_anchor_tokens(trim_text):
    s = str(trim_text or "")
    known = TRIM_ANCHOR_KEYWORDS
    anchors = [k for k in known if k in s]
    if anchors:
        return list(dict.fromkeys(anchors))

    junk = {
        "가솔린", "휘발유", "디젤", "전기", "LPG", "하이브리드", "오토", "자동", "수동", "CVT",
        "2WD", "4WD", "AWD", "터보", "세단", "왜건", "쿠페", "스포츠", "1.6", "2.0", "3.3",
    }
    toks = [t for t in re.split(r"\s+", s) if t]
    out = []
    for t in toks:
        if t in junk:
            continue
        if re.fullmatch(r"[\d\.\-/]+", t):
            continue
        if len(t) >= 2:
            out.append(t)
    return list(dict.fromkeys(out[:3]))


def pick_primary_trim_anchor(trim_text, anchors):
    txt = str(trim_text or "")
    if not anchors:
        return None
    best = None
    best_pos = -1
    best_len = -1
    for a in anchors:
        pos = txt.rfind(str(a))
        if pos < 0:
            pos = -1
        alen = len(str(a))
        if (pos > best_pos) or (pos == best_pos and alen > best_len):
            best = a
            best_pos = pos
            best_len = alen
    return best or anchors[0]


def parse_int_from_text(text, key):
    if not text:
        return None
    m = re.search(rf"{re.escape(key)}=(\d+)", text)
    return to_int(m.group(1)) if m else None


def normalize_text_key(text):
    s = str(text or "").lower()
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"(더\s*뉴|올\s*뉴|디\s*올\s*뉴|신형|페이스리프트|세대)", " ", s)
    s = re.sub(r"[^0-9a-z가-힣]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def load_option_synonyms():
    global _OPTION_SYNONYM_CACHE, _OPTION_SYNONYM_MTIME
    try:
        mtime = OPTION_SYNONYMS_PATH.stat().st_mtime
    except Exception:
        _OPTION_SYNONYM_CACHE = {"exact": {}, "contains": {}}
        _OPTION_SYNONYM_MTIME = None
        return _OPTION_SYNONYM_CACHE

    if (_OPTION_SYNONYM_CACHE is not None) and (_OPTION_SYNONYM_MTIME == mtime):
        return _OPTION_SYNONYM_CACHE

    try:
        data = json.loads(OPTION_SYNONYMS_PATH.read_text(encoding="utf-8"))
    except Exception:
        data = {}

    exact = {}
    contains = {}
    for src, dst in (data.get("exact") or {}).items():
        sk = normalize_text_key(src)
        dk = normalize_text_key(dst)
        if sk and dk:
            exact[sk] = dk
    for src, dst in (data.get("contains") or {}).items():
        sk = normalize_text_key(src)
        dk = normalize_text_key(dst)
        if sk and dk:
            contains[sk] = dk

    _OPTION_SYNONYM_CACHE = {"exact": exact, "contains": contains}
    _OPTION_SYNONYM_MTIME = mtime
    return _OPTION_SYNONYM_CACHE


def apply_option_synonym_text(text):
    s = normalize_text_key(text)
    if not s:
        return s
    syn = load_option_synonyms()
    exact = syn.get("exact") or {}
    contains = syn.get("contains") or {}
    if s in exact:
        s = exact[s]
    for src, dst in contains.items():
        if src in s:
            s = s.replace(src, dst)
    return re.sub(r"\s+", " ", s).strip()


def transmission_key(v):
    s = str(v or "").upper().replace(" ", "")
    if s in {"A/T", "AT", "AUTO", "오토", "자동"}:
        return "auto"
    if s in {"M/T", "MT", "수동", "MANUAL"}:
        return "manual"
    if "CVT" in s:
        return "cvt"
    return s.lower()


def trim_profile(trim_text, fuel_text=None, transmission_text=None):
    raw = str(trim_text or "")
    s = normalize_text_key(trim_text)
    fk = fuel_key(fuel_text or "")
    tk = transmission_key(transmission_text or "")
    displacement = sorted(set(re.findall(r"\b\d\.\d\b", raw)))

    drive = None
    up = str(trim_text or "").upper()
    if "AWD" in up:
        drive = "awd"
    elif "4WD" in up:
        drive = "4wd"
    elif "2WD" in up:
        drive = "2wd"

    turbo = None
    if ("터보" in str(trim_text or "")) or (" TURBO" in up):
        turbo = True
    elif displacement:
        turbo = False

    power = None
    base = str(trim_text or "") + " " + str(fuel_text or "")
    if ("전기" in base) or ("EV" in base.upper()):
        power = "ev"
    elif ("하이브리드" in base) or ("HEV" in base.upper()) or ("PHEV" in base.upper()):
        power = "hybrid"
    elif ("LPG" in base.upper()):
        power = "lpg"
    elif ("디젤" in base) or ("경유" in base):
        power = "diesel"
    elif ("가솔린" in base) or ("휘발유" in base):
        power = "gasoline"
    elif fk in {"ev", "hybrid", "lpg", "diesel", "gasoline"}:
        power = fk

    return {
        "displacement": displacement,
        "drive": drive,
        "turbo": turbo,
        "power": power,
        "fuel_key": fk if fk else None,
        "transmission_key": tk if tk else None,
    }


def _base_trim_profile_match(target, row):
    t_disp = target.get("displacement") or []
    r_disp = row.get("displacement") or []

    t_drive = target.get("drive")
    r_drive = row.get("drive")
    if t_drive and r_drive and t_drive != r_drive:
        return False

    t_power = target.get("power")
    r_power = row.get("power")
    if t_power and r_power and t_power != r_power:
        return False

    t_fk = target.get("fuel_key")
    r_fk = row.get("fuel_key")
    if t_fk and r_fk and t_fk != r_fk:
        return False

    t_turbo = target.get("turbo")
    r_turbo = row.get("turbo")
    if (t_turbo is not None) and (r_turbo is not None) and (t_turbo != r_turbo):
        return False

    t_trans = target.get("transmission_key")
    r_trans = row.get("transmission_key")
    if t_trans and r_trans and t_trans != r_trans:
        return False

    return True


def same_trim_profile(target, row):
    if not _base_trim_profile_match(target, row):
        return False
    t_disp = target.get("displacement") or []
    r_disp = row.get("displacement") or []
    # Some Encar trims omit displacement in grade text (e.g. "디 에센셜").
    # Enforce displacement only when both sides provide it.
    if t_disp and r_disp and (not any(d in r_disp for d in t_disp)):
        return False
    return True


OPTION_KEYWORDS = {
    "style": ["스타일"],
    "navigation": ["내비", "네비", "내비게이션", "네비게이션"],
    "drivewise": ["드라이브와이즈", "드라이브 와이즈", "스마트센스", "스마트 센스"],
    "sunroof": ["선루프", "썬루프"],
    "comfort": ["컴포트"],
    "smart_connect": ["하이패스", "ecm"],
}

OPTION_CODE_KEYS = {
    "1036": "style",
    "1037": "drivewise",
    "1038": "navigation",
    "1042": "navigation",
}

ROMAN_CHAR_MAP = str.maketrans({
    "Ⅰ": "1", "Ⅱ": "2", "Ⅲ": "3", "Ⅳ": "4", "Ⅴ": "5",
    "Ⅵ": "6", "Ⅶ": "7", "Ⅷ": "8", "Ⅸ": "9", "Ⅹ": "10",
    "ⅰ": "1", "ⅱ": "2", "ⅲ": "3", "ⅳ": "4", "ⅴ": "5",
    "ⅵ": "6", "ⅶ": "7", "ⅷ": "8", "ⅸ": "9", "ⅹ": "10",
})

ROMAN_TOKEN_MAP = {
    "x": "10", "ix": "9", "viii": "8", "vii": "7", "vi": "6",
    "v": "5", "iv": "4", "iii": "3", "ii": "2", "i": "1",
}

TRIM_NOISE_TOKENS = {
    "가솔린", "휘발유", "디젤", "경유", "전기", "하이브리드", "lpg", "ev", "hev", "phev",
    "오토", "자동", "수동", "cvt", "at", "mt", "turbo", "터보",
}


def detect_option_keys_from_text(texts):
    found = set()
    source = " ".join([str(x or "") for x in (texts or [])]).lower()
    for key, kws in OPTION_KEYWORDS.items():
        if any(kw.lower() in source for kw in kws):
            found.add(key)
    return found


def detect_option_keys_from_codes(codes):
    found = set()
    for cd in (codes or []):
        k = OPTION_CODE_KEYS.get(str(cd))
        if k:
            found.add(k)
    return found


def normalize_option_label(text):
    s = str(text or "").strip()
    if not s:
        return ""
    s = s.translate(ROMAN_CHAR_MAP)
    s = re.sub(r"^\d+\)\s*", "", s)
    s = re.sub(r"\([\d,\s]*만원[^)]*\)", " ", s)
    s = re.sub(r"\(\s*기본형[^)]*\)", " ", s)
    s = re.sub(r"\(\s*선택[^)]*\)", " ", s)
    s = re.sub(r"\b(turbo|lpi|hev|ev)\b", " ", s, flags=re.IGNORECASE)
    s = normalize_text_key(s)
    s = apply_option_synonym_text(s)
    if not s:
        return ""
    out = []
    for tok in s.split():
        if tok in ROMAN_TOKEN_MAP:
            out.append(ROMAN_TOKEN_MAP[tok])
        else:
            out.append(tok)
    return " ".join(out).strip()


def option_similarity(a, b):
    ak = normalize_option_label(a)
    bk = normalize_option_label(b)
    if not ak or not bk:
        return 0.0
    ac = ak.replace(" ", "")
    bc = bk.replace(" ", "")
    if ac == bc:
        return 1.0
    if ac and bc and (ac in bc or bc in ac):
        return 0.92
    at = set(ak.split())
    bt = set(bk.split())
    if not at or not bt:
        return 0.0
    inter = len(at & bt)
    union = len(at | bt)
    jac = (inter / union) if union else 0.0
    nums_a = set(re.findall(r"\d+", ak))
    nums_b = set(re.findall(r"\d+", bk))
    num_bonus = 0.08 if (nums_a and nums_b and (nums_a & nums_b)) else 0.0
    return jac + num_bonus


def dedupe_option_texts(raw_options, similarity=0.90):
    out = []
    seen_keys = set()
    for raw in (raw_options or []):
        name = " ".join(str(raw or "").replace("\ufeff", "").split())
        if not name:
            continue
        key = normalize_option_label(name) or normalize_text_key(name) or name
        if key in seen_keys:
            continue
        dup = False
        for ex in out:
            if option_similarity(name, ex) >= similarity:
                dup = True
                break
        if dup:
            continue
        seen_keys.add(key)
        out.append(name)
    return out


def is_numeric_option_code(code):
    return bool(re.fullmatch(r"\d+", str(code or "").strip()))


def build_text_option_catalog(raw_options):
    out = {}
    for name in dedupe_option_texts(raw_options, similarity=0.90):
        key = normalize_option_label(name) or normalize_text_key(name)
        if not key:
            continue
        code = "TXT_" + hashlib.sha1(key.encode("utf-8")).hexdigest()[:12].upper()
        out[code] = name
    return out


def map_target_options_to_codes(raw_options, option_catalog):
    raw_options = dedupe_option_texts(raw_options, similarity=0.90)
    target_codes = []
    target_names = []
    unmatched = []
    if not raw_options:
        return target_codes, target_names, unmatched
    items = [
        (str(cd), str(nm or "").strip())
        for cd, nm in (option_catalog or {}).items()
        if is_numeric_option_code(cd) and str(nm or "").strip()
    ]
    if not items:
        return target_codes, target_names, [str(x).strip() for x in raw_options if str(x).strip()]
    used = set()
    for raw in raw_options:
        raw_s = str(raw or "").strip()
        if not raw_s:
            continue
        best = None
        best_score = 0.0
        for cd, nm in items:
            sc = option_similarity(raw_s, nm)
            if sc > best_score:
                best = (cd, nm)
                best_score = sc
        if best and best_score >= 0.58:
            cd, nm = best
            if cd not in used:
                used.add(cd)
                target_codes.append(cd)
                target_names.append(nm)
        else:
            unmatched.append(raw_s)
    return target_codes, target_names, unmatched


def build_option_scope_items(option_catalog_by_code, selected_codes=None, selected_names=None):
    selected = set(str(cd) for cd in (selected_codes or []))
    selected_name_list = [str(x or "").strip() for x in dedupe_option_texts(selected_names or [], similarity=0.90) if str(x or "").strip()]
    merged = {}
    for cd, nm in (option_catalog_by_code or {}).items():
        code = str(cd or "").strip()
        name = str(nm or "").strip()
        if (not code) or (not name):
            continue
        key = normalize_option_label(name) or normalize_text_key(name) or name
        item = merged.get(key)
        if item is None and merged:
            # Merge near-duplicate labels (e.g. same option with explanatory note suffix).
            for ex in merged.values():
                if option_similarity(name, ex.get("name")) >= 0.90:
                    item = ex
                    break
        if item is None:
            item = {"key": key, "name": name, "codes": set()}
            merged[key] = item
        item["codes"].add(code)

    out = []
    for item in merged.values():
        numeric_codes = sorted([cd for cd in item["codes"] if is_numeric_option_code(cd)])
        if numeric_codes:
            is_selected = bool(selected.intersection(numeric_codes))
        else:
            is_selected = any(option_similarity(item.get("name"), sn) >= 0.90 for sn in selected_name_list)
        out.append(
            {
                "key": item["key"],
                "name": item["name"],
                "codes": numeric_codes,
                "selected": is_selected,
            }
        )
    out.sort(key=lambda x: normalize_text_key(x.get("name")))
    return out


def build_option_scope_items_from_text(raw_options):
    items = []
    for raw in (raw_options or []):
        name = str(raw or "").strip()
        if not name:
            continue
        found = None
        for it in items:
            if option_similarity(name, it.get("name")) >= 0.90:
                found = it
                break
        if found is None:
            items.append(
                {
                    "key": normalize_option_label(name) or normalize_text_key(name) or name,
                    "name": name,
                    "codes": [],
                    "selected": True,
                }
            )
    items.sort(key=lambda x: normalize_text_key(x.get("name")))
    return items


def comp_matches_option_text(comp_item, option_name):
    name = str(option_name or "").strip()
    if not name:
        return False
    for rn in (comp_item.get("selected_option_names") or []):
        if option_similarity(name, rn) >= 0.58:
            return True
    target_keys = detect_option_keys_from_text([name])
    row_keys = set(comp_item.get("option_keys") or [])
    if target_keys and row_keys and (set(target_keys) & row_keys):
        return True
    return False


def build_option_catalog_scope(parsed, target_profile=None, trim_anchors=None):
    profile = target_profile or {}
    trim_text = str(parsed.get("trim") or "")
    anchors = list(trim_anchors or trim_anchor_tokens(trim_text))
    primary_anchor = pick_primary_trim_anchor(trim_text, anchors) if anchors else None
    disp = (profile.get("displacement") or [None])[0]
    return {
        "brand": str(parsed.get("brand") or "").strip(),
        "model_key": normalize_scope_model_key(parsed.get("model")),
        "trim_anchor_key": normalize_text_key(primary_anchor or ""),
        "power_key": str(profile.get("power") or "").strip().lower(),
        "displacement_key": str(disp or "").strip(),
        "drive_key": str(profile.get("drive") or "").strip().lower(),
        "transmission_key": str(profile.get("transmission_key") or "").strip().lower(),
    }


def load_option_catalog_by_scope(scope):
    brand = str((scope or {}).get("brand") or "").strip()
    model_key = str((scope or {}).get("model_key") or "").strip()
    if (not brand) or (not model_key):
        return {}
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT trim_anchor_key, power_key, displacement_key, drive_key, transmission_key,
                   option_code, option_name, seen_count
            FROM encar_option_catalog
            WHERE brand = ? AND model_key = ?
            ORDER BY seen_count DESC, last_seen_at DESC
            LIMIT 3000
            """,
            (brand, model_key),
        ).fetchall()
    except Exception:
        conn.close()
        return {}
    conn.close()

    scope_trim = str(scope.get("trim_anchor_key") or "")
    scope_power = str(scope.get("power_key") or "")
    scope_disp = str(scope.get("displacement_key") or "")
    scope_drive = str(scope.get("drive_key") or "")
    scope_trans = str(scope.get("transmission_key") or "")

    scored = {}
    for r in rows:
        row_power = str(r["power_key"] or "")
        row_disp = str(r["displacement_key"] or "")
        if scope_power and row_power and (scope_power != row_power):
            continue
        if scope_disp and row_disp and (scope_disp != row_disp):
            continue

        sc = 0
        row_trim = str(r["trim_anchor_key"] or "")
        if scope_trim and row_trim and (scope_trim == row_trim):
            sc += 4
        elif scope_trim and row_trim and (scope_trim != row_trim):
            sc -= 1
        if scope_power and row_power and (scope_power == row_power):
            sc += 3
        if scope_disp and row_disp and (scope_disp == row_disp):
            sc += 3
        row_drive = str(r["drive_key"] or "")
        if scope_drive and row_drive and (scope_drive == row_drive):
            sc += 1
        row_trans = str(r["transmission_key"] or "")
        if scope_trans and row_trans and (scope_trans == row_trans):
            sc += 1
        if sc < 2:
            continue

        cd = str(r["option_code"] or "").strip()
        nm = str(r["option_name"] or "").strip()
        if (not cd) or (not nm):
            continue
        prev = scored.get(cd)
        row_seen = to_int(r["seen_count"]) or 0
        rank = (sc, row_seen)
        if (prev is None) or (rank > prev[0]):
            scored[cd] = (rank, nm)

    return {cd: v[1] for cd, v in scored.items()}


def upsert_option_catalog_by_scope(scope, option_catalog):
    brand = str((scope or {}).get("brand") or "").strip()
    model_key = str((scope or {}).get("model_key") or "").strip()
    if (not brand) or (not model_key) or (not option_catalog):
        return 0

    trim_anchor_key = str(scope.get("trim_anchor_key") or "").strip()
    power_key = str(scope.get("power_key") or "").strip().lower()
    displacement_key = str(scope.get("displacement_key") or "").strip()
    drive_key = str(scope.get("drive_key") or "").strip().lower()
    transmission_key = str(scope.get("transmission_key") or "").strip().lower()
    ts = now_iso()

    rows = []
    for cd, nm in (option_catalog or {}).items():
        code = str(cd or "").strip()
        name = str(nm or "").strip()
        if (not code) or (not name):
            continue
        rows.append(
            (
                brand, model_key, trim_anchor_key, power_key, displacement_key, drive_key, transmission_key,
                code, name, ts, ts,
            )
        )
    if not rows:
        return 0

    conn = get_conn()
    try:
        conn.executemany(
            """
            INSERT INTO encar_option_catalog(
                brand, model_key, trim_anchor_key, power_key, displacement_key, drive_key, transmission_key,
                option_code, option_name, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(brand, model_key, trim_anchor_key, power_key, displacement_key, drive_key, transmission_key, option_code)
            DO UPDATE SET
                option_name = excluded.option_name,
                seen_count = encar_option_catalog.seen_count + 1,
                last_seen_at = excluded.last_seen_at
            """,
            rows,
        )
        conn.commit()
    except Exception:
        conn.close()
        return 0
    conn.close()
    return len(rows)


def mileage_window(target_km):
    km = to_int(target_km)
    if km is None:
        return None, None
    lo = max(0, km - 20000)
    hi = km + 30000
    return lo, hi


def market_mileage_window(target_km):
    # Match manual Encar search behavior:
    # use nearest-lower 10k-km anchor and apply +-20k window.
    km = to_int(target_km)
    if km is None:
        return None, None, None
    base = (km // 10000) * 10000
    lo = max(0, base - 20000)
    hi = base + 20000
    return lo, hi, base


def review_mileage_upper_km(target_km, fallback_hi=None):
    km = to_int(target_km)
    if km is None:
        km = to_int(fallback_hi)
    if km is None:
        return None
    raw_hi = km + REVIEW_MILEAGE_EXTRA_KM
    rounded = (raw_hi // 10000) * 10000
    return max(10000, rounded)


def build_review_action_query(
    direct_action,
    *,
    review_km_hi,
    maker,
    model_group_name,
    target_year,
    model_name=None,
    car_type="Y",
):
    if review_km_hi is None:
        return None
    if direct_action:
        return re.sub(r"Mileage\.range\([^)]*\)", f"Mileage.range(..{review_km_hi})", direct_action)
    mk_term = escape_action_term(maker)
    model_group_term = escape_action_term(model_group_name)
    model_term = escape_action_term(model_name)
    if not (mk_term and target_year):
        return None
    year_from = f"{target_year}01"
    year_to = f"{target_year}12"
    if model_group_term and model_term:
        return (
            f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
            f"(C.ModelGroup.{model_group_term}._.(C.Model.{model_term}.))))_."
            f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
        )
    if model_group_term:
        return (
            f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
            f"(C.ModelGroup.{model_group_term}.)))_."
            f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
        )
    return (
        f"(And.Hidden.N._.(C.CarType.{car_type}._.Manufacturer.{mk_term}.)_."
        f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
    )


def is_valid_encar_action_query(action_query):
    if not action_query:
        return False
    try:
        list_url = build_encar_list_url(
            action_query,
            sr="|Mileage|0|20",
            count="true",
            inav="|Metadata|Sort",
        )
        fetch_json_url(list_url, timeout=8)
        return True
    except Exception:
        return False


def encar_action_first_page_count(action_query):
    if not action_query:
        return -1
    try:
        list_url = build_encar_list_url(
            action_query,
            sr="|Mileage|0|20",
            count="true",
            inav="|Metadata|Sort",
        )
        data = fetch_json_url(list_url, timeout=8)
        rows = data.get("SearchResults") or []
        return len(rows)
    except Exception:
        return -1


def encar_action_preview_rows(action_query, limit=20):
    if not action_query:
        return []
    try:
        list_url = build_encar_list_url(
            action_query,
            sr=f"|Mileage|0|{max(1, int(limit))}",
            count="true",
            inav="|Metadata|Sort",
        )
        data = fetch_json_url(list_url, timeout=8)
        return data.get("SearchResults") or []
    except Exception:
        return []


def score_review_action_rows(
    rows,
    model_group_name=None,
    target_model_scope=None,
    target_fuel_key=None,
    manual_badge_candidates=None,
    anchor_badge=None,
    badge_group_candidates=None,
    drive_badge_text=None,
):
    rows = rows or []
    if not rows:
        return (0, 0, 0, 0)
    mg_key = normalize_text_key(model_group_name or "")
    trim_terms = []
    for mb in (manual_badge_candidates or []):
        k = normalize_text_key(mb)
        if k:
            trim_terms.append(k)
    ab = normalize_text_key(anchor_badge or "")
    if ab:
        trim_terms.append(ab)
    trim_terms = list(dict.fromkeys(trim_terms))
    bg_terms = []
    for bg in (badge_group_candidates or []):
        k = normalize_text_key(bg)
        if k:
            bg_terms.append(k)
    bg_terms = list(dict.fromkeys(bg_terms))
    drive_key = normalize_text_key(drive_badge_text or "")

    trim_hits = 0
    drive_hits = 0
    bg_hits = 0
    mg_hits = 0
    model_exact_hits = 0
    fuel_hits = 0
    for r in rows:
        row_trim_key = normalize_text_key(str(r.get("Badge") or "") + " " + str(r.get("BadgeDetail") or ""))
        row_badge_key = normalize_text_key(r.get("Badge") or "")
        row_bg_key = normalize_text_key(r.get("BadgeGroup") or "")
        row_mg_key = normalize_text_key(r.get("ModelGroup") or "")
        row_model_scope = normalize_scope_model_key(r.get("Model"))
        row_fuel_key = fuel_key(r.get("FuelType"))
        if trim_terms and any(t and (t in row_trim_key) for t in trim_terms):
            trim_hits += 1
        if drive_key and row_badge_key and (drive_key in row_badge_key):
            drive_hits += 1
        if bg_terms and any(t and (t in row_bg_key) for t in bg_terms):
            bg_hits += 1
        if mg_key and (mg_key == row_mg_key):
            mg_hits += 1
        if target_model_scope and row_model_scope and (row_model_scope == target_model_scope):
            model_exact_hits += 1
        if target_fuel_key and row_fuel_key and (target_fuel_key == row_fuel_key):
            fuel_hits += 1
    return (model_exact_hits, fuel_hits, drive_hits, trim_hits, bg_hits, mg_hits, len(rows))


def choose_review_action_query(
    direct_action,
    *,
    review_km_hi,
    maker_candidates,
    preferred_maker,
    model_group_name,
    target_year,
    target_model_scope=None,
    target_fuel_key=None,
    model_candidates=None,
    model_name=None,
    badge_group_candidates=None,
    drive_badge_text=None,
    manual_badge_candidates=None,
    anchor_badge=None,
    car_type="Y",
):
    candidates = []
    direct_candidate = None
    if direct_action:
        direct_candidate = build_review_action_query(
            direct_action,
            review_km_hi=review_km_hi,
            maker=preferred_maker,
            model_group_name=model_group_name,
            target_year=target_year,
            model_name=model_name,
            car_type=car_type,
        )

    mk_list = []
    if preferred_maker:
        mk_list.append(str(preferred_maker))
    for mk in (maker_candidates or []):
        mks = str(mk or "").strip()
        if mks and (mks not in mk_list):
            mk_list.append(mks)

    year_from = f"{target_year}01" if target_year else ""
    year_to = f"{target_year}12" if target_year else ""

    bg_list = [str(x).strip() for x in (badge_group_candidates or []) if str(x).strip()]
    bg_list = list(dict.fromkeys(bg_list[:5]))

    def add_candidate(s):
        if s and (s not in candidates):
            candidates.append(s)

    for mk in mk_list:
        mk_term = escape_action_term(mk)
        mg_term = escape_action_term(model_group_name)
        for mdl in (model_candidates or []):
            mdl_term = escape_action_term(mdl)
            if mk_term and mg_term and mdl_term and year_from and year_to:
                add_candidate(
                    f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                    f"(C.ModelGroup.{mg_term}._.(C.Model.{mdl_term}.))))_."
                    f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                )
                if drive_badge_text and anchor_badge:
                    dbt = escape_action_term(drive_badge_text)
                    abt = escape_action_term(anchor_badge)
                    if dbt and abt:
                        add_candidate(
                            f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                            f"(C.ModelGroup.{mg_term}._.(C.Model.{mdl_term}._."
                            f"(C.Badge.{dbt}._.BadgeDetail.{abt}.)))))_."
                            f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                        )
        drive_badge_term = escape_action_term(drive_badge_text)
        if drive_badge_term and mk_term and mg_term and year_from and year_to:
            if anchor_badge:
                ab_term = escape_action_term(anchor_badge)
                if ab_term:
                    add_candidate(
                        f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                        f"(C.ModelGroup.{mg_term}._.(C.Badge.{drive_badge_term}._.BadgeDetail.{ab_term}.)))))_."
                        f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                    )
            add_candidate(
                f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                f"(C.ModelGroup.{mg_term}._.Badge.{drive_badge_term}.)))_."
                f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
            )
            for bg in bg_list:
                bg_term = escape_action_term(bg)
                if bg_term:
                    if anchor_badge:
                        ab_term = escape_action_term(anchor_badge)
                        if ab_term:
                            add_candidate(
                                f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                                f"(C.ModelGroup.{mg_term}._.(C.BadgeGroup.{bg_term}._."
                                f"(C.Badge.{drive_badge_term}._.BadgeDetail.{ab_term}.))))))_."
                                f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                            )
                    add_candidate(
                        f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                        f"(C.ModelGroup.{mg_term}._.(C.BadgeGroup.{bg_term}._.Badge.{drive_badge_term}.)))))_."
                        f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                    )
        # 1) strict trim candidates first: Badge + BadgeDetail (e.g. V3 + 스페셜)
        for mb in (manual_badge_candidates or []):
            mbs = " ".join(str(mb or "").split()).strip()
            if not (mbs and mk_term and mg_term and year_from and year_to):
                continue
            toks = mbs.split()
            if len(toks) >= 2:
                b = escape_action_term(toks[0])
                d = escape_action_term(" ".join(toks[1:]))
                if b and d:
                    add_candidate(
                        f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                        f"(C.ModelGroup.{mg_term}._.(C.Badge.{b}._.BadgeDetail.{d}.)))))_."
                        f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                    )
                    for bg in bg_list:
                        bg_term = escape_action_term(bg)
                        if bg_term:
                            add_candidate(
                                f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                                f"(C.ModelGroup.{mg_term}._.(C.BadgeGroup.{bg_term}._."
                                f"(C.Badge.{b}._.BadgeDetail.{d}.))))))_."
                                f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                            )
            if anchor_badge and (normalize_text_key(mbs) != normalize_text_key(anchor_badge)):
                b = escape_action_term(anchor_badge)
                d = escape_action_term(mbs)
                if b and d:
                    add_candidate(
                        f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                        f"(C.ModelGroup.{mg_term}._.(C.Badge.{b}._.BadgeDetail.{d}.)))))_."
                        f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                    )
            mbs_term = escape_action_term(mbs)
            if mbs_term:
                add_candidate(
                    f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                    f"(C.ModelGroup.{mg_term}._.BadgeDetail.{mbs_term}.)))_."
                    f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                )
                add_candidate(
                    f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                    f"(C.ModelGroup.{mg_term}._.Badge.{mbs_term}.)))_."
                    f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                )
                for bg in bg_list:
                    bg_term = escape_action_term(bg)
                    if bg_term:
                        add_candidate(
                            f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                            f"(C.ModelGroup.{mg_term}._.(C.BadgeGroup.{bg_term}._.Badge.{mbs_term}.)))))_."
                            f"Year.range({year_from}..{year_to})._.Mileage.range(..{review_km_hi}).)"
                        )

        # 2) model-group candidate
        act1 = build_review_action_query(
            None,
            review_km_hi=review_km_hi,
            maker=mk,
            model_group_name=model_group_name,
            target_year=target_year,
            model_name=model_name,
            car_type=car_type,
        )
        add_candidate(act1)
        # 3) maker-only broad backup
        act2 = build_review_action_query(
            None,
            review_km_hi=review_km_hi,
            maker=mk,
            model_group_name=None,
            target_year=target_year,
            model_name=None,
            car_type=car_type,
        )
        add_candidate(act2)

    if direct_candidate:
        add_candidate(direct_candidate)

    checked = 0
    best_action = None
    best_score = (-1, -1, -1, -1)
    for act in candidates:
        if checked >= 24:
            break
        checked += 1
        rows = encar_action_preview_rows(act, limit=20)
        if not rows:
            continue
        sc = score_review_action_rows(
            rows,
            model_group_name=model_group_name,
            target_model_scope=target_model_scope,
            target_fuel_key=target_fuel_key,
            manual_badge_candidates=manual_badge_candidates,
            anchor_badge=anchor_badge,
            badge_group_candidates=badge_group_candidates,
            drive_badge_text=drive_badge_text,
        )
        if sc > best_score:
            best_score = sc
            best_action = act
        # Strong hit: many trim/badge-group matches on first page.
        if (sc[0] >= 2) and (sc[1] >= 2) and (sc[2] >= 2) and (sc[3] >= 2):
            return act
    if best_action:
        return best_action

    for act in candidates:
        if is_valid_encar_action_query(act):
            return act
    return candidates[0] if candidates else None


def infer_model_group_name_from_rows(rows, model_toks=None, target_model_scope=None):
    model_toks = [normalize_text_key(t) for t in (model_toks or []) if normalize_text_key(t)]
    scored = {}
    for r in (rows or []):
        g = str(r.get("ModelGroup") or "").strip()
        if not g:
            continue
        row_model_scope = normalize_scope_model_key(r.get("Model"))
        if target_model_scope and row_model_scope and (not model_scope_match(target_model_scope, row_model_scope)):
            continue
        key = normalize_text_key(g)
        if not key:
            continue
        mg_key = normalize_text_key(str(r.get("Model") or "") + " " + g)
        bonus = 0
        if model_toks and any(t in mg_key for t in model_toks):
            bonus = 1
        scored[g] = (to_int(scored.get(g)) or 0) + 1 + bonus
    if not scored:
        return None
    return sorted(scored.items(), key=lambda kv: (-kv[1], len(str(kv[0]))))[0][0]


def build_review_model_candidates(model_name, rows=None, target_model_scope=None, limit=6):
    out = []
    base = str(model_name or "").strip()
    if base:
        out.append(base)
    score = {}
    for r in (rows or []):
        nm = str(r.get("Model") or "").strip()
        if not nm:
            continue
        row_scope = normalize_scope_model_key(nm)
        exact = 1 if (target_model_scope and row_scope == target_model_scope) else 0
        if target_model_scope and row_scope and (not model_scope_match(target_model_scope, row_scope)):
            continue
        prev = score.get(nm) or [0, 0]
        prev[0] += exact
        prev[1] += 1
        score[nm] = prev
    ranked = sorted(score.items(), key=lambda kv: (-kv[1][0], -kv[1][1], len(kv[0])))
    for nm, _ in ranked:
        if nm and (nm not in out):
            out.append(nm)
        if len(out) >= limit:
            break
    return out[:limit]


def guess_model_group_name(model_name):
    s = str(model_name or "")
    s = re.sub(r"\(.*?\)", " ", s)
    s = re.sub(r"(베리\s*뉴|더\s*뉴|올\s*뉴|디\s*올\s*뉴|신형|페이스리프트|뉴)", " ", s)
    toks = [t for t in re.split(r"\s+", s.strip()) if t]
    if not toks:
        return None
    core = [t for t in toks if not re.fullmatch(r"\d+세대", t)]
    if not core:
        core = toks
    maker_noise = {"기아", "현대", "제네시스", "쉐보레", "르노", "BMW", "벤츠", "아우디", "쌍용", "KG모빌리티"}
    core = [t for t in core if t not in maker_noise] or core
    for t in core:
        if re.search(r"[A-Za-z가-힣]", t):
            return t
    return core[0]


def build_badge_group_text(profile):
    disp = (profile.get("displacement") or [None])[0]
    power = profile.get("power")
    if not disp or not power:
        return None
    try:
        cc = int(round(float(disp) * 1000))
    except Exception:
        return None
    fuel_map = {
        "gasoline": "가솔린",
        "diesel": "디젤",
        "lpg": "LPG",
        "hybrid": "하이브리드",
        "ev": "전기",
    }
    ftxt = fuel_map.get(power)
    if not ftxt:
        return None
    return f"{ftxt} {cc}cc"


def _fuel_label_for_badge(text):
    s = str(text or "")
    if ("디젤" in s) or ("경유" in s):
        return "디젤"
    if ("가솔린" in s) or ("휘발유" in s):
        return "가솔린"
    if "LPG" in s.upper():
        return "LPG"
    if "하이브리드" in s:
        return "하이브리드"
    if "전기" in s:
        return "전기"
    return None


def build_badge_group_candidates(trim_text, profile):
    out = []
    cc_group = build_badge_group_text(profile)
    if cc_group:
        out.append(cc_group)
    drive = profile.get("drive")
    fuel_label = _fuel_label_for_badge(trim_text)
    if fuel_label and drive:
        out.append(f"{fuel_label} {drive.upper()}")
    # de-dup preserve order
    return list(dict.fromkeys([x for x in out if x]))


def build_action_badge_candidates(trim_text, anchor_badge, profile, trim_anchors=None):
    out = []
    raw = str(trim_text or "")
    if raw:
        t = raw
        for a in (trim_anchors or []):
            if a:
                t = t.replace(str(a), " ")
        if anchor_badge:
            t = t.replace(str(anchor_badge), " ")
        t = re.sub(r"\s+", " ", t).strip()
        if t:
            # Encar action syntax expects 1_.6 form for decimal displacement.
            t = re.sub(r"(\d)\.(\d)", r"\1_.\2", t)
            out.append(t)
    disp = (profile.get("displacement") or [None])[0]
    drive = profile.get("drive")
    fuel_label = _fuel_label_for_badge(trim_text)
    if disp and fuel_label:
        d = disp.replace(".", "_.")
        if drive:
            out.append(f"{fuel_label} {d} {drive.upper()}")
        out.append(f"{fuel_label} {d}")
    # keep legacy fallback
    if disp:
        out.append(disp.replace(".", "_."))
    if anchor_badge:
        out.append(anchor_badge)
    return list(dict.fromkeys([x for x in out if x]))


def build_manual_badge_candidates(trim_text, anchor_badge=None):
    s = " ".join(str(trim_text or "").strip().split())
    out = []
    if s:
        t = s
        t = re.sub(r"^(?:가솔린|휘발유|디젤|경유|LPG|전기|하이브리드)\s+", "", t, flags=re.IGNORECASE)
        t = re.sub(r"^\d\.\d(?:\s*터보)?\s+", "", t, flags=re.IGNORECASE)
        t = re.sub(r"^(?:2WD|4WD|AWD|FF|FR)\s+", "", t, flags=re.IGNORECASE)
        t = " ".join(t.strip().split())
        if t:
            out.append(t)
    if anchor_badge:
        out.append(str(anchor_badge).strip())
    return list(dict.fromkeys([x for x in out if x]))


def extract_drive_badge_text(trim_text):
    s = " ".join(str(trim_text or "").strip().split())
    if not s:
        return None
    m = re.search(r"(\d\.\d(?:\s*터보)?\s*(?:2WD|4WD|AWD))", s, flags=re.IGNORECASE)
    if not m:
        return None
    return " ".join(m.group(1).split()).upper()


def estimate_optimal_bid(parsed, estimated_sale):
    fixed_cost = 120
    target_margin = 100

    model_key = normalize_text_key(parsed.get("model"))
    trim_key = normalize_text_key(parsed.get("trim"))
    model_tokens = [t for t in model_key.split() if len(t) >= 2 or any(ch.isdigit() for ch in t)]
    trim_tokens = [t for t in trim_key.split() if len(t) >= 2 and not re.fullmatch(r"[\d\.]+", t)]
    trim_anchors = trim_anchor_tokens(parsed.get("trim") or "")
    target_profile = trim_profile(
        parsed.get("trim"),
        parsed.get("fuel"),
        parsed.get("transmission"),
    )

    where_sql = ["source_url LIKE 'heydealer://%'", "(auction_top_bid IS NOT NULL OR auction_my_bid IS NOT NULL)"]
    params = []
    if parsed.get("brand"):
        where_sql.append("brand = ?")
        params.append(parsed.get("brand"))
    if model_tokens:
        where_sql.append("(" + " OR ".join(["model LIKE ?"] * len(model_tokens)) + ")")
        params.extend([f"%{t}%" for t in model_tokens])

    query = f"""
        SELECT model, trim, fuel, transmission, registration_year, mileage_km, auction_top_bid, auction_my_bid,
               auction_participants, evaluator_memo, auction_end_date
        FROM cars
        WHERE {" AND ".join(where_sql)}
        ORDER BY id DESC
        LIMIT 8000
    """
    conn = get_conn()
    try:
        rows = conn.execute(query, tuple(params)).fetchall()
    except Exception:
        rows = []
    finally:
        conn.close()

    target_year = parsed.get("registration_year")
    target_km = parsed.get("mileage_km")
    target_km_lo, target_km_hi = mileage_window(target_km)

    def collect_comps(year_mode):
        comps_local = []
        tier1_local = 0
        base_model_local = 0
        strict_local = 0
        for r in rows:
            row_model = str(r["model"] or "")
            row_trim = str(r["trim"] or "")
            row_fuel = str(r["fuel"] or "")
            row_trans = str(r["transmission"] or "")
            row_model_key = normalize_text_key(row_model)
            row_trim_key = normalize_text_key(row_trim)

            if model_tokens and not any(t in row_model_key for t in model_tokens):
                continue

            model_exact = (model_key and row_model_key == model_key)
            trim_anchor_match = bool(trim_anchors and any(a in row_trim for a in trim_anchors))
            trim_token_match = bool(trim_tokens and any(t in row_trim_key for t in trim_tokens))
            # Use same model + same trim family only.
            if not (model_exact and (trim_anchor_match or trim_token_match)):
                continue
            base_model_local += 1

            row_profile = trim_profile(row_trim, row_fuel, row_trans)
            if not same_trim_profile(target_profile, row_profile):
                continue

            ry = to_int(r["registration_year"])
            if target_year and ry:
                if year_mode == "fixed":
                    if ry != target_year:
                        continue
                elif year_mode == "pm1":
                    if abs(ry - target_year) > 1:
                        continue
                elif year_mode == "pm2":
                    if abs(ry - target_year) > 2:
                        continue

            rkm = to_int(r["mileage_km"])
            if (
                (target_km_lo is not None)
                and (target_km_hi is not None)
                and (rkm is not None)
                and not (target_km_lo <= rkm <= target_km_hi)
            ):
                continue
            strict_local += 1
            tier1_local += 1

            ref_price = r["auction_top_bid"] if r["auction_top_bid"] is not None else r["auction_my_bid"]
            if ref_price is None:
                continue

            adjusted = int(ref_price)
            if target_year and ry:
                adjusted += int(round((target_year - ry) * 18))
            if target_km and rkm:
                adjusted += int(round((rkm - target_km) / 10000.0 * 14))

            memo = str(r["evaluator_memo"] or "")
            if "유사고(프레임)" in memo:
                adjusted -= 60
            elif "유사고(비프레임)" in memo:
                adjusted -= 25

            exchange_cnt = parse_int_from_text(memo, "exchange")
            if exchange_cnt:
                adjusted -= int(exchange_cnt * 6)
            panel_cnt = parse_int_from_text(memo, "painted")
            if panel_cnt:
                adjusted -= int(panel_cnt * 3)

            participants = to_int(r["auction_participants"])
            comps_local.append(
                {
                    "price_ref": int(ref_price),
                    "adjusted_price": max(0, adjusted),
                    "participants": participants,
                    "tier": 1,
                }
            )
        return comps_local, tier1_local, base_model_local, strict_local

    year_mode = "fixed"
    comps, tier1_count, base_model_match_count, strict_filter_count = collect_comps("fixed")
    if target_year and len(comps) < 5:
        best = (comps, tier1_count, base_model_match_count, strict_filter_count, "fixed")
        for ym in ["pm1", "pm2", "any"]:
            c, t1, bm, sf = collect_comps(ym)
            if len(c) > len(best[0]):
                best = (c, t1, bm, sf, ym)
            if len(c) >= 5:
                best = (c, t1, bm, sf, ym)
                break
        comps, tier1_count, base_model_match_count, strict_filter_count, year_mode = best

    adjusted_prices = [c["adjusted_price"] for c in comps]
    participants = [c["participants"] for c in comps if c["participants"] is not None]
    p50 = safe_quantile(adjusted_prices, 0.50)
    p75 = safe_quantile(adjusted_prices, 0.75)
    p90 = safe_quantile(adjusted_prices, 0.90)
    median_participants = safe_median(participants)

    competition_premium = 0
    if median_participants is not None:
        if median_participants >= 40:
            competition_premium = 35
        elif median_participants >= 30:
            competition_premium = 20
        elif median_participants >= 20:
            competition_premium = 10

    comp_count = len(adjusted_prices)
    if comp_count < 5:
        return {
            "recommended_bid_price": None,
            "expected_profit": None,
            "fixed_cost": fixed_cost,
            "target_margin": target_margin,
            "risk_buffer": None,
            "profit_ceiling": None,
            "market_target": None,
            "reason": "제로 경매 동일 트림 표본 부족",
            "stats": {
                "comp_count": comp_count,
                "p50": p50,
                "p75": p75,
                "p90": p90,
                "median_participants": median_participants,
                "competition_premium": competition_premium,
                "match_tier1": tier1_count,
                "match_tier2": 0,
                "match_tier3": 0,
                "base_model_trim_count": base_model_match_count,
                "strict_filter_count": strict_filter_count,
                "year_mode": year_mode,
                "km_window": [target_km_lo, target_km_hi],
            },
        }

    if p50 is not None:
        if p75 is not None and comp_count >= 20:
            base_target = int(round(p50 * 0.6 + p75 * 0.4))
        elif p75 is not None and comp_count >= 8:
            base_target = int(round(p50 * 0.75 + p75 * 0.25))
        else:
            base_target = p50
        market_target = base_target + competition_premium
    elif adjusted_prices:
        market_target = int(round(sum(adjusted_prices) / len(adjusted_prices)))
    else:
        market_target = None

    risk_buffer = 0
    my_acc = parsed.get("my_accident_count")
    if my_acc:
        risk_buffer += int(my_acc * 15)
    if parsed.get("panel_count"):
        risk_buffer += int((parsed.get("panel_count") or 0) * 3)
    if parsed.get("owner_changes"):
        risk_buffer += int((parsed.get("owner_changes") or 0) * 5)
    if parsed.get("simple_exchange"):
        risk_buffer += 10
    if (parsed.get("accident_free") is False) and (my_acc is None):
        risk_buffer += 10

    if estimated_sale is None or market_target is None:
        return {
            "recommended_bid_price": None,
            "expected_profit": None,
            "fixed_cost": fixed_cost,
            "target_margin": target_margin,
            "risk_buffer": risk_buffer,
            "profit_ceiling": None,
            "market_target": market_target,
            "reason": "판매가 또는 동일 트림 경매 근거 부족",
            "stats": {
                "comp_count": len(adjusted_prices),
                "p50": p50,
                "p75": p75,
                "p90": p90,
                "median_participants": median_participants,
                "competition_premium": competition_premium,
                "match_tier1": tier1_count,
                "match_tier2": 0,
                "match_tier3": 0,
                "base_model_trim_count": base_model_match_count,
                "strict_filter_count": strict_filter_count,
                "year_mode": year_mode,
                "km_window": [target_km_lo, target_km_hi],
            },
        }

    profit_ceiling = max(0, int(estimated_sale - fixed_cost - target_margin))
    recommended = max(0, min(profit_ceiling, market_target - risk_buffer))
    expected_profit = int(estimated_sale - recommended - fixed_cost)

    return {
        "recommended_bid_price": recommended,
        "expected_profit": expected_profit,
        "fixed_cost": fixed_cost,
        "target_margin": target_margin,
        "risk_buffer": risk_buffer,
        "profit_ceiling": profit_ceiling,
        "market_target": market_target,
        "stats": {
            "comp_count": len(adjusted_prices),
            "p50": p50,
            "p75": p75,
            "p90": p90,
            "median_participants": median_participants,
            "competition_premium": competition_premium,
            "match_tier1": tier1_count,
            "match_tier2": 0,
            "match_tier3": 0,
            "base_model_trim_count": base_model_match_count,
            "strict_filter_count": strict_filter_count,
            "year_mode": year_mode,
            "km_window": [target_km_lo, target_km_hi],
        },
    }


def fetch_market(parsed):
    brand = parsed.get("brand")
    model = parsed.get("model")
    trim = parsed.get("trim") or ""
    if not brand or not model:
        return None
    cached_market = load_market_cache(parsed)
    if isinstance(cached_market, dict):
        return cached_market
    model_toks = tokenize_model(model)
    target_model_scope = normalize_scope_model_key(model)
    target_year = parsed.get("registration_year")
    target_km = parsed.get("mileage_km")
    target_km_lo, target_km_hi, target_km_base = market_mileage_window(target_km)
    target_fuel = parsed.get("fuel") or ""
    target_fuel_key = fuel_key(target_fuel)
    stage3_km_lo = None if target_km_lo is None else max(0, target_km_lo - 60000)
    stage3_km_hi = None if target_km_hi is None else (target_km_hi + 60000)

    maker_candidates = [brand]
    maker_aliases = {
        "KG모빌리티": ["KG모빌리티", "KG모빌리티(쌍용_)", "쌍용"],
        "쉐보레": ["쉐보레", "쉐보레(GM대우)"],
        "르노": ["르노", "르노코리아"],
    }
    if brand in maker_aliases:
        maker_candidates = maker_aliases[brand]
    car_type = "Y"
    selected_maker = maker_candidates[0] if maker_candidates else None

    rows = []
    scanned_pages = 0
    fetch_errors = 0
    last_fetch_error = None

    def coarse_row_prefilter(it):
        if not ENABLE_COARSE_PREFILTER:
            return True
        mname = str(it.get("Model") or "")
        gname = str(it.get("ModelGroup") or "")
        mg_key = normalize_text_key(mname + " " + gname)
        if model_toks and not any(normalize_text_key(t) in mg_key for t in model_toks):
            return False
        # Keep coarse prefilter loose to avoid over-pruning candidates.
        # Year/fuel strictness is handled later in strict/semi/relaxed stages.
        if target_km and it.get("Mileage") is not None and stage3_km_lo is not None and stage3_km_hi is not None:
            rkm = to_int(it.get("Mileage"))
            if rkm is not None and not (stage3_km_lo <= rkm <= stage3_km_hi):
                return False
        sell_type = str(it.get("SellType") or "")
        if sell_type and ("일반" not in sell_type):
            return False
        return True

    for mk in maker_candidates:
        mk_rows_all = []
        mk_rows = []
        seen_ids = set()
        coarse_hits = 0
        fail_streak = 0
        for start in range(0, 22000, 220):
            query = f"(And.Hidden.N._.(C.CarType.{car_type}._.Manufacturer.{mk}.))"
            list_url = build_encar_list_url(
                query,
                sr=f"|ModifiedDate|{start}|220",
                count="true",
                inav="|Metadata|Sort",
            )
            try:
                data = fetch_json_url(list_url, timeout=12)
            except Exception:
                fetch_errors += 1
                last_fetch_error = "list_fetch_failed"
                fail_streak += 1
                if fail_streak >= 3:
                    break
                continue
            fail_streak = 0
            scanned_pages += 1
            cand = data.get("SearchResults") or []
            if not cand:
                break
            for it in cand:
                ad_id = str(it.get("Id") or "")
                if ad_id and ad_id in seen_ids:
                    continue
                if ad_id:
                    seen_ids.add(ad_id)
                mk_rows_all.append(it)
                mname = str(it.get("Model") or "")
                gname = str(it.get("ModelGroup") or "")
                if model_toks and any(t in mname or t in gname for t in model_toks):
                    coarse_hits += 1
                if not coarse_row_prefilter(it):
                    continue
                mk_rows.append(it)
            if len(cand) < 220:
                break
            # Stop early once enough target-model candidates are collected.
            if coarse_hits >= COARSE_SCAN_TARGET_HITS and start >= COARSE_SCAN_MIN_START:
                break
            if len(mk_rows) >= COARSE_SCAN_MAX_ROWS:
                break
        # If coarse prefilter became too strict, fall back to broad rows.
        if (len(mk_rows) < COARSE_PREFILTER_MIN_ROWS) and mk_rows_all:
            mk_rows = mk_rows_all[:COARSE_SCAN_MAX_ROWS]
        if len(mk_rows) > len(rows):
            rows = mk_rows
            selected_maker = mk

    trim_anchors = trim_anchor_tokens(trim)
    trim_key = normalize_text_key(trim)
    trim_tokens_all = [t for t in trim_key.split() if len(t) >= 2 and not re.fullmatch(r"[\d\.]+", t)]
    trim_tokens_norm = [t for t in trim_tokens_all if t not in TRIM_NOISE_TOKENS]
    if not trim_tokens_norm:
        trim_tokens_norm = trim_tokens_all
    target_profile = trim_profile(trim, parsed.get("fuel"), parsed.get("transmission"))
    gen_toks = re.findall(r"\d+세대", model or "")
    raw_target_options = parsed.get("options") or []
    target_option_keys = detect_option_keys_from_text(raw_target_options)
    target_option_codes = []
    target_option_names = []
    target_option_unmatched = []
    option_catalog_by_code = {}
    parsed_option_catalog = {}
    learn_from_parsed = OPTION_CATALOG_LEARN_SOURCE in {"parsed", "both"}
    learn_from_encar = OPTION_CATALOG_LEARN_SOURCE in {"encar", "both"}
    observed_option_catalog = {}
    option_catalog_saved_count = 0
    option_scope_items = []
    option_scope_total_count = 0
    target_option_scope_items = []
    target_option_scope_selected_count = 0
    anchor_badge = pick_primary_trim_anchor(trim, trim_anchors)
    if trim_anchors and anchor_badge:
        anchor_keys = {normalize_text_key(a) for a in trim_anchors}
        selected_anchor_key = normalize_text_key(anchor_badge)
        reduced = []
        for t in trim_tokens_norm:
            tk = normalize_text_key(t)
            if (tk in anchor_keys) and (tk != selected_anchor_key):
                continue
            reduced.append(t)
        if reduced:
            trim_tokens_norm = reduced
    model_group_name = guess_model_group_name(model)
    model_group_inferred = infer_model_group_name_from_rows(
        rows,
        model_toks=model_toks,
        target_model_scope=target_model_scope,
    )
    if model_group_inferred:
        inferred_scope = normalize_scope_model_key(model_group_inferred)
        if (not target_model_scope) or model_scope_match(target_model_scope, inferred_scope):
            model_group_name = model_group_inferred
    badge_group_candidates = build_badge_group_candidates(trim, target_profile)
    inferred_badge_groups = []
    if (not badge_group_candidates) and rows and anchor_badge:
        inferred_badge_groups = infer_badge_group_candidates_from_rows(
            rows,
            model_toks=model_toks,
            model_group_name=model_group_name,
            anchor_badge=anchor_badge,
            target_year=target_year,
            target_fuel_key=target_fuel_key,
            max_samples=6,
        )
        if inferred_badge_groups:
            badge_group_candidates = inferred_badge_groups
    action_badge_candidates = build_action_badge_candidates(trim, anchor_badge, target_profile, trim_anchors=trim_anchors)
    manual_badge_candidates = build_manual_badge_candidates(trim, anchor_badge=anchor_badge)
    drive_badge_text = extract_drive_badge_text(trim)
    review_model_candidates = build_review_model_candidates(
        model,
        rows=rows,
        target_model_scope=target_model_scope,
        limit=6,
    )
    target_manual_badge_keys = {
        normalize_text_key(x) for x in build_manual_badge_candidates(trim, anchor_badge=None) if normalize_text_key(x)
    }
    option_catalog_scope = build_option_catalog_scope(parsed, target_profile=target_profile, trim_anchors=trim_anchors)
    if learn_from_parsed:
        parsed_option_catalog = build_text_option_catalog(raw_target_options)
        if parsed_option_catalog:
            option_catalog_saved_count += upsert_option_catalog_by_scope(option_catalog_scope, parsed_option_catalog)
    option_catalog_cached = load_option_catalog_by_scope(option_catalog_scope)
    if option_catalog_cached:
        option_catalog_by_code.update(option_catalog_cached)

    # Query mode 1: emulate manual action search as closely as possible.
    direct_rows = []
    direct_scanned_pages = 0
    direct_last_action = None
    direct_model_variants_used = []
    if (
        maker_candidates
        and model_group_name
        and anchor_badge
        and target_year
        and target_km_lo is not None
        and target_km_hi is not None
    ):
        for mk in maker_candidates:
            mk_term = escape_action_term(mk)
            model_group_term = escape_action_term(model_group_name)
            anchor_badge_term = escape_action_term(anchor_badge)

            model_variants_raw = build_direct_model_variants(
                model,
                model_toks,
                model_group_name,
                rows,
                limit=8,
            )
            model_variants = []
            for mv in model_variants_raw:
                mvt = escape_action_term(mv)
                if mvt and (mvt not in model_variants):
                    model_variants.append(mvt)

            badge_group_terms = []
            for bg in badge_group_candidates:
                bgt = escape_action_term(bg)
                if bgt and (bgt not in badge_group_terms):
                    badge_group_terms.append(bgt)

            action_badge_terms = []
            for ab in action_badge_candidates:
                abt = escape_action_term(ab)
                if abt and (abt not in action_badge_terms):
                    action_badge_terms.append(abt)

            manual_badge_terms = []
            for mb in manual_badge_candidates:
                mbt = escape_action_term(mb)
                if mbt and (mbt not in manual_badge_terms):
                    manual_badge_terms.append(mbt)

            action_candidates = []
            for mv in model_variants:
                # Manual-like path first (BadgeGroup + Badge) for high hit rate.
                for bg in badge_group_terms:
                    for mb in manual_badge_terms:
                        if not (bg and mb):
                            continue
                        action_candidates.append(
                            f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                            f"(C.ModelGroup.{model_group_term}._.(C.Model.{mv}._."
                            f"(C.BadgeGroup.{bg}._.Badge.{mb}.)))))_."
                            f"Year.range({target_year}01..{target_year}12)._.Mileage.range({target_km_lo}..{target_km_hi}).)"
                        )

                # Primary template: follow Encar web action structure as closely as possible.
                for bg in badge_group_terms:
                    for ab in action_badge_terms:
                        if not (bg and ab and anchor_badge_term):
                            continue
                        action_candidates.append(
                            f"(And.Hidden.N._.(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                            f"(C.ModelGroup.{model_group_term}._.(C.Model.{mv}._."
                            f"(C.BadgeGroup.{bg}._.(C.Badge.{ab}._.BadgeDetail.{anchor_badge_term}.))))))_."
                            f"Year.range({target_year}01..{target_year}12)._.Mileage.range({target_km_lo}..{target_km_hi}).)"
                        )
                        action_candidates.append(
                            f"(And.Hidden.N._.Year.range({target_year}00..{target_year}99)._."
                            f"Mileage.range({target_km_lo}..{target_km_hi})._."
                            f"(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                            f"(C.ModelGroup.{model_group_term}._.(C.Model.{mv}._."
                            f"(C.BadgeGroup.{bg}._.(C.Badge.{ab}._.BadgeDetail.{anchor_badge_term}.)))))))"
                        )
                for ab in action_badge_terms:
                    if ab and anchor_badge_term and (ab != anchor_badge_term):
                        action_candidates.append(
                            f"(And.Hidden.N._.Year.range({target_year}00..{target_year}99)._."
                            f"Mileage.range({target_km_lo}..{target_km_hi})._."
                            f"(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                            f"(C.ModelGroup.{model_group_term}._.(C.Model.{mv}._."
                            f"(C.Badge.{ab}._.BadgeDetail.{anchor_badge_term}.))))))"
                        )
                action_candidates.append(
                    f"(And.Hidden.N._.Year.range({target_year}00..{target_year}99)._."
                    f"Mileage.range({target_km_lo}..{target_km_hi})._."
                    f"(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                    f"(C.ModelGroup.{model_group_term}._.(C.Model.{mv}._."
                    f"(C.Badge.{anchor_badge_term}.))))))"
                )

            # Fallback: some Encar model names reject exact path queries.
            for ab in action_badge_terms:
                if ab and anchor_badge_term and (ab != anchor_badge_term):
                    action_candidates.append(
                        f"(And.Hidden.N._.Year.range({target_year}00..{target_year}99)._."
                        f"Mileage.range({target_km_lo}..{target_km_hi})._."
                        f"(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                        f"(C.ModelGroup.{model_group_term}._.(C.Badge.{ab}._.BadgeDetail.{anchor_badge_term}.))))))"
                    )
            action_candidates.append(
                f"(And.Hidden.N._.Year.range({target_year}00..{target_year}99)._."
                f"Mileage.range({target_km_lo}..{target_km_hi})._."
                f"(C.CarType.{car_type}._.(C.Manufacturer.{mk_term}._."
                f"(C.ModelGroup.{model_group_term}._.Badge.{anchor_badge_term}.))))"
            )

            action_candidates = list(dict.fromkeys([a for a in action_candidates if a]))
            best_mk_rows = []
            best_mk_action = None
            best_mk_score = (-1, -1, -1)  # (enough_samples, specificity, row_count)
            for action in action_candidates[:DIRECT_ACTION_MAX_CANDIDATES]:
                mk_rows = []
                seen_ids = set()
                ok = True
                invalid_query = False
                fail_streak = 0
                for start in range(0, DIRECT_ACTION_MAX_START, 200):
                    list_url = build_encar_list_url(
                        action,
                        sr=f"|Mileage|{start}|200",
                        count="true",
                        inav="|Metadata|Sort",
                    )
                    try:
                        data = fetch_json_url(list_url, timeout=12)
                    except urllib.error.HTTPError as e:
                        ok = False
                        if e.code == 400:
                            invalid_query = True
                            last_fetch_error = "direct_query_invalid"
                        else:
                            fetch_errors += 1
                            last_fetch_error = "direct_query_fetch_failed"
                            fail_streak += 1
                            if fail_streak >= 3:
                                break
                            continue
                        break
                    except Exception:
                        fetch_errors += 1
                        last_fetch_error = "direct_query_fetch_failed"
                        fail_streak += 1
                        if fail_streak >= 3:
                            ok = False
                            break
                        continue
                    fail_streak = 0
                    direct_scanned_pages += 1
                    cand = data.get("SearchResults") or []
                    if not cand:
                        break
                    for it in cand:
                        ad_id = str(it.get("Id") or "")
                        if ad_id and ad_id in seen_ids:
                            continue
                        if ad_id:
                            seen_ids.add(ad_id)
                        mk_rows.append(it)
                    if len(cand) < 200:
                        break
                    if len(mk_rows) >= COARSE_SCAN_MAX_ROWS:
                        break
                if ok and mk_rows:
                    row_count = len(mk_rows)
                    enough = 1 if row_count >= MIN_MARKET_COMPS else 0
                    spec = action_specificity_score(action)
                    score = (enough, spec, row_count)
                    if score > best_mk_score:
                        best_mk_score = score
                        best_mk_rows = mk_rows
                        best_mk_action = action
                    # Early stop only for highly specific query with enough samples.
                    if enough and spec >= 4:
                        break
                if invalid_query:
                    continue
            if len(best_mk_rows) > len(direct_rows):
                direct_rows = best_mk_rows
                direct_last_action = best_mk_action
                direct_model_variants_used = model_variants_raw

    if direct_rows:
        merged = []
        seen_ids = set()
        for it in direct_rows + rows:
            ad_id = str(it.get("Id") or "")
            if ad_id and ad_id in seen_ids:
                continue
            if ad_id:
                seen_ids.add(ad_id)
            merged.append(it)
        rows = merged
        scanned_pages += direct_scanned_pages
    if not rows:
        # Safety fallback: when prefilter/direct query produced no rows, do broad maker scan once.
        for mk in maker_candidates:
            seen_ids = set()
            for start in range(0, 6600, 220):
                query = f"(And.Hidden.N._.(C.CarType.{car_type}._.Manufacturer.{mk}.))"
                list_url = build_encar_list_url(
                    query,
                    sr=f"|ModifiedDate|{start}|220",
                    count="true",
                    inav="|Metadata|Sort",
                )
                try:
                    data = fetch_json_url(list_url, timeout=12)
                except Exception:
                    fetch_errors += 1
                    last_fetch_error = "fallback_scan_failed"
                    break
                scanned_pages += 1
                cand = data.get("SearchResults") or []
                if not cand:
                    break
                for it in cand:
                    ad_id = str(it.get("Id") or "")
                    if ad_id and ad_id in seen_ids:
                        continue
                    if ad_id:
                        seen_ids.add(ad_id)
                    rows.append(it)
                if len(rows) >= (COARSE_SCAN_MAX_ROWS * 2):
                    break
                if len(cand) < 220:
                    break
            if rows:
                break
    direct_applied = bool(direct_rows)
    direct_web_url = build_encar_web_search_url(direct_last_action, sort="Mileage", page=1, limit=20) if direct_last_action else None
    review_km_hi = review_mileage_upper_km(target_km, fallback_hi=target_km_hi)
    review_last_action = choose_review_action_query(
        direct_last_action,
        review_km_hi=review_km_hi,
        maker_candidates=maker_candidates,
        preferred_maker=selected_maker,
        model_group_name=model_group_name,
        target_year=target_year,
        target_model_scope=target_model_scope,
        target_fuel_key=target_fuel_key,
        model_candidates=review_model_candidates,
        model_name=model,
        badge_group_candidates=badge_group_candidates,
        drive_badge_text=drive_badge_text,
        manual_badge_candidates=manual_badge_candidates,
        anchor_badge=anchor_badge,
        car_type=car_type,
    )
    review_web_url = (
        build_encar_web_search_url(review_last_action, sort="Mileage", page=1, limit=20)
        if review_last_action
        else None
    )
    review_broad_action = build_review_action_query(
        None,
        review_km_hi=review_km_hi,
        maker=selected_maker,
        model_group_name=None,
        target_year=target_year,
        model_name=None,
        car_type=car_type,
    )
    review_broad_web_url = (
        build_encar_web_search_url(review_broad_action, sort="Mileage", page=1, limit=20)
        if review_broad_action
        else None
    )
    review_manual_url = "https://www.encar.com/dc/dc_carsearchlist.do?carType=kor&searchType=model&TG.R=A"

    def strict_match(r):
        mname = str(r.get("Model") or "")
        gname = str(r.get("ModelGroup") or "")
        mg_key = normalize_text_key(mname + " " + gname)
        row_model_scope = normalize_scope_model_key(mname)
        sell_type = str(r.get("SellType") or "")

        if sell_type and ("일반" not in sell_type):
            return False
        if target_model_scope and row_model_scope and (not model_scope_match(target_model_scope, row_model_scope)):
            return False
        if model_toks and not any(normalize_text_key(t) in mg_key for t in model_toks):
            return False
        if gen_toks and not any(gt in mname or gt in gname for gt in gen_toks):
            return False
        if target_year:
            yr = extract_year(r.get("Year"))
            if yr is None or yr != target_year:
                return False
        if target_km and r.get("Mileage") is not None:
            rkm = float(r.get("Mileage"))
            if (target_km_lo is not None) and (target_km_hi is not None):
                if not (target_km_lo <= rkm <= target_km_hi):
                    return False
            else:
                if not (target_km * 0.65 <= rkm <= target_km * 1.45):
                    return False
        row_fuel = str(r.get("FuelType") or "")
        if target_fuel:
            rfk = fuel_key(row_fuel)
            if target_fuel_key and rfk and (target_fuel_key != rfk):
                return False
        return True

    target_comp_count = 20
    stage2_km_lo = None if target_km_lo is None else max(0, target_km_lo - 30000)
    stage2_km_hi = None if target_km_hi is None else (target_km_hi + 30000)
    stage3_km_lo = None if target_km_lo is None else max(0, target_km_lo - 60000)
    stage3_km_hi = None if target_km_hi is None else (target_km_hi + 60000)
    strict = [r for r in rows if strict_match(r)]
    work = strict
    semi = []
    relaxed = []
    year_mode = "fixed"

    if len(work) < target_comp_count:
        # Stage 2: widen by +30,000km on both sides from base window.
        semi_km_lo = stage2_km_lo
        semi_km_hi = stage2_km_hi

        def semi_match(r):
            mname = str(r.get("Model") or "")
            gname = str(r.get("ModelGroup") or "")
            mg_key = normalize_text_key(mname + " " + gname)
            row_model_scope = normalize_scope_model_key(mname)
            sell_type = str(r.get("SellType") or "")

            if sell_type and ("일반" not in sell_type):
                return False
            if target_model_scope and row_model_scope and (not model_scope_match(target_model_scope, row_model_scope)):
                return False
            if model_toks and not any(normalize_text_key(t) in mg_key for t in model_toks):
                return False
            if target_year:
                yr = extract_year(r.get("Year"))
                if yr is None or yr != target_year:
                    return False
            if target_km and r.get("Mileage") is not None:
                rkm = float(r.get("Mileage"))
                if (semi_km_lo is not None) and (semi_km_hi is not None):
                    if not (semi_km_lo <= rkm <= semi_km_hi):
                        return False
            row_fuel = str(r.get("FuelType") or "")
            if target_fuel:
                rfk = fuel_key(row_fuel)
                if target_fuel_key and rfk and (target_fuel_key != rfk):
                    return False
            return True

        semi = [r for r in rows if semi_match(r)]
        if len(semi) > len(work):
            work = semi
            year_mode = "fixed_km_stage2"

    if len(work) < target_comp_count:
        # Stage 3: widen by another +30,000km on both sides.
        relax_km_lo = stage3_km_lo
        relax_km_hi = stage3_km_hi

        def relaxed_match(r):
            mname = str(r.get("Model") or "")
            gname = str(r.get("ModelGroup") or "")
            mg_key = normalize_text_key(mname + " " + gname)
            row_model_scope = normalize_scope_model_key(mname)
            sell_type = str(r.get("SellType") or "")

            if sell_type and ("일반" not in sell_type):
                return False
            if target_model_scope and row_model_scope and (not model_scope_match(target_model_scope, row_model_scope)):
                return False
            if model_toks and not any(normalize_text_key(t) in mg_key for t in model_toks):
                return False
            if target_year:
                yr = extract_year(r.get("Year"))
                if yr is None or yr != target_year:
                    return False
            if target_km and r.get("Mileage") is not None:
                rkm = float(r.get("Mileage"))
                if (relax_km_lo is not None) and (relax_km_hi is not None):
                    if not (relax_km_lo <= rkm <= relax_km_hi):
                        return False
            row_fuel = str(r.get("FuelType") or "")
            if target_fuel:
                rfk = fuel_key(row_fuel)
                if target_fuel_key and rfk and (target_fuel_key != rfk):
                    return False
            return True

        relaxed = [r for r in rows if relaxed_match(r)]
        if len(relaxed) > len(work):
            work = relaxed
            year_mode = "fixed_km_stage3"

    def score(r):
        s = 0.0
        mname = str(r.get("Model") or "")
        gname = str(r.get("ModelGroup") or "")
        badge = (str(r.get("Badge") or "") + " " + str(r.get("BadgeDetail") or "")).strip()
        row_model_scope = normalize_scope_model_key(mname)
        if target_model_scope and row_model_scope and (target_model_scope == row_model_scope):
            s += 2.2
        elif target_model_scope and row_model_scope and model_scope_match(target_model_scope, row_model_scope):
            s += 1.4
        for t in model_toks:
            if t in mname or t in gname:
                s += 2.0
        for t in trim_tokens_norm:
            if t in normalize_text_key(badge):
                s += 1.2
        if target_year and r.get("Year"):
            yr = extract_year(r.get("Year"))
            if yr is not None:
                s += max(0, 1.2 - abs(target_year - yr) * 0.5)
        if target_km and r.get("Mileage") is not None:
            s += max(0, 1.0 - abs(target_km - float(r.get("Mileage"))) / 50000.0)
        if target_fuel and fuel_key(target_fuel) == fuel_key(str(r.get("FuelType") or "")):
            s += 0.7
        return s

    excluded_contract_count = 0
    excluded_price_count = 0
    excluded_outlier_count = 0
    target_option_codes, target_option_names, target_option_unmatched = map_target_options_to_codes(
        raw_target_options,
        option_catalog_by_code,
    )
    scope_catalog_by_code = dict(option_catalog_by_code)
    for cd, nm in (parsed_option_catalog or {}).items():
        if cd and nm and (cd not in scope_catalog_by_code):
            scope_catalog_by_code[cd] = nm
    for cd, nm in (observed_option_catalog or {}).items():
        if cd and nm and (cd not in scope_catalog_by_code):
            scope_catalog_by_code[cd] = nm
    for cd in target_option_codes:
        if (cd not in scope_catalog_by_code) and (cd in option_catalog_by_code):
            scope_catalog_by_code[cd] = option_catalog_by_code[cd]
    option_scope_items = build_option_scope_items(
        scope_catalog_by_code,
        selected_codes=target_option_codes,
        selected_names=raw_target_options,
    )
    option_scope_total_count = len(option_scope_items)
    target_option_scope_selected_count = len([it for it in option_scope_items if it.get("selected")])
    target_option_scope_items = [
        {
            "name": it.get("name"),
            "selected": bool(it.get("selected")),
        }
        for it in option_scope_items
    ]

    if not work:
        return finalize_market_result(parsed, {
            "count": 0,
            "min": None,
            "median": None,
            "avg": None,
            "max": None,
            "debug": {
                "raw_count": len(rows),
                "strict_count": len(strict),
                "semi_count": len(semi),
                "relaxed_count": len(relaxed),
                "sample_ids": [],
                "api_calls": 0,
                "dedup_vehicle_count": 0,
                "code_effects": {},
                "scanned_pages": scanned_pages,
                "direct_scanned_pages": direct_scanned_pages,
                "direct_candidate_count": len(direct_rows),
                "direct_applied": direct_applied,
                "direct_model_variants_used": direct_model_variants_used[:8],
                "direct_last_action": direct_last_action,
                "direct_web_url": direct_web_url,
                "review_last_action": review_last_action,
                "review_web_url": review_web_url,
                "review_broad_action": review_broad_action,
                "review_broad_web_url": review_broad_web_url,
                "review_manual_url": review_manual_url,
                "review_km_hi": review_km_hi,
                "review_selected_maker": selected_maker,
                "review_model_group_name": model_group_name,
                "review_drive_badge_text": drive_badge_text,
                "review_model_candidates": review_model_candidates,
                "review_fuel_key": target_fuel_key,
                "anchor_badge": anchor_badge,
                "badge_group_candidates": badge_group_candidates[:8],
                "inferred_badge_groups": inferred_badge_groups[:8],
                "km_anchor": target_km_base,
                "km_window": [target_km_lo, target_km_hi],
                "year_mode": year_mode,
                "detailed_fetch_enabled": ENABLE_ENCAR_DETAILED_FETCH,
                "logic_version": LOGIC_VERSION,
                "fetch_errors": fetch_errors,
                "last_fetch_error": last_fetch_error,
                "excluded_contract_count": excluded_contract_count,
                "excluded_price_count": excluded_price_count,
                "excluded_outlier_count": excluded_outlier_count,
                "target_option_keys": sorted(target_option_keys),
                "target_option_codes": sorted(target_option_codes),
                "target_option_names": target_option_names,
                "target_option_unmatched": target_option_unmatched,
                "option_catalog_count": len(option_catalog_by_code),
                "option_catalog_cached_count": len(option_catalog_cached),
                "option_catalog_saved_count": option_catalog_saved_count,
                "option_catalog_learn_source": OPTION_CATALOG_LEARN_SOURCE,
                "option_scope_total_count": option_scope_total_count,
                "target_option_scope_selected_count": target_option_scope_selected_count,
                "target_option_scope_items": target_option_scope_items,
                "reason": "엔카 동일 트림 표본 없음",
            },
            "comps": [],
        })

    scored = sorted(((score(r), r) for r in work), key=lambda x: x[0], reverse=True)
    # Keep broad candidate set like manual search; do not narrow to top-score only.
    top_rows = [r for _, r in scored[:TOP_CANDIDATE_POOL]]

    exact_comps = []
    similar_comps = []
    api_calls = 0
    seen_vehicle = set()
    choice_catalog_cache = {}
    detailed_fetch_rows = 0
    detailed_fetch_early_stop = False

    if ENABLE_ENCAR_DETAILED_FETCH:
        for r in top_rows:
            if detailed_fetch_rows >= DETAILED_FETCH_MAX_ROWS:
                detailed_fetch_early_stop = True
                break
            row_trim_text = (str(r.get("Badge") or "") + " " + str(r.get("BadgeDetail") or "")).strip()
            row_profile_hint = trim_profile(
                row_trim_text,
                r.get("FuelType"),
                r.get("Transmission"),
            )
            if not same_trim_profile(target_profile, row_profile_hint):
                continue
            ad_id = str(r.get("Id") or "")
            if not ad_id:
                continue
            detailed_fetch_rows += 1
            try:
                detail = fetch_json_url_retry(f"https://api.encar.com/v1/readside/vehicle/{ad_id}", timeout=8, attempts=2)
                api_calls += 1
            except Exception:
                continue

            cat = detail.get("category") or {}
            detail_trim_text = f"{cat.get('gradeName') or ''} {cat.get('gradeDetailName') or ''}".strip()
            detail_key = normalize_text_key(detail_trim_text)
            anchor_ok = (not trim_anchors) or any(t in detail_trim_text for t in trim_anchors)
            token_ok = (not trim_tokens_norm) or all(t in detail_key for t in trim_tokens_norm)
            trim_exact_match = bool(anchor_ok and token_ok)
            detail_profile = trim_profile(
                detail_trim_text,
                r.get("FuelType"),
                r.get("Transmission"),
            )
            if not same_trim_profile(target_profile, detail_profile):
                continue

            adv = detail.get("advertisement") or {}
            sales_status = str(adv.get("salesStatus") or "").strip().upper()
            status_text = " ".join([
                str(r.get("PriceText") or ""),
                str(r.get("PriceTitle") or ""),
                str(r.get("PriceView") or ""),
                str(r.get("Title") or ""),
                str(adv.get("oneLineText") or ""),
            ]).replace(" ", "")
            if sales_status == "CONTRACT" or ("계약중" in status_text):
                excluded_contract_count += 1
                continue

            detail_price = to_int(adv.get("price"))
            row_price = to_int(r.get("Price"))
            price = detail_price if detail_price is not None else row_price
            if is_abnormal_market_price(price):
                excluded_price_count += 1
                continue

            vehicle_id = str(detail.get("vehicleId") or ad_id)
            if vehicle_id in seen_vehicle:
                continue
            seen_vehicle.add(vehicle_id)

            row_token_set = set(detail_key.split())
            target_token_set = set(trim_tokens_norm)
            if target_token_set:
                overlap = len(row_token_set & target_token_set)
                union = len(row_token_set | target_token_set) or 1
                trim_similarity = overlap / union
            else:
                trim_similarity = 0.0
            if trim_exact_match:
                trim_similarity = 1.0
            detail_manual_candidates = build_manual_badge_candidates(detail_trim_text, anchor_badge=None)
            detail_manual_key = normalize_text_key(detail_manual_candidates[0]) if detail_manual_candidates else ""
            trim_name_exact = bool(target_manual_badge_keys and detail_manual_key in target_manual_badge_keys)
            if not target_manual_badge_keys:
                trim_name_exact = trim_exact_match

            selected_codes = [str(x) for x in ((detail.get("options") or {}).get("choice") or [])]
            if vehicle_id in choice_catalog_cache:
                vehicle_choice_catalog = choice_catalog_cache.get(vehicle_id) or {}
            else:
                vehicle_choice_catalog = {}
                if trim_name_exact:
                    try:
                        choice_rows = fetch_json_url_retry(
                            f"https://api.encar.com/v1/readside/vehicles/car/{vehicle_id}/options/choice",
                            timeout=8,
                            attempts=2,
                        )
                        api_calls += 1
                        if isinstance(choice_rows, list):
                            for it in choice_rows:
                                cd = str((it or {}).get("optionCd") or "").strip()
                                nm = str((it or {}).get("optionName") or "").strip()
                                if cd and nm:
                                    vehicle_choice_catalog[cd] = nm
                    except Exception:
                        pass
                choice_catalog_cache[vehicle_id] = vehicle_choice_catalog

            selected_option_name_map = {}
            for cd in selected_codes:
                nm = vehicle_choice_catalog.get(cd)
                if nm:
                    selected_option_name_map[cd] = nm
            selected_option_names = [selected_option_name_map[cd] for cd in selected_codes if cd in selected_option_name_map]

            option_keys_from_codes = detect_option_keys_from_codes(selected_codes)
            option_keys_from_text = detect_option_keys_from_text(
                [((detail.get("contents") or {}).get("text") or "")] + selected_option_names
            )
            row_option_keys = sorted(option_keys_from_codes | option_keys_from_text)

            my_acc = None
            accident_cnt = None
            other_acc = None
            panel_cnt = None
            simple_repair = None
            diagn_accident = None
            try:
                rec = fetch_json_url_retry(
                    f"https://api.encar.com/v1/readside/record/vehicle/{vehicle_id}/summary",
                    timeout=8,
                    attempts=2,
                )
                api_calls += 1
                my_acc = to_int(rec.get("myAccidentCnt"))
                accident_cnt = to_int(rec.get("accidentCnt"))
                other_acc = to_int(rec.get("otherAccidentCnt"))
            except Exception:
                pass

            try:
                insp = fetch_json_url_retry(
                    f"https://api.encar.com/v1/readside/inspection/vehicle/{vehicle_id}/summary",
                    timeout=8,
                    attempts=2,
                )
                api_calls += 1
                panel_cnt = 0
                for it in insp.get("outerSummarys") or []:
                    panel_cnt += to_int(it.get("count") or 0) or 0
            except Exception:
                pass

            try:
                insp_detail = fetch_json_url_retry(
                    f"https://api.encar.com/v1/readside/inspection/vehicle/{vehicle_id}",
                    timeout=8,
                    attempts=2,
                )
                api_calls += 1
                master = insp_detail.get("master") or {}
                simple_repair = master.get("simpleRepair")
                if simple_repair is None:
                    simple_repair = master.get("simplerepair")
                diagn_accident = master.get("accdient")
                if diagn_accident is None:
                    diagn_accident = master.get("accident")
            except Exception:
                pass

            # Optional: Encar-derived learning is disabled by default (parsed-source learning).
            if learn_from_encar and trim_name_exact:
                for cd, nm in selected_option_name_map.items():
                    if cd and nm:
                        if cd not in option_catalog_by_code:
                            option_catalog_by_code[cd] = nm
                        observed_option_catalog[cd] = nm

            comp_item = {
                "ad_id": ad_id,
                "vehicle_id": vehicle_id,
                "detail_url": f"https://fem.encar.com/cars/detail/{ad_id}",
                "analysis_level": "detailed",
                "price": price,
                "year": extract_year(r.get("Year")),
                "mileage": to_int(r.get("Mileage")),
                "fuel": str(r.get("FuelType") or ""),
                "trim_detail": detail_trim_text,
                "codes": selected_codes,
                "selected_option_names": selected_option_names,
                "option_keys": row_option_keys,
                "option_matched_keys": [],
                "option_matched_codes": [],
                "option_matched_names": [],
                "option_match_count": 0,
                "option_target_count": 0,
                "my_accident_count": my_acc,
                "other_accident_count": other_acc,
                "accident_count": accident_cnt,
                "diagnosis_accident": diagn_accident,
                "simple_repair": simple_repair,
                "panel_count": panel_cnt,
                "trim_exact_match": trim_exact_match,
                "trim_name_exact": trim_name_exact,
                "trim_similarity": round(trim_similarity, 4),
            }
            if trim_exact_match:
                exact_comps.append(comp_item)
            else:
                similar_comps.append(comp_item)

            if (
                (len(exact_comps) + len(similar_comps) >= DETAILED_FETCH_POOL_TARGET)
                and (len(exact_comps) >= MIN_MARKET_COMPS)
            ):
                detailed_fetch_early_stop = True
                break
    else:
        for r in top_rows:
            row_trim_text = (str(r.get("Badge") or "") + " " + str(r.get("BadgeDetail") or "")).strip()
            row_profile_hint = trim_profile(
                row_trim_text,
                r.get("FuelType"),
                r.get("Transmission"),
            )
            if not same_trim_profile(target_profile, row_profile_hint):
                continue
            ad_id = str(r.get("Id") or "").strip()
            if not ad_id:
                continue
            status_text = " ".join([
                str(r.get("PriceText") or ""),
                str(r.get("PriceTitle") or ""),
                str(r.get("PriceView") or ""),
                str(r.get("Title") or ""),
            ]).replace(" ", "")
            if "계약중" in status_text:
                excluded_contract_count += 1
                continue
            price = to_int(r.get("Price"))
            if is_abnormal_market_price(price):
                excluded_price_count += 1
                continue
            if ad_id in seen_vehicle:
                continue
            seen_vehicle.add(ad_id)

            row_key = normalize_text_key(row_trim_text)
            anchor_ok = (not trim_anchors) or any(t in row_trim_text for t in trim_anchors)
            token_ok = (not trim_tokens_norm) or all(t in row_key for t in trim_tokens_norm)
            trim_exact_match = bool(anchor_ok and token_ok)
            row_token_set = set(row_key.split())
            target_token_set = set(trim_tokens_norm)
            if target_token_set:
                overlap = len(row_token_set & target_token_set)
                union = len(row_token_set | target_token_set) or 1
                trim_similarity = overlap / union
            else:
                trim_similarity = 0.0
            if trim_exact_match:
                trim_similarity = 1.0
            row_manual_candidates = build_manual_badge_candidates(row_trim_text, anchor_badge=None)
            row_manual_key = normalize_text_key(row_manual_candidates[0]) if row_manual_candidates else ""
            trim_name_exact = bool(target_manual_badge_keys and row_manual_key in target_manual_badge_keys)
            if not target_manual_badge_keys:
                trim_name_exact = trim_exact_match

            comp_item = {
                "ad_id": ad_id,
                "vehicle_id": ad_id,
                "detail_url": f"https://fem.encar.com/cars/detail/{ad_id}",
                "analysis_level": "lite",
                "price": price,
                "year": extract_year(r.get("Year")),
                "mileage": to_int(r.get("Mileage")),
                "fuel": str(r.get("FuelType") or ""),
                "trim_detail": row_trim_text,
                "codes": [],
                "selected_option_names": [],
                "option_keys": sorted(detect_option_keys_from_text([row_trim_text])),
                "option_matched_keys": [],
                "option_matched_codes": [],
                "option_matched_names": [],
                "option_match_count": 0,
                "option_target_count": 0,
                "my_accident_count": None,
                "other_accident_count": None,
                "accident_count": None,
                "diagnosis_accident": None,
                "simple_repair": None,
                "panel_count": None,
                "trim_exact_match": trim_exact_match,
                "trim_name_exact": trim_name_exact,
                "trim_similarity": round(trim_similarity, 4),
            }
            if trim_exact_match:
                exact_comps.append(comp_item)
            else:
                similar_comps.append(comp_item)

    if ENABLE_ENCAR_DETAILED_FETCH and learn_from_encar and observed_option_catalog:
        option_catalog_saved_count += upsert_option_catalog_by_scope(option_catalog_scope, observed_option_catalog)

    # Option similarity is informative score only; do not hard-filter rows.
    comps = list(exact_comps)
    trim_fill_mode = "exact_only"
    tkm = to_int(target_km) or 0
    similar_sorted = sorted(
        similar_comps,
        key=lambda c: (
            c.get("trim_similarity") or 0.0,
            -abs((to_int(c.get("mileage")) or tkm) - tkm),
            to_int(c.get("year")) or 0,
        ),
        reverse=True,
    )
    if len([c["price"] for c in exact_comps if c.get("price") is not None]) < MIN_MARKET_COMPS and similar_sorted:
        needed = max(0, MIN_MARKET_COMPS - len(comps))
        comps.extend(similar_sorted[:needed])
        if needed > 0:
            trim_fill_mode = "exact_plus_similar_fill"

    # Show only fully analyzed rows in comparison table.
    if len(comps) < DETAILED_COMPARE_LIMIT and similar_sorted:
        have = {str(c.get("ad_id") or "") for c in comps}
        for c in similar_sorted:
            ad_id = str(c.get("ad_id") or "")
            if ad_id in have:
                continue
            comps.append(c)
            have.add(ad_id)
            if len(comps) >= DETAILED_COMPARE_LIMIT:
                break
        if len(comps) > len(exact_comps):
            trim_fill_mode = "exact_plus_similar_display_fill"

    if comps:
        base_prices = [c["price"] for c in comps if c.get("price") is not None]
        if base_prices:
            base_avg = sum(base_prices) / len(base_prices)
            filtered = []
            for c in comps:
                p = c.get("price")
                if p is None:
                    continue
                # Drop listings too far from cohort average (>= 2000만원 gap).
                if abs(p - base_avg) >= 2000:
                    excluded_outlier_count += 1
                    continue
                filtered.append(c)
            comps = filtered

    target_option_codes, target_option_names, target_option_unmatched = map_target_options_to_codes(
        raw_target_options,
        option_catalog_by_code,
    )
    # UI/검토용 옵션 분모는 "해당 모델+트림에서 가능한 옵션 전체"를 우선 사용한다.
    # (타겟 입력옵션 수로 축소되지 않도록 유지)
    scope_catalog_by_code = dict(option_catalog_by_code)
    for cd, nm in (parsed_option_catalog or {}).items():
        if cd and nm and (cd not in scope_catalog_by_code):
            scope_catalog_by_code[cd] = nm
    for cd, nm in (observed_option_catalog or {}).items():
        if cd and nm and (cd not in scope_catalog_by_code):
            scope_catalog_by_code[cd] = nm
    for cd in target_option_codes:
        if (cd not in scope_catalog_by_code) and (cd in option_catalog_by_code):
            scope_catalog_by_code[cd] = option_catalog_by_code[cd]

    option_scope_items = build_option_scope_items(
        scope_catalog_by_code,
        selected_codes=target_option_codes,
        selected_names=raw_target_options,
    )
    option_scope_mode = "catalog_code"
    if not option_scope_items and raw_target_options:
        # Fallback: when code mapping is unavailable, use parsed option text as denominator.
        option_scope_items = build_option_scope_items_from_text(raw_target_options)
        option_scope_mode = "target_text_fallback"
    option_scope_total_count = len(option_scope_items)
    target_option_scope_selected_count = len([it for it in option_scope_items if it.get("selected")])
    target_option_scope_items = [
        {
            "name": it.get("name"),
            "selected": bool(it.get("selected")),
        }
        for it in option_scope_items
    ]

    for c in comps:
        row_codes = [str(x) for x in (c.get("codes") or [])]
        row_code_set = set(row_codes)
        scope_selected_names = []
        scope_missing_names = []
        for it in option_scope_items:
            codes = set(it.get("codes") or [])
            matched = False
            if codes:
                matched = bool(row_code_set.intersection(codes))
            else:
                matched = comp_matches_option_text(c, it.get("name"))
            if matched:
                scope_selected_names.append(it.get("name"))
            else:
                scope_missing_names.append(it.get("name"))
        c["option_scope_selected_count"] = len(scope_selected_names)
        c["option_scope_total_count"] = option_scope_total_count
        c["option_scope_selected_names"] = scope_selected_names
        c["option_scope_missing_names"] = scope_missing_names
        if target_option_codes:
            matched_codes = [cd for cd in target_option_codes if cd in row_codes]
            matched_names = [option_catalog_by_code.get(cd) for cd in matched_codes if option_catalog_by_code.get(cd)]
            c["option_matched_codes"] = matched_codes
            c["option_matched_names"] = matched_names
            c["option_matched_keys"] = matched_names
            c["option_match_count"] = len(matched_codes)
            c["option_target_count"] = len(target_option_codes)
        else:
            matched_option_keys = sorted(set(target_option_keys) & set(c.get("option_keys") or []))
            c["option_matched_keys"] = matched_option_keys
            c["option_match_count"] = len(matched_option_keys)
            c["option_target_count"] = len(target_option_keys)

    def comp_price_stage_weight(comp_item):
        # Base-window comps carry full weight; widened-window comps are discounted.
        kmv = to_int(comp_item.get("mileage"))
        if (kmv is None) or (target_km_lo is None) or (target_km_hi is None):
            return 0.85
        if target_km_lo <= kmv <= target_km_hi:
            return 1.0
        if (stage2_km_lo is not None) and (stage2_km_hi is not None) and (stage2_km_lo <= kmv <= stage2_km_hi):
            return 0.85
        return 0.70

    price_weight_pairs = []
    stage_weight_counts = {"base": 0, "stage2": 0, "stage3": 0}
    for c in comps:
        p = to_int(c.get("price"))
        if p is None:
            continue
        w = comp_price_stage_weight(c)
        if w >= 0.999:
            stage_weight_counts["base"] += 1
        elif w >= 0.80:
            stage_weight_counts["stage2"] += 1
        else:
            stage_weight_counts["stage3"] += 1
        price_weight_pairs.append((p, w))

    prices = sorted([p for p, _ in price_weight_pairs])
    weight_sum = sum(w for _, w in price_weight_pairs)
    weighted_avg = int(round(sum(p * w for p, w in price_weight_pairs) / weight_sum)) if weight_sum > 0 else None
    if len(prices) < MIN_MARKET_COMPS:
        partial_count = len(prices)
        partial_min = prices[0] if prices else None
        partial_avg = weighted_avg if weighted_avg is not None else (int(round(sum(prices) / len(prices))) if prices else None)
        partial_max = prices[-1] if prices else None
        return finalize_market_result(parsed, {
            "count": partial_count,
            "min": partial_min,
            "median": None,
            "avg": partial_avg,
            "max": partial_max,
            "debug": {
                "raw_count": len(rows),
                "strict_count": len(strict),
                "semi_count": len(semi),
                "relaxed_count": len(relaxed),
                "sample_ids": [c["ad_id"] for c in comps[:8]],
                "api_calls": api_calls,
                "dedup_vehicle_count": len(comps),
                "code_effects": {},
                "scanned_pages": scanned_pages,
                "direct_scanned_pages": direct_scanned_pages,
                "direct_candidate_count": len(direct_rows),
                "direct_applied": direct_applied,
                "direct_model_variants_used": direct_model_variants_used[:8],
                "direct_last_action": direct_last_action,
                "direct_web_url": direct_web_url,
                "review_last_action": review_last_action,
                "review_web_url": review_web_url,
                "review_broad_action": review_broad_action,
                "review_broad_web_url": review_broad_web_url,
                "review_manual_url": review_manual_url,
                "review_km_hi": review_km_hi,
                "review_selected_maker": selected_maker,
                "review_model_group_name": model_group_name,
                "review_drive_badge_text": drive_badge_text,
                "review_model_candidates": review_model_candidates,
                "review_fuel_key": target_fuel_key,
                "anchor_badge": anchor_badge,
                "badge_group_candidates": badge_group_candidates[:8],
                "inferred_badge_groups": inferred_badge_groups[:8],
                "km_anchor": target_km_base,
                "km_window": [target_km_lo, target_km_hi],
                "year_mode": year_mode,
                "logic_version": LOGIC_VERSION,
                "fetch_errors": fetch_errors,
                "last_fetch_error": last_fetch_error,
                "excluded_contract_count": excluded_contract_count,
                "excluded_price_count": excluded_price_count,
                "excluded_outlier_count": excluded_outlier_count,
                "target_option_keys": sorted(target_option_keys),
                "target_option_codes": sorted(target_option_codes),
                "target_option_names": target_option_names,
                "target_option_unmatched": target_option_unmatched,
                "option_catalog_count": len(option_catalog_by_code),
                "option_catalog_cached_count": len(option_catalog_cached),
                "option_catalog_saved_count": option_catalog_saved_count,
                "option_catalog_learn_source": OPTION_CATALOG_LEARN_SOURCE,
                "option_scope_total_count": option_scope_total_count,
                "target_option_scope_selected_count": target_option_scope_selected_count,
                "target_option_scope_items": target_option_scope_items,
                "option_scope_mode": option_scope_mode,
                "trim_exact_count": len(exact_comps),
                "trim_similar_pool_count": len(similar_comps),
                "trim_fill_mode": trim_fill_mode,
                "detailed_fetch_enabled": ENABLE_ENCAR_DETAILED_FETCH,
                "detailed_fetch_rows": detailed_fetch_rows,
                "detailed_fetch_early_stop": detailed_fetch_early_stop,
                "price_stage_weight_counts": stage_weight_counts,
                "price_stage_weight_mode": "base=1.00,stage2=0.85,stage3=0.70",
                "reason": f"엔카 동일 트림 유사 매물 부족({partial_count}건)",
            },
            "comps": comps,
        })

    code_effects = {}
    all_codes = sorted({cd for c in comps for cd in c.get("codes", [])})
    for cd in all_codes:
        has = [c["price"] for c in comps if cd in c.get("codes", []) and c.get("price") is not None]
        no = [c["price"] for c in comps if cd not in c.get("codes", []) and c.get("price") is not None]
        if len(has) >= 4 and len(no) >= 4:
            code_effects[cd] = int(round(safe_median(has) - safe_median(no)))

    median_raw = None
    if price_weight_pairs:
        sorted_pairs = sorted(price_weight_pairs, key=lambda x: x[0])
        half_weight = sum(w for _, w in sorted_pairs) / 2.0
        running = 0.0
        for p, w in sorted_pairs:
            running += w
            if running >= half_weight:
                median_raw = p
                break
    median_floor = int((median_raw // 100) * 100) if median_raw is not None else None

    return finalize_market_result(parsed, {
        "count": len(prices),
        "min": prices[0],
        "median": median_floor,
        "avg": weighted_avg if weighted_avg is not None else int(round(sum(prices) / len(prices))),
        "max": prices[-1],
        "debug": {
            "raw_count": len(rows),
            "strict_count": len(strict),
            "semi_count": len(semi),
            "relaxed_count": len(relaxed),
            "sample_ids": [c["ad_id"] for c in comps[:8]],
            "api_calls": api_calls,
            "dedup_vehicle_count": len(comps),
            "code_effects": code_effects,
            "scanned_pages": scanned_pages,
            "direct_scanned_pages": direct_scanned_pages,
            "direct_candidate_count": len(direct_rows),
            "direct_applied": direct_applied,
            "direct_model_variants_used": direct_model_variants_used[:8],
            "direct_last_action": direct_last_action,
            "direct_web_url": direct_web_url,
            "review_last_action": review_last_action,
            "review_web_url": review_web_url,
            "review_broad_action": review_broad_action,
            "review_broad_web_url": review_broad_web_url,
            "review_manual_url": review_manual_url,
            "review_km_hi": review_km_hi,
            "review_selected_maker": selected_maker,
            "review_model_group_name": model_group_name,
            "review_drive_badge_text": drive_badge_text,
            "review_model_candidates": review_model_candidates,
            "review_fuel_key": target_fuel_key,
            "anchor_badge": anchor_badge,
            "badge_group_candidates": badge_group_candidates[:8],
            "inferred_badge_groups": inferred_badge_groups[:8],
            "km_anchor": target_km_base,
            "km_window": [target_km_lo, target_km_hi],
            "year_mode": year_mode,
            "target_option_keys": sorted(target_option_keys),
            "target_option_codes": sorted(target_option_codes),
            "target_option_names": target_option_names,
            "target_option_unmatched": target_option_unmatched,
            "option_catalog_count": len(option_catalog_by_code),
            "option_catalog_cached_count": len(option_catalog_cached),
            "option_catalog_saved_count": option_catalog_saved_count,
            "option_catalog_learn_source": OPTION_CATALOG_LEARN_SOURCE,
            "option_scope_total_count": option_scope_total_count,
            "target_option_scope_selected_count": target_option_scope_selected_count,
            "target_option_scope_items": target_option_scope_items,
            "option_scope_mode": option_scope_mode,
            "logic_version": LOGIC_VERSION,
            "fetch_errors": fetch_errors,
            "last_fetch_error": last_fetch_error,
            "excluded_contract_count": excluded_contract_count,
            "excluded_price_count": excluded_price_count,
            "excluded_outlier_count": excluded_outlier_count,
            "median_raw": median_raw,
            "price_stage_weight_counts": stage_weight_counts,
            "price_stage_weight_mode": "base=1.00,stage2=0.85,stage3=0.70",
            "trim_exact_count": len(exact_comps),
            "trim_similar_pool_count": len(similar_comps),
            "trim_fill_mode": trim_fill_mode,
            "detailed_fetch_enabled": ENABLE_ENCAR_DETAILED_FETCH,
            "detailed_fetch_rows": detailed_fetch_rows,
            "detailed_fetch_early_stop": detailed_fetch_early_stop,
        },
        "comps": comps,
    })


def estimate_sale(parsed, market):
    if market and market.get("median"):
        sale = market["median"]
    else:
        return None

    market_adj = 0
    target_km = to_int(parsed.get("mileage_km"))
    comp_kms = [to_int(c.get("mileage")) for c in (market.get("comps") or []) if to_int(c.get("mileage")) is not None]
    if target_km and len(comp_kms) >= 3:
        # Market median already comes from a mileage-bounded cohort.
        # Use in-cohort mileage difference only, with moderate weight.
        comp_km_median = safe_median(comp_kms)
        if comp_km_median is not None:
            diff = target_km - comp_km_median
            mileage_adj = int(round(-(diff / 10000.0) * 20))
            mileage_adj = max(-80, min(80, mileage_adj))
            market_adj += mileage_adj
    elif target_km and parsed.get("registration_year"):
        # Fallback when comparable mileage data is sparse.
        reg_year = to_int(parsed.get("registration_year"))
        reg_month = to_int(parsed.get("registration_month")) or 6
        if reg_year:
            now = datetime.now()
            months = max(1, (now.year - reg_year) * 12 + (now.month - reg_month))
            exp = int(round((months / 12.0) * 13000))
            diff = target_km - exp
            mileage_adj = int(round(-(diff / 10000.0) * 20))
            mileage_adj = max(-80, min(80, mileage_adj))
            market_adj += mileage_adj

    condition_adj = 0
    my_acc = parsed.get("my_accident_count")
    if parsed.get("accident_free") and ((my_acc is None) or (my_acc == 0)) and (not parsed.get("simple_exchange")):
        condition_adj += 20
    if my_acc is not None:
        condition_adj -= int(my_acc * 30)
    if parsed.get("simple_exchange"):
        condition_adj -= 25
    if parsed.get("panel_count"):
        condition_adj -= int((parsed.get("panel_count") or 0) * 7)
    if parsed.get("owner_changes") and parsed.get("owner_changes") > 0:
        condition_adj -= int(parsed.get("owner_changes") * 10)

    if market and market.get("debug", {}).get("code_effects"):
        effects = market["debug"]["code_effects"]
        target_codes = [str(cd) for cd in (market.get("debug", {}).get("target_option_codes") or [])]
        if target_codes:
            opt_bonus = 0
            for cd in target_codes:
                opt_bonus += max(0, to_int(effects.get(cd)) or 0)
            market_adj += min(120, opt_bonus)
        else:
            text_opts = " ".join(parsed.get("options") or [])
            if any(k in text_opts for k in ["내비", "네비", "멀티미디어"]):
                market_adj += max(effects.get("1038", 0), effects.get("1042", 0), 0)
            if any(k in text_opts for k in ["스타일"]):
                market_adj += max(effects.get("1036", 0), 0)
            if any(k in text_opts for k in ["드라이브와이즈", "드라이브 와이즈", "스마트센스", "스마트 센스"]):
                market_adj += max(effects.get("1037", 0), 0)

    # Reduce market-derived adjustment impact when comparable sample is thin.
    sample_count = to_int(market.get("count")) or 0
    sample_weight = min(1.0, 0.45 + 0.08 * sample_count)
    weighted_market_adj = int(round(market_adj * sample_weight))
    sale = int(round(sale + weighted_market_adj + condition_adj))

    # Keep estimate near observed market center.
    lo = int(round(market["median"] * 0.90))
    hi = int(round(market["median"] * 1.10))
    sale = max(lo, min(hi, sale))

    return max(0, int(round(sale)))


def solve_practical_bid(sale, repair, ad, margin, platform):
    sale_v = float(sale)
    repair_v = float(repair or 0)
    ad_v = float(ad or 0)
    margin_v = float(margin or 0)
    fixed_cost = 22.0  # shipping 10 + polishing 12
    k = fixed_cost + repair_v + ad_v + margin_v

    if platform == "autoinside":
        candidate = (sale_v - k) / 1.044
        if candidate < 500:
            commission = 11.0
            bid = (sale_v - (k + commission)) / 1.022
        elif candidate > 2000:
            commission = 44.0
            bid = (sale_v - (k + commission)) / 1.022
        else:
            bid = candidate
            commission = bid * 0.022
    else:
        tiers = [
            (14.0, 100.0),
            (30.0, 500.0),
            (36.0, 1500.0),
            (39.0, 3000.0),
            (47.0, 4000.0),
            (50.0, float("inf")),
        ]
        bid = 0.0
        commission = 0.0
        for fee, limit in tiers:
            cand = (sale_v - (k + fee)) / 1.022
            if cand <= limit:
                bid = cand
                commission = fee
                break

    buying_cost = bid * 0.022
    total_deductions = commission + 10.0 + 12.0 + buying_cost + repair_v + ad_v + margin_v
    expected_profit = sale_v - bid - (commission + buying_cost + 10.0 + 12.0 + repair_v + ad_v)
    return {
        "sale_price": int(round(sale_v)),
        "bid_price": int(round(bid)),
        "commission": int(round(commission)),
        "buying_cost": int(round(buying_cost)),
        "repair_cost": int(round(repair_v)),
        "ad_cost": int(round(ad_v)),
        "margin_target": int(round(margin_v)),
        "fixed_cost": 22,
        "total_deductions": int(round(total_deductions)),
        "expected_profit": int(round(expected_profit)),
        "platform": platform,
    }


def estimate_practical_calc(auto_sale, practical_req):
    req = practical_req or {}
    sale_in = to_int(req.get("sale_price"))
    repair = to_int(req.get("repair_cost")) or 0
    ad = to_int(req.get("ad_cost"))
    if ad is None:
        ad = 11
    margin = to_int(req.get("margin_target")) or 0
    platform = str(req.get("platform") or "autoinside").strip().lower()
    if platform not in {"autoinside", "dealer"}:
        platform = "autoinside"

    sale_base = sale_in if sale_in is not None else (to_int(auto_sale) if auto_sale is not None else None)
    if sale_base is None or sale_base <= 0:
        return {
            "reason": "판매가 기준값 없음",
            "platform": platform,
            "sale_price": None,
            "bid_price": None,
            "expected_profit": None,
        }
    result = solve_practical_bid(sale_base, repair, ad, margin, platform)
    result["source"] = "manual_sale" if sale_in is not None else "auto_estimated_sale"
    return result


def save_analysis(raw_text, parsed, market, estimated_sale, source_site="generic"):
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO analyses(
            raw_text, source_site, parser_profile_version,
            brand, model, trim, registration_year, registration_month,
            fuel, transmission, mileage_km, new_price, options_json, market_json,
            estimated_sale_price, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            raw_text,
            normalize_source_site(source_site),
            PARSER_PROFILE_VERSION,
            parsed.get("brand"),
            parsed.get("model"),
            parsed.get("trim"),
            parsed.get("registration_year"),
            parsed.get("registration_month"),
            parsed.get("fuel"),
            parsed.get("transmission"),
            parsed.get("mileage_km"),
            parsed.get("new_price"),
            json.dumps(parsed.get("options") or [], ensure_ascii=False),
            json.dumps(market or {}, ensure_ascii=False),
            estimated_sale,
            now_iso(),
        ),
    )
    row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "DELETE FROM analyses WHERE id NOT IN (SELECT id FROM analyses ORDER BY id DESC LIMIT ?)",
        (MAX_RECENT_ANALYSES,),
    )
    conn.commit()
    conn.close()
    return row_id


def recent_analyses(limit=MAX_RECENT_ANALYSES):
    conn = get_conn()
    rows = conn.execute(
        "SELECT id, source_site, brand, model, trim, mileage_km, estimated_sale_price, created_at FROM analyses ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


class AppHandler(BaseHTTPRequestHandler):
    def _json(self, data, status=200):
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _text(self, text, status=200, ctype="text/html; charset=utf-8"):
        payload = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            index = STATIC_DIR / "index.html"
            self._text(index.read_text(encoding="utf-8"))
            return

        if path.startswith("/static/"):
            rel = path.replace("/static/", "", 1)
            f = STATIC_DIR / rel
            if f.exists() and f.is_file():
                ctype = "text/plain; charset=utf-8"
                if f.suffix == ".html":
                    ctype = "text/html; charset=utf-8"
                elif f.suffix == ".js":
                    ctype = "application/javascript; charset=utf-8"
                elif f.suffix == ".css":
                    ctype = "text/css; charset=utf-8"
                self._text(f.read_text(encoding="utf-8"), ctype=ctype)
                return
            self._json({"error": "not found"}, status=404)
            return

        if path == "/api/recent":
            self._json({"items": recent_analyses()})
            return

        if path == "/api/config":
            self._json(
                {
                    "enable_source_profiles": ENABLE_SOURCE_PROFILES,
                    "can_disable_per_request": True,
                    "parser_profile_version": PARSER_PROFILE_VERSION,
                    "supported_source_sites": sorted(SUPPORTED_SOURCE_SITES),
                }
            )
            return

        self._json({"error": "not found"}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b"{}"
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:
            data = {}

        if path == "/api/analyze":
            raw_text = (data.get("raw_text") or "").strip()
            if not raw_text:
                self._json({"error": "raw_text is required"}, status=400)
                return
            use_source_profiles = ENABLE_SOURCE_PROFILES and parse_bool(data.get("use_source_profiles"), default=True)
            source_site = normalize_source_site(data.get("source_site"))
            model_override = (data.get("model_override") or "").strip()
            trim_override = (data.get("trim_override") or "").strip()
            if not use_source_profiles:
                source_site = "generic"
                model_override = ""
                trim_override = ""

            try:
                parsed_data = parse_text(
                    raw_text,
                    source_site=source_site,
                    model_override=model_override,
                    trim_override=trim_override,
                )
            except Exception as e:
                self._json({"error": f"parse failed: {e}"}, status=400)
                return

            market = None
            market_error = None
            try:
                market = fetch_market(parsed_data)
            except Exception as e:
                market_error = str(e)

            est_sale = estimate_sale(parsed_data, market)
            optimal_bid = estimate_optimal_bid(parsed_data, est_sale)
            practical_calc = estimate_practical_calc(est_sale, data.get("practical"))
            row_id = save_analysis(raw_text, parsed_data, market, est_sale, source_site=source_site)

            self._json(
                {
                    "id": row_id,
                    "parsed": parsed_data,
                    "market": market,
                    "market_error": market_error,
                    "estimated_sale_price": est_sale,
                    "optimal_bid": optimal_bid,
                    "practical_calc": practical_calc,
                    "pricing_basis": "market_median" if (est_sale is not None) else "no_market_data",
                    "source_site": source_site,
                    "parser_profile_version": PARSER_PROFILE_VERSION,
                    "enable_source_profiles": ENABLE_SOURCE_PROFILES,
                    "use_source_profiles": use_source_profiles,
                },
                status=201,
            )
            return

        self._json({"error": "not found"}, status=404)


def run():
    init_db()
    server = ThreadingHTTPServer(("127.0.0.1", 8080), AppHandler)
    url = "http://127.0.0.1:8080"
    print(f"Server running at {url}")
    threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    server.serve_forever()


if __name__ == "__main__":
    run()


