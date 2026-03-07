# Refactor Phase 1 (Safe Layout)

## Applied changes
- Added directories: `app/`, `scripts/`, `docs/`, `archive/`.
- Moved script implementations into `scripts/`:
  - `scripts/collect_all_zero.py`
  - `scripts/import_heydealer_by_url.py`
  - `scripts/import_cn7_heydealer_30.py`
  - `scripts/rebuild_car_store.py`
- Kept compatibility wrappers at project root with original filenames.

## Why this is safe
- `app.py`, `run_app.bat`, `data.db`, `static/` untouched.
- Existing command habits still work:
  - `py collect_all_zero.py`
  - `py import_heydealer_by_url.py --url ...`
  - `py import_cn7_heydealer_30.py`
  - `py rebuild_car_store.py`

## Next phased target (not applied yet)
- Split `app.py` into route/service/repository modules under `app/` with no behavior change.
