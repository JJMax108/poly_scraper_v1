# run_all_colours.py
# Single-worker, max-speed pass across colours_index.json with hard resource blocking and crisp logs

import asyncio
import json
import argparse
from pathlib import Path
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, List, Set

from playwright.async_api import async_playwright

from colour_worker import process_colour, ColourRow
from csv_writer import RangeCsvWriter

SESSION_FILE = Path("storage_state.json")
COLOURS_JSON = Path("colours_index.json")
DEFAULT_CSV_DIR = Path("csv")
LOG_DIR = Path("logs")
STATE_FILE = Path("run_state.json")

CORE_FIELDS = [
    "colour_name",
    "finish",
    "product_family",
    "sku_code",
    "title_raw",
    "qty_used_for_checks",
    "stock_result_raw",
    "price_result_raw",
    "product_url",
    "checked_at_iso",
]

def setup_root_logging() -> logging.Logger:
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("poly")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    logger.addHandler(ch)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    fh = RotatingFileHandler(str(LOG_DIR / f"all_colours-{ts}.log"), maxBytes=5_000_000, backupCount=3)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)

    return logger

def slugify(name: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")

def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            return {"done": []}
    return {"done": []}

def save_state(done: Set[str]):
    STATE_FILE.write_text(json.dumps({"done": sorted(done)}, indent=2))

def load_colours_in_json_order() -> List[Dict[str, Any]]:
    if not COLOURS_JSON.exists():
        raise SystemExit("colours_index.json not found. Run colours_index.py first.")
    data = json.loads(COLOURS_JSON.read_text())
    if not isinstance(data, list) or not data:
        raise SystemExit("No colours found in colours_index.json")
    return data

def slice_from_name(items: List[Dict[str, Any]], from_name: str) -> List[Dict[str, Any]]:
    if not from_name:
        return items
    target = from_name.strip().lower()
    for i, c in enumerate(items):
        if c.get("name", "").strip().lower() == target:
            return items[i:]
    return items

# hard resource blocking set
BLOCKED_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".woff", ".woff2", ".ttf", ".otf", ".mp4", ".webm")
BLOCKED_PARTS = (
    "google-analytics", "googletagmanager", "gtag/js", "doubleclick", "facebook", "hotjar",
    "segment.io", "/analytics", "/pixel", "/collect", "visualwebsiteoptimizer", "optimizely",
)

async def speed_routes(context):
    async def route_handler(route):
        req = route.request
        url = req.url.lower()
        if any(url.endswith(ext) for ext in BLOCKED_EXT):
            return await route.abort()
        if any(part in url for part in BLOCKED_PARTS):
            return await route.abort()
        if req.resource_type in ("image", "media", "font"):
            return await route.abort()
        return await route.continue_()
    await context.route("**/*", route_handler)

async def run_all(
    outdir: Path,
    start_index: int = 0,
    limit: int = 0,
    headless: bool = False,
    reset_state: bool = False,
    from_name: str = "",
    stop_after_error: bool = False
):
    logger = logging.getLogger("poly")

    if not SESSION_FILE.exists():
        raise SystemExit("storage_state.json not found. Run login_polytec.py first.")

    colours = load_colours_in_json_order()
    if from_name:
        colours = slice_from_name(colours, from_name)
    if start_index:
        colours = colours[start_index:]
    if limit and limit > 0:
        colours = colours[:limit]

    if reset_state and STATE_FILE.exists():
        STATE_FILE.unlink(missing_ok=True)

    state = load_state()
    done: Set[str] = set(state.get("done", []))

    writer = RangeCsvWriter(base_dir=outdir, core_fields=CORE_FIELDS)

    total = len(colours)
    logger.info("===============================================")
    logger.info("Polytec all colours scraper starting")
    logger.info(f"Input count: {total}")
    logger.info(f"Headless: {headless}")
    logger.info(f"CSV dir: {outdir.resolve()}")
    logger.info(f"Logs dir: {LOG_DIR.resolve()}")
    logger.info(f"State file: {STATE_FILE.resolve()}")
    logger.info(f"Worker: colour_worker.process_colour")
    logger.info("===============================================")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=headless,
            args=[
                "--disable-gpu",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--disable-extensions",
                "--no-default-browser-check",
                "--no-first-run",
            ],
        )
        context = await browser.new_context(
            storage_state=str(SESSION_FILE),
            java_script_enabled=True,
            viewport={"width": 1200, "height": 800},
            service_workers="block",
        )
        context.set_default_timeout(1400)
        context.set_default_navigation_timeout(4500)

        await speed_routes(context)

        page = await context.new_page()
        page.set_default_timeout(1400)
        page.set_default_navigation_timeout(4500)

        touched_overall: Set[str] = set()
        written_rows = 0

        for idx, colour in enumerate(colours, start=1):
            name = colour.get("name", f"colour_{idx}")
            url = colour["url"]
            slug = slugify(name)

            if url in done:
                logger.info(f"[{idx}/{total}] Skip, already done, {name}")
                continue

            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            file_handler = RotatingFileHandler(str(LOG_DIR / f"{slug}-{ts}.log"), maxBytes=2_000_000, backupCount=2)
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            logger.addHandler(file_handler)

            try:
                logger.info(f"[{idx}/{total}] Processing {name}  {url}")
                rows = await process_colour(page, url)

                touched_this_colour: Set[str] = set()
                for r in rows:
                    assert isinstance(r, ColourRow)
                    writer.append_row(
                        range_name=r.product_range_display,
                        core=r.core,
                        specs=r.specs
                    )
                    touched_this_colour.add(r.product_range_display)
                    written_rows += 1

                touched_overall.update(touched_this_colour)
                done.add(url)
                save_state(done)

                logger.info(f"[{idx}/{total}] Done {name}, rows={len(rows)}, ranges={sorted(touched_this_colour)}")

            except Exception as exc:
                logger.error(f"[{idx}/{total}] Error on {name}: {exc}")
                if stop_after_error:
                    raise
            finally:
                logger.removeHandler(file_handler)
                file_handler.close()
                await page.wait_for_timeout(60)  # tiny breather to look less botty

        await browser.close()

    logger.info("===============================================")
    logger.info(f"All colours complete. Rows written={written_rows}. Ranges touched={sorted(touched_overall)}")
    logger.info(f"State at {STATE_FILE.resolve()}, CSVs at {outdir.resolve()}, logs at {LOG_DIR.resolve()}")
    logger.info("===============================================")

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run Polytec scraper for all colours")
    p.add_argument("--start", type=int, default=0, help="Start index within the JSON order")
    p.add_argument("--limit", type=int, default=0, help="Limit number of colours")
    p.add_argument("--outdir", type=str, default=str(DEFAULT_CSV_DIR), help="Output CSV folder")
    p.add_argument("--headless", action="store_true", help="Run browser headless")
    p.add_argument("--reset-state", action="store_true", help="Delete run_state.json first")
    p.add_argument("--from-name", type=str, default="", help="Start from this colour name")
    p.add_argument("--stop-after-error", action="store_true", help="Abort on first error")
    return p

if __name__ == "__main__":
    setup_root_logging()
    args = build_arg_parser().parse_args()
    outdir = Path(args.outdir)
    asyncio.run(
        run_all(
            outdir=outdir,
            start_index=args.start,
            limit=args.limit,
            headless=args.headless,
            reset_state=args.reset_state,
            from_name=args.from_name,
            stop_after_error=args.stop_after_error,
        )
    )