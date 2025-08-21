# run_all_colours.py
# Iterate every colour in colours_index.json, write per range CSVs, resilient with resume and clear logs

import asyncio
import json
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
CSV_DIR = Path("csv")
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

def setup_root_logging():
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

async def run_all(start_index: int = 0, limit: int = 0):
    logger = logging.getLogger("poly")

    if not SESSION_FILE.exists():
        raise SystemExit("storage_state.json not found. Run login_polytec.py first.")
    if not COLOURS_JSON.exists():
        raise SystemExit("colours_index.json not found. Run colours_index.py first.")

    colours: List[Dict[str, Any]] = json.loads(COLOURS_JSON.read_text())
    if not colours:
        raise SystemExit("No colours found in colours_index.json")

    # slice if requested
    if start_index:
        colours = colours[start_index:]
    if limit and limit > 0:
        colours = colours[:limit]

    state = load_state()
    done: Set[str] = set(state.get("done", []))

    writer = RangeCsvWriter(base_dir=CSV_DIR, core_fields=CORE_FIELDS)

    total = len(colours)
    logger.info(f"Starting run across {total} colours")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=str(SESSION_FILE))
        context.set_default_timeout(1800)
        context.set_default_navigation_timeout(6000)

        page = await context.new_page()
        page.set_default_timeout(1800)
        page.set_default_navigation_timeout(6000)

        touched_overall: Set[str] = set()
        written_rows = 0

        for idx, colour in enumerate(colours, start=1):
            name = colour.get("name", f"colour_{idx}")
            url = colour["url"]
            slug = slugify(name)

            if url in done:
                logger.info(f"[{idx}/{total}] Skip, already done, {name}")
                continue

            # per colour log file
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
                # keep going to the next colour
            finally:
                # remove the per colour handler so the next colour gets a fresh file
                logger.removeHandler(file_handler)
                file_handler.close()

                # tiny breather to avoid hammering
                await page.wait_for_timeout(120)

        await browser.close()

    logger.info(f"All colours complete. Rows written={written_rows}. Ranges touched={sorted(touched_overall)}")
    logger.info(f"State file at {STATE_FILE.resolve()}, CSV dir at {CSV_DIR.resolve()}, logs at {LOG_DIR.resolve()}")

if __name__ == "__main__":
    # Optional slicing by editing these numbers before running
    asyncio.run(run_all(start_index=0, limit=0))