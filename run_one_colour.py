# run_one_colour.py
# Process the first colour and write to per range CSVs
# Sets up console + file logging so you can track speed and actions

import asyncio
import json
from pathlib import Path
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from playwright.async_api import async_playwright

from colour_worker import process_colour, ColourRow
from csv_writer import RangeCsvWriter

SESSION_FILE = Path("storage_state.json")
COLOURS_JSON = Path("colours_index.json")
CSV_DIR = Path("csv")
LOG_DIR = Path("logs")

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

def setup_logging(colour_slug: str):
    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("poly")
    logger.setLevel(logging.INFO)

    # clear old handlers if re running in same process
    logger.handlers.clear()

    # console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    logger.addHandler(ch)

    # file
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    fh = RotatingFileHandler(str(LOG_DIR / f"{colour_slug}-{ts}.log"), maxBytes=2_000_000, backupCount=2)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)

    return logger

def slugify(name: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in name).strip("_")

async def main():
    if not SESSION_FILE.exists():
        raise SystemExit("storage_state.json not found. Run login_polytec.py first.")
    if not COLOURS_JSON.exists():
        raise SystemExit("colours_index.json not found. Run colours_index.py first.")

    colours = json.loads(COLOURS_JSON.read_text())
    if not colours:
        raise SystemExit("No colours found in colours_index.json")

    target = colours[0]
    url = target["url"]
    colour_slug = slugify(target.get("name", "colour"))
    logger = setup_logging(colour_slug)

    logger.info(f"Processing first colour: {target['name']}  {url}")

    writer = RangeCsvWriter(base_dir=CSV_DIR, core_fields=CORE_FIELDS)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=str(SESSION_FILE))
        page = await context.new_page()

        rows = await process_colour(page, url)

        count = 0
        touched = set()
        for r in rows:
            assert isinstance(r, ColourRow)
            writer.append_row(
                range_name=r.product_range_display,
                core=r.core,
                specs=r.specs
            )
            touched.add(r.product_range_display)
            count += 1

        await browser.close()

    logger.info(f"Run complete. Rows written={count}. CSVs touched={sorted(touched)}. Output dir={CSV_DIR.resolve()}")

if __name__ == "__main__":
    asyncio.run(main())