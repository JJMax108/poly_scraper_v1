# run_one_colour.py
# Process the first colour from colours_index.json and write to per range CSVs

import asyncio
import json
from pathlib import Path
from playwright.async_api import async_playwright

from colour_worker import process_colour, ColourRow
from csv_writer import RangeCsvWriter

SESSION_FILE = Path("storage_state.json")
COLOURS_JSON = Path("colours_index.json")
CSV_DIR = Path("csv")

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
    print(f"Processing first colour: {target['name']}  {url}")

    writer = RangeCsvWriter(base_dir=CSV_DIR, core_fields=CORE_FIELDS)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=str(SESSION_FILE))
        page = await context.new_page()

        rows = await process_colour(page, url)

        count = 0
        for r in rows:
            assert isinstance(r, ColourRow)
            writer.append_row(
                range_name=r.product_range_display,
                core=r.core,
                specs=r.specs
            )
            count += 1

        await browser.close()

    print(f"Wrote {count} row(s) across per range CSVs in {CSV_DIR.resolve()}")

if __name__ == "__main__":
    asyncio.run(main())