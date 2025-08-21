# colour_worker.py
# Visit a single colour page, iterate finishes and SKUs, capture specs, check stock and price

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Tuple
from playwright.async_api import Page, TimeoutError as PWTimeout, Locator

@dataclass
class ColourRow:
    product_range_display: str   # H4 text as shown on site
    core: Dict[str, str]         # fixed identifying fields
    specs: Dict[str, str]        # flexible specs, keys become CSV columns

async def _visible_text_or_empty(loc: Locator) -> str:
    try:
        txt = await loc.text_content()
        return (txt or "").strip()
    except Exception:
        return ""

async def _wait_for_text_change(loc: Locator, timeout_ms: int) -> str:
    # Poll for non empty text with a hard cap
    end = datetime.now().timestamp() + timeout_ms / 1000.0
    last_txt = await _visible_text_or_empty(loc)
    while datetime.now().timestamp() < end:
        txt = await _visible_text_or_empty(loc)
        if txt and txt != last_txt:
            return txt
        await loc.page.wait_for_timeout(200)
    return await _visible_text_or_empty(loc)

async def _click_and_get_result(item: Locator, qty: int) -> Tuple[str, str]:
    await item.scroll_into_view_if_needed()
    await item.page.wait_for_timeout(80)

    # quantity is optional on some rows, so fail soft
    qty_input = item.locator("input[name='truck-item-qty']").first
    try:
        await qty_input.fill(str(qty))
        await qty_input.blur()
    except Exception:
        # Edgetape or non orderable rows might hide the input
        pass

    # stock
    stock_btn = item.locator("button.check-stock, .get-price.check-stock button:has-text('Check Stock')").first
    stock_result = item.locator("div.check-stock-result").first
    stock_text = ""
    try:
        await stock_btn.click(timeout=3500)
        try:
            await stock_result.wait_for(state="visible", timeout=4000)
        except PWTimeout:
            pass
        stock_text = await _wait_for_text_change(stock_result, timeout_ms=8000) or "EMPTY"
    except Exception:
        stock_text = "ERROR"

    # price
    price_btn = item.locator("button.get-price, .get-price.check-stock button:has-text('Get My Price')").first
    price_result = item.locator("div.get-price-result").first
    price_text = ""
    try:
        await price_btn.click(timeout=3500)
        try:
            await price_result.wait_for(state="visible", timeout=4000)
        except PWTimeout:
            pass
        price_text = await _wait_for_text_change(price_result, timeout_ms=8000) or "EMPTY"
    except Exception:
        price_text = "ERROR"

    return stock_text, price_text

async def _extract_specs_from_item(item: Locator) -> Dict[str, str]:
    await item.scroll_into_view_if_needed()
    await item.page.wait_for_timeout(50)

    # SKU and Title are best effort
    try:
        sku = (await item.locator("span.label").first.text_content() or "").strip()
    except Exception:
        sku = ""
    try:
        title = (await item.locator("h5").first.text_content() or "").strip()
    except Exception:
        title = ""

    # Attributes list is flexible, parse any Strong: Value pairs
    specs: Dict[str, str] = {}
    try:
        lis = item.locator("ul.item-attributes li")
        count = await lis.count()
        for i in range(count):
            txt = await lis.nth(i).text_content()
            if not txt:
                continue
            txt = txt.strip()
            # Expect patterns like "Finish: Woodmatt"
            if ":" in txt:
                key, val = txt.split(":", 1)
                key = key.strip()
                val = val.strip()
                if key and val:
                    specs[key] = val
    except Exception:
        pass

    # Pack Size line, if present
    try:
        info = await item.locator("h5.info").first.text_content()
        if info and "Pack Size:" in info:
            specs["Pack Size"] = info.split("Pack Size:", 1)[1].strip()
    except Exception:
        pass

    # Always include SKU and Title in specs map too, but the writer will keep core as authoritative
    specs.setdefault("SKU", sku)
    specs.setdefault("Title", title)

    return specs

async def _iter_family_items_in_active_panel(page: Page):
    panel = page.locator("div.tabs-panel.content.is-active").first
    items = panel.locator("div.items").locator(":scope > div.item")
    families = []
    handles = []
    count = await items.count()
    for i in range(count):
        el = items.nth(i)
        fam = await el.evaluate("""
        el => {
          let p = el.previousElementSibling
          while (p) {
            if (p.tagName === "H4") return p.textContent.trim()
            p = p.previousElementSibling
          }
          return ""
        }
        """)
        families.append(fam or "")
        handles.append(el)
    return list(zip(families, handles))

async def process_colour(page: Page, url: str) -> List[ColourRow]:
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_selector("#product-tabs", timeout=15000)

    colour_name = (await page.locator(".product-hero h1").first.text_content() or "").strip()

    tab_titles = page.locator("#product-tabs li.tabs-title a")
    n_tabs = await tab_titles.count()

    rows: List[ColourRow] = []

    for i in range(n_tabs):
        tab_link = tab_titles.nth(i)
        finish_text = (await tab_link.text_content() or "").strip()
        await tab_link.click()
        await page.wait_for_selector("div.tabs-panel.content.is-active", timeout=10000)

        panel = page.locator("div.tabs-panel.content.is-active").first
        panel_finish = (await panel.get_attribute("data-finish")) or finish_text

        family_items = await _iter_family_items_in_active_panel(page)

        for family, item in family_items:
            # Best effort specs parse
            specs = await _extract_specs_from_item(item)

            # Price and stock checks do not halt on error
            stock_text, price_text = await _click_and_get_result(item, qty=1)

            # Core fields are fixed
            core = {
                "colour_name": colour_name,
                "finish": panel_finish,
                "product_family": family,
                "sku_code": specs.get("SKU", ""),
                "title_raw": specs.get("Title", ""),
                "qty_used_for_checks": "1",  # text for CSV
                "stock_result_raw": stock_text,
                "price_result_raw": price_text,
                "product_url": url,
                "checked_at_iso": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }

            # Remove duplicates from specs that already exist in core, and rename a few to consistent headers
            normalised_specs: Dict[str, str] = {}
            rename = {
                "Substrate": "substrate",
                "Thickness": "thickness",
                "Length": "length",
                "Width": "width",
                "Pack Size": "pack_size",
                "Finish": "finish_attr",  # keep if present, but avoid colliding with core finish
            }
            for k, v in specs.items():
                if k in ("SKU", "Title"):
                    continue
                key = rename.get(k, k)
                if key in core:
                    # do not duplicate core fields
                    continue
                normalised_specs[key] = v

            rows.append(ColourRow(
                product_range_display=family or "Unknown",
                core=core,
                specs=normalised_specs
            ))

            # small pause to keep UI stable
            await page.wait_for_timeout(150)

    return rows