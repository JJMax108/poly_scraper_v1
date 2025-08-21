# colour_worker.py
# Faster waits + detailed logging
# Visit a single colour page, iterate finishes and SKUs, capture specs, check stock and price

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Tuple
import time
import logging
from playwright.async_api import Page, TimeoutError as PWTimeout, Locator

log = logging.getLogger("poly")

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

async def _clear_result_box(loc: Locator):
    try:
        await loc.evaluate("el => { el.textContent = ''; }")
    except Exception:
        pass

async def _wait_until_nonempty(loc: Locator, max_wait_ms: int = 2200) -> str:
    """Fast path. Return as soon as there is any text. Never wait for a change."""
    end = time.monotonic() + max_wait_ms / 1000.0
    while time.monotonic() < end:
        txt = await _visible_text_or_empty(loc)
        if txt:
            return txt
        await loc.page.wait_for_timeout(120)
    return await _visible_text_or_empty(loc)

async def _click_and_get_result(item: Locator, qty: int) -> Tuple[str, str]:
    await item.scroll_into_view_if_needed()
    await item.page.wait_for_timeout(40)

    # quantity can be absent on some rows
    qty_input = item.locator("input[name='truck-item-qty']").first
    try:
        await qty_input.fill(str(qty))
        await qty_input.blur()
    except Exception:
        pass

    # result boxes
    stock_btn = item.locator("button.check-stock, .get-price.check-stock button:has-text('Check Stock')").first
    stock_result = item.locator("div.check-stock-result").first
    price_btn = item.locator("button.get-price, .get-price.check-stock button:has-text('Get My Price')").first
    price_result = item.locator("div.get-price-result").first

    # clear old text so we do not wait on stale content
    await _clear_result_box(stock_result)
    await _clear_result_box(price_result)

    # stock
    stock_text = ""
    try:
        await stock_btn.click(timeout=1500)
        try:
            await stock_result.wait_for(state="visible", timeout=1200)
        except PWTimeout:
            pass
        stock_text = await _wait_until_nonempty(stock_result, max_wait_ms=1800) or "EMPTY"
    except Exception:
        stock_text = "ERROR"

    # price
    price_text = ""
    try:
        await price_btn.click(timeout=1500)
        try:
            await price_result.wait_for(state="visible", timeout=1200)
        except PWTimeout:
            pass
        price_text = await _wait_until_nonempty(price_result, max_wait_ms=1800) or "EMPTY"
    except Exception:
        price_text = "ERROR"

    return stock_text, price_text

async def _extract_specs_from_item(item: Locator) -> Dict[str, str]:
    await item.scroll_into_view_if_needed()
    await item.page.wait_for_timeout(30)

    # SKU and Title are best effort
    try:
        sku = (await item.locator("span.label").first.text_content() or "").strip()
    except Exception:
        sku = ""
    try:
        title = (await item.locator("h5").first.text_content() or "").strip()
    except Exception:
        title = ""

    # Flexible attributes Strong: Value
    specs: Dict[str, str] = {}
    try:
        lis = item.locator("ul.item-attributes li")
        count = await lis.count()
        for i in range(count):
            txt = await lis.nth(i).text_content()
            if not txt:
                continue
            txt = txt.strip()
            if ":" in txt:
                key, val = txt.split(":", 1)
                key = key.strip()
                val = val.strip()
                if key and val:
                    specs[key] = val
    except Exception:
        pass

    # Pack Size
    try:
        info = await item.locator("h5.info").first.text_content()
        if info and "Pack Size:" in info:
            specs["Pack Size"] = info.split("Pack Size:", 1)[1].strip()
    except Exception:
        pass

    specs.setdefault("SKU", sku)
    specs.setdefault("Title", title)

    return specs

async def _iter_family_items_in_active_panel(page: Page):
    panel = page.locator("div.tabs-panel.content.is-active").first
    items = panel.locator("div.items").locator(":scope > div.item")
    families = []
    handles = []
    count = await items.count()
    # We will also emit a marker when the range (H4) changes
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
    log.info(f"run start colour={url}")
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_selector("#product-tabs", timeout=10000)

    colour_name = (await page.locator(".product-hero h1").first.text_content() or "").strip()
    log.info(f"colour name: {colour_name}")

    tab_titles = page.locator("#product-tabs li.tabs-title a")
    n_tabs = await tab_titles.count()
    log.info(f"finish tabs: {n_tabs}")

    rows: List[ColourRow] = []

    for i in range(n_tabs):
        tab_link = tab_titles.nth(i)
        finish_text = (await tab_link.text_content() or "").strip()
        log.info(f"tab {i+1}/{n_tabs} -> {finish_text}")
        await tab_link.click()
        await page.wait_for_selector("div.tabs-panel.content.is-active", timeout=5000)

        panel = page.locator("div.tabs-panel.content.is-active").first
        panel_finish = (await panel.get_attribute("data-finish")) or finish_text

        family_items = await _iter_family_items_in_active_panel(page)
        log.info(f"items in finish '{panel_finish}': {len(family_items)}")

        current_range = None
        for idx, (family, item) in enumerate(family_items, start=1):
            if family != current_range:
                current_range = family
                log.info(f"range -> {current_range or 'Unknown'}")

            t0 = time.monotonic()
            specs = await _extract_specs_from_item(item)
            stock_text, price_text = await _click_and_get_result(item, qty=1)
            dt = time.monotonic() - t0

            core = {
                "colour_name": colour_name,
                "finish": panel_finish,
                "product_family": family,
                "sku_code": specs.get("SKU", ""),
                "title_raw": specs.get("Title", ""),
                "qty_used_for_checks": "1",
                "stock_result_raw": stock_text,
                "price_result_raw": price_text,
                "product_url": url,
                "checked_at_iso": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }

            # Normalise spec keys
            normalised_specs: Dict[str, str] = {}
            rename = {
                "Substrate": "substrate",
                "Thickness": "thickness",
                "Length": "length",
                "Width": "width",
                "Pack Size": "pack_size",
                "Finish": "finish_attr",
            }
            for k, v in specs.items():
                if k in ("SKU", "Title"):
                    continue
                key = rename.get(k, k)
                if key in core:
                    continue
                normalised_specs[key] = v

            rows.append(ColourRow(
                product_range_display=family or "Unknown",
                core=core,
                specs=normalised_specs
            ))

            # concise status for console and file log
            stock_flag = "OK" if stock_text not in ("", "EMPTY", "ERROR") else stock_text or "EMPTY"
            price_flag = "OK" if price_text not in ("", "EMPTY", "ERROR") else price_text or "EMPTY"
            log.info(f"row {idx}: sku={core['sku_code']} title='{core['title_raw'][:60]}' stock={stock_flag} price={price_flag} time={dt:.2f}s")

            # tiny pause only
            await page.wait_for_timeout(40)

    log.info(f"run end rows={len(rows)}")
    return rows