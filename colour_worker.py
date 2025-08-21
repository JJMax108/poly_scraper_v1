# colour_worker.py
# Clamp waits, force clicks, JS qty set, and wait on network responses that include the SKU code

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Callable
import time
import logging
from playwright.async_api import Page, TimeoutError as PWTimeout, Locator, Response

log = logging.getLogger("poly")

@dataclass
class ColourRow:
    product_range_display: str
    core: Dict[str, str]
    specs: Dict[str, str]

async def _visible_text_or_empty(loc: Locator, timeout: int = 1000) -> str:
    try:
        txt = await loc.text_content(timeout=timeout)
        return (txt or "").strip()
    except Exception:
        return ""

async def _clear_result_box(loc: Locator):
    try:
        await loc.evaluate("el => { el.textContent = ''; el.classList.remove('hide'); }")
    except Exception:
        pass

async def _wait_text_fast(loc: Locator, max_wait_ms: int = 1200) -> str:
    try:
        handle = await loc.element_handle(timeout=500)
    except Exception:
        handle = None
    try:
        if handle:
            await loc.page.wait_for_function(
                "el => el && el.textContent && el.textContent.trim().length > 0",
                arg=handle,
                timeout=max_wait_ms
            )
    except PWTimeout:
        pass
    return await _visible_text_or_empty(loc, timeout=300)

async def _js_set_qty(qty_input: Locator, value: int):
    try:
        el = await qty_input.element_handle(timeout=400)
        if el:
            await qty_input.page.evaluate(
                """(el, v) => {
                    el.value = String(v);
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                }""",
                el, value
            )
            return
    except Exception:
        pass
    try:
        await qty_input.fill(str(value), timeout=500)
        await qty_input.blur()
    except Exception:
        pass

async def _safe_click(btn: Locator, fast_timeout_ms: int = 700):
    try:
        await btn.click(force=True, no_wait_after=True, timeout=fast_timeout_ms)
        return True
    except Exception:
        pass
    try:
        handle = await btn.element_handle(timeout=300)
        if handle:
            await btn.page.evaluate("el => el.click()", handle)
            return True
    except Exception:
        pass
    return False

def _resp_matcher_contains(code: str) -> Callable[[Response], bool]:
    # Many sites include the SKU or data-code in the 2 ajax endpoints.
    # We keep it generic so we do not depend on exact paths.
    def ok(r: Response) -> bool:
        try:
            u = r.url or ""
            return code and code in u
        except Exception:
            return False
    return ok

async def _click_and_get_result(page: Page, item: Locator, qty: int) -> Tuple[str, str]:
    await item.scroll_into_view_if_needed()
    await item.page.wait_for_timeout(10)

    # get the per row code so we can latch onto network traffic
    code = ""
    try:
        code = await item.locator(".item-inputs").first.get_attribute("data-code", timeout=500) or ""
    except Exception:
        pass

    qty_input = item.locator("input[name='truck-item-qty']").first
    await _js_set_qty(qty_input, qty)

    stock_btn = item.locator(":scope button.check-stock, :scope .get-price.check-stock button:has-text('Check Stock')").first
    stock_result = item.locator(":scope div.check-stock-result").first
    price_btn = item.locator(":scope button.get-price, :scope .get-price.check-stock button:has-text('Get My Price')").first
    price_result = item.locator(":scope div.get-price-result").first

    await _clear_result_box(stock_result)
    await _clear_result_box(price_result)

    # stock - click, then wait for either the matching response or quick text
    stock_text = ""
    try:
        if code:
            try:
                async with page.expect_response(_resp_matcher_contains(code), timeout=1200):
                    await _safe_click(stock_btn)
            except PWTimeout:
                await _safe_click(stock_btn)
        else:
            await _safe_click(stock_btn)
        stock_text = await _wait_text_fast(stock_result, max_wait_ms=1200) or "EMPTY"
    except Exception:
        stock_text = "ERROR"

    # price - same idea
    price_text = ""
    try:
        if code:
            try:
                async with page.expect_response(_resp_matcher_contains(code), timeout=1200):
                    await _safe_click(price_btn)
            except PWTimeout:
                await _safe_click(price_btn)
        else:
            await _safe_click(price_btn)
        price_text = await _wait_text_fast(price_result, max_wait_ms=1200) or "EMPTY"
    except Exception:
        price_text = "ERROR"

    return stock_text, price_text

async def _extract_specs_from_item(item: Locator) -> Dict[str, str]:
    await item.scroll_into_view_if_needed()
    await item.page.wait_for_timeout(5)

    try:
        sku = (await item.locator("span.label").first.text_content(timeout=600) or "").strip()
    except Exception:
        sku = ""
    try:
        title = (await item.locator("h5").first.text_content(timeout=600) or "").strip()
    except Exception:
        title = ""

    specs: Dict[str, str] = {}
    try:
        lis = item.locator("ul.item-attributes li")
        count = await lis.count()
        for i in range(count):
            try:
                txt = await lis.nth(i).text_content(timeout=400)
            except Exception:
                txt = ""
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

    try:
        info = await item.locator("h5.info").first.text_content(timeout=500)
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
    await page.wait_for_selector("#product-tabs", timeout=6000)

    colour_name = (await page.locator(".product-hero h1").first.text_content(timeout=1000) or "").strip()
    log.info(f"colour name: {colour_name}")

    tab_titles = page.locator("#product-tabs li.tabs-title a")
    n_tabs = await tab_titles.count()
    log.info(f"finish tabs: {n_tabs}")

    rows: List[ColourRow] = []

    for i in range(n_tabs):
        tab_link = tab_titles.nth(i)
        finish_text = (await tab_link.text_content(timeout=800) or "").strip()
        log.info(f"tab {i+1}/{n_tabs} -> {finish_text}")
        await tab_link.click()
        await page.wait_for_selector("div.tabs-panel.content.is-active", timeout=3000)

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
            stock_text, price_text = await _click_and_get_result(page, item, qty=1)
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

            rename = {
                "Substrate": "substrate",
                "Thickness": "thickness",
                "Length": "length",
                "Width": "width",
                "Pack Size": "pack_size",
                "Finish": "finish_attr",
            }
            normalised_specs: Dict[str, str] = {}
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

            stock_flag = "OK" if stock_text not in ("", "EMPTY", "ERROR") else stock_text or "EMPTY"
            price_flag = "OK" if price_text not in ("", "EMPTY", "ERROR") else price_text or "EMPTY"
            log.info(f"row {idx}: sku={core['sku_code']} title='{core['title_raw'][:60]}' stock={stock_flag} price={price_flag} time={dt:.2f}s")

            await page.wait_for_timeout(10)

    log.info(f"run end rows={len(rows)}")
    return rows