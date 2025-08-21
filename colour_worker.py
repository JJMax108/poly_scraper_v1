# colour_worker.py
# Per-item MOQ detection only, with optional retry if MOQ appears after first click
# Multi tab finishes, overlay dismissal, detailed logging

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Callable, Set
import re
import time
import logging
from playwright.async_api import Page, TimeoutError as PWTimeout, Locator, Response

log = logging.getLogger("poly")

@dataclass
class ColourRow:
    product_range_display: str
    core: Dict[str, str]
    specs: Dict[str, str]

# tiny helpers

async def _visible_text_or_empty(loc: Locator, timeout: int = 900) -> str:
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
        handle = await loc.element_handle(timeout=350)
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
    return await _visible_text_or_empty(loc, timeout=280)

async def _js_set_qty(qty_input: Locator, value: int):
    try:
        el = await qty_input.element_handle(timeout=320)
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
        await qty_input.fill(str(value), timeout=380)
        await qty_input.blur()
    except Exception:
        pass

async def _safe_click(btn: Locator, fast_timeout_ms: int = 620):
    try:
        await btn.click(force=True, no_wait_after=True, timeout=fast_timeout_ms)
        return True
    except Exception:
        pass
    try:
        handle = await btn.element_handle(timeout=220)
        if handle:
            await btn.page.evaluate("el => el.click()", handle)
            return True
    except Exception:
        pass
    return False

def _resp_matcher_contains(code: str) -> Callable[[Response], bool]:
    def ok(r: Response) -> bool:
        try:
            u = r.url or ""
            return bool(code and code in u)
        except Exception:
            return False
    return ok

# overlays

async def _dismiss_overlays(page: Page):
    try:
        await page.keyboard.press("Escape")
    except Exception:
        pass
    try:
        closers = page.locator(".reveal .close-button, .reveal [data-close], .modal [data-close]")
        n = await closers.count()
        for i in range(min(n, 3)):
            try:
                await closers.nth(i).click(force=True, timeout=200)
            except Exception:
                pass
    except Exception:
        pass
    try:
        await page.evaluate("""
        () => {
          for (const sel of ['.reveal-overlay', '.reveal', '.modal-overlay']) {
            document.querySelectorAll(sel).forEach(el => el.remove());
          }
        }""")
    except Exception:
        pass

# MOQ detection

_MOQ_RE_PRIMARY = re.compile(r"(?:minimum|min\.?)\s+(?:order\s+)?(?:qty|quantity)\s*[:\-]?\s*(\d+)", re.I)
_MOQ_RE_ALT = re.compile(r"\bMOQ\s*[:\-]?\s*(\d+)", re.I)
_STEP_RE = re.compile(r"(?:multiples|packs?)\s+of\s+(\d+)", re.I)

def _parse_int(s: Optional[str]) -> Optional[int]:
    if s is None:
        return None
    try:
        return int(str(s).strip())
    except Exception:
        return None

def _extract_moq_from_text(texts: List[str]) -> Tuple[Optional[int], Optional[int]]:
    joined = " ".join(t for t in texts if t)
    m1 = _MOQ_RE_PRIMARY.search(joined)
    m2 = _MOQ_RE_ALT.search(joined)
    min_qty = int(m1.group(1)) if m1 else (int(m2.group(1)) if m2 else None)
    k = _STEP_RE.search(joined)
    step = int(k.group(1)) if k else None
    return min_qty, step

async def _read_item_moq_hints(item: Locator) -> Tuple[Optional[int], Optional[int], List[str]]:
    qty_input = item.locator("input[name='truck-item-qty']").first
    min_attr = step_attr = None
    try:
        min_attr = await qty_input.get_attribute("min")
    except Exception:
        pass
    try:
        step_attr = await qty_input.get_attribute("step")
    except Exception:
        pass

    texts: List[str] = []
    try:
        # Only scan within the item itself
        local_alerts = await item.locator(
            ":scope h5.alert, :scope h5.info, :scope h5.label.warning, "
            ":scope .label.warning, :scope .get-price-result, :scope .check-stock-result"
        ).all_text_contents()
        texts.extend(local_alerts or [])
    except Exception:
        pass

    min_qty_text, step_text = _extract_moq_from_text(texts)

    min_qty = _parse_int(min_attr) if min_attr else None
    step = _parse_int(step_attr) if step_attr and str(step_attr).lower() != "any" else None

    if min_qty_text and (min_qty is None or min_qty_text > min_qty):
        min_qty = min_qty_text
    if step_text and (step is None or step_text > step):
        step = step_text

    return min_qty, step, texts

def _bump_to_multiple(qty: int, min_qty: int, step: int) -> int:
    q = max(qty, min_qty)
    if step > 1:
        rem = q % step
        if rem != 0:
            q += (step - rem)
    return q

def _need_moq_retry(stock_text: str, price_text: str) -> bool:
    probe = " ".join([stock_text or "", price_text or ""])
    return bool(_MOQ_RE_PRIMARY.search(probe) or _MOQ_RE_ALT.search(probe))

# per row ops

async def _click_pair(
    page: Page,
    item: Locator,
    code: str,
    stock_btn: Locator,
    stock_result: Locator,
    price_btn: Locator,
    price_result: Locator,
    qty_input: Locator,
    qty_value: int,
    wait_ms: int = 1100
) -> Tuple[str, str]:
    await _clear_result_box(stock_result)
    await _clear_result_box(price_result)
    await _js_set_qty(qty_input, qty_value)

    stock_text = ""
    price_text = ""

    try:
        if code:
            try:
                async with page.expect_response(_resp_matcher_contains(code), timeout=1050):
                    await _safe_click(stock_btn)
            except PWTimeout:
                await _safe_click(stock_btn)
        else:
            await _safe_click(stock_btn)
        stock_text = await _wait_text_fast(stock_result, max_wait_ms=wait_ms) or "EMPTY"
    except Exception:
        stock_text = "ERROR"

    try:
        if code:
            try:
                async with page.expect_response(_resp_matcher_contains(code), timeout=1050):
                    await _safe_click(price_btn)
            except PWTimeout:
                await _safe_click(price_btn)
        else:
            await _safe_click(price_btn)
        price_text = await _wait_text_fast(price_result, max_wait_ms=wait_ms) or "EMPTY"
    except Exception:
        price_text = "ERROR"

    return stock_text, price_text

async def _click_and_get_result(
    page: Page,
    item: Locator,
    requested_qty: int
) -> Tuple[str, str, int, int, int]:
    """
    Returns stock_text, price_text, used_qty, moq_min, moq_step
    MOQ is scoped to this item only.
    """
    await item.scroll_into_view_if_needed()
    await page.wait_for_timeout(8)

    # sku for nicer logs
    try:
        sku_for_log = (await item.locator("span.label").first.text_content(timeout=420) or "").strip()
    except Exception:
        sku_for_log = ""

    code = ""
    try:
        code = await item.locator(".item-inputs").first.get_attribute("data-code", timeout=420) or ""
    except Exception:
        pass

    qty_input = item.locator("input[name='truck-item-qty']").first
    stock_btn = item.locator(":scope button.check-stock, :scope .get-price.check-stock button:has-text('Check Stock')").first
    stock_result = item.locator(":scope div.check-stock-result").first
    price_btn = item.locator(":scope button.get-price, :scope .get-price.check-stock button:has-text('Get My Price')").first
    price_result = item.locator(":scope div.get-price-result").first

    await _dismiss_overlays(page)

    # read hints inside this item only
    moq_min_hint, moq_step_hint, moq_texts = await _read_item_moq_hints(item)

    # first pass quantity
    if moq_min_hint or moq_step_hint:
        moq_min = moq_min_hint or 1
        moq_step = moq_step_hint or 1
        used_qty = _bump_to_multiple(requested_qty, moq_min, moq_step)
        if used_qty != requested_qty or moq_min > 1 or moq_step > 1:
            log.info(f"MOQ for SKU {sku_for_log or code or '?'} min={moq_min} step={moq_step} qty {requested_qty} -> {used_qty}")
            for line in (moq_texts or [])[:3]:
                t = (line or "").strip()
                if t:
                    log.info(f"MOQ hint: {t}")
    else:
        moq_min = 1
        moq_step = 1
        used_qty = requested_qty

    # click with initial used_qty
    stock_text, price_text = await _click_pair(
        page, item, code, stock_btn, stock_result, price_btn, price_result, qty_input, used_qty
    )

    # if MOQ message appears only after first click, retry this item only
    if _need_moq_retry(stock_text, price_text):
        # try to parse exact numbers from the result texts plus any new label text inside the item
        more_texts: List[str] = []
        try:
            more_texts.extend(await item.locator(":scope .check-stock-result, :scope .get-price-result").all_text_contents() or [])
        except Exception:
            pass
        min_retry, step_retry = _extract_moq_from_text(more_texts)
        eff_min = max(moq_min, min_retry or 1)
        eff_step = max(moq_step, step_retry or 1)
        bumped = _bump_to_multiple(max(used_qty, requested_qty), eff_min, eff_step)
        if bumped != used_qty:
            log.info(f"MOQ retry for SKU {sku_for_log or code or '?'} min={eff_min} step={eff_step} qty {used_qty} -> {bumped}")
            used_qty = bumped
            stock_text, price_text = await _click_pair(
                page, item, code, stock_btn, stock_result, price_btn, price_result, qty_input, used_qty, wait_ms=1100
            )
        moq_min = eff_min
        moq_step = eff_step

    # final fallback one more quick try if both empty
    if stock_text in ("", "EMPTY") and price_text in ("", "EMPTY"):
        await _dismiss_overlays(page)
        stock_text, price_text = await _click_pair(
            page, item, code, stock_btn, stock_result, price_btn, price_result, qty_input, used_qty, wait_ms=900
        )

    return stock_text, price_text, used_qty, moq_min, moq_step

async def _extract_specs_from_item(item: Locator) -> Dict[str, str]:
    await item.scroll_into_view_if_needed()
    await item.page.wait_for_timeout(5)

    try:
        sku = (await item.locator("span.label").first.text_content(timeout=520) or "").strip()
    except Exception:
        sku = ""
    try:
        title = (await item.locator("h5").first.text_content(timeout=520) or "").strip()
    except Exception:
        title = ""

    specs: Dict[str, str] = {}
    try:
        lis = item.locator("ul.item-attributes li")
        count = await lis.count()
        for i in range(count):
            try:
                txt = await lis.nth(i).text_content(timeout=320)
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
        info = await item.locator("h5.info").first.text_content(timeout=380)
        if info and "Pack Size:" in info:
            specs["Pack Size"] = info.split("Pack Size:", 1)[1].strip()
    except Exception:
        pass

    specs.setdefault("SKU", sku)
    specs.setdefault("Title", title)
    return specs

# tabs

async def _get_finish_tabs(page: Page) -> List[Tuple[str, str]]:
    tabs = page.locator("#product-tabs li.tabs-title a")
    n = await tabs.count()
    out: List[Tuple[str, str]] = []
    for i in range(n):
        a = tabs.nth(i)
        title = (await a.text_content(timeout=620) or "").strip()
        href = (await a.get_attribute("href")) or ""
        out.append((title, href if href and href.startswith("#") else ""))
    return out or [("Default", "")]

async def _activate_tab(page: Page, title: str, href: str):
    await _dismiss_overlays(page)
    tabs = page.locator("#product-tabs li.tabs-title a")
    n = await tabs.count()
    target = None
    for i in range(n):
        t = (await tabs.nth(i).text_content(timeout=480) or "").strip()
        if t.lower() == title.lower():
            target = tabs.nth(i)
            break
    if not target:
        target = tabs.first

    for _ in range(3):
        try:
            await target.click(timeout=800, force=True)
            break
        except Exception:
            await _dismiss_overlays(page)

    if href.startswith("#"):
        pid = href[1:]
        try:
            await page.wait_for_selector(f"div.tabs-panel.content.is-active#{pid}", timeout=2400)
            return
        except PWTimeout:
            pass
    await page.wait_for_selector("div.tabs-panel.content.is-active", timeout=2400)

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

# main

async def process_colour(page: Page, url: str) -> List[ColourRow]:
    log.info(f"run start colour={url}")
    await page.goto(url, wait_until="domcontentloaded")
    await page.wait_for_selector("#product-tabs", timeout=6000)

    colour_name = (await page.locator(".product-hero h1").first.text_content(timeout=880) or "").strip()
    log.info(f"colour name: {colour_name}")

    tabs = await _get_finish_tabs(page)
    log.info(f"finish tabs: {len(tabs)}")

    rows: List[ColourRow] = []
    seen: Set[Tuple[str, str]] = set()  # (finish, sku)

    for idx_tab, (tab_title, tab_href) in enumerate(tabs, start=1):
        await _activate_tab(page, tab_title, tab_href)
        panel = page.locator("div.tabs-panel.content.is-active").first
        panel_finish = (await panel.get_attribute("data-finish")) or tab_title
        finish_display = (panel_finish or tab_title or "Unknown").strip()
        log.info(f"tab {idx_tab}/{len(tabs)} -> {finish_display}")

        family_items = await _iter_family_items_in_active_panel(page)
        log.info(f"items in finish '{finish_display}': {len(family_items)}")

        current_range = None
        for idx_row, (family, item) in enumerate(family_items, start=1):
            if family != current_range:
                current_range = family
                log.info(f"range -> {current_range or 'Unknown'}")

            t0 = time.monotonic()

            specs = await _extract_specs_from_item(item)
            sku = specs.get("SKU", "").strip()
            if (finish_display, sku) in seen and sku:
                log.info(f"row {idx_row}: sku={sku} duplicate in finish '{finish_display}', skipping")
                continue
            if sku:
                seen.add((finish_display, sku))

            stock_text, price_text, used_qty, moq_min, moq_step = await _click_and_get_result(
                page, item, requested_qty=1
            )
            dt = time.monotonic() - t0

            core = {
                "colour_name": colour_name,
                "finish": finish_display,
                "product_family": family,
                "sku_code": sku,
                "title_raw": specs.get("Title", ""),
                "qty_used_for_checks": str(used_qty),
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

            normalised_specs["minimum_order_qty"] = str(moq_min)
            normalised_specs["order_multiple"] = str(moq_step)

            rows.append(ColourRow(
                product_range_display=family or "Unknown",
                core=core,
                specs=normalised_specs
            ))

            sflag = "OK" if stock_text not in ("", "EMPTY", "ERROR") else stock_text or "EMPTY"
            pflag = "OK" if price_text not in ("", "EMPTY", "ERROR") else price_text or "EMPTY"
            log.info(
                f"row {idx_row}: sku={sku} title='{core['title_raw'][:60]}' "
                f"stock={sflag} price={pflag} "
                f"qty_used={used_qty} moq_min={moq_min} step={moq_step} time={dt:.2f}s"
            )

            await page.wait_for_timeout(8)

    log.info(f"run end rows={len(rows)}")
    return rows