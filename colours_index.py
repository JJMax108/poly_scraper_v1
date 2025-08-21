# colours_index.py
# Collect the full Colours list in on screen order and save to colours_index.json

import asyncio
import json
from pathlib import Path
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

BASE_URL = "https://www.polytec.com.au/"
COLOURS_URL = "https://www.polytec.com.au/colours/"
SESSION_FILE = Path("storage_state.json")
OUT_JSON = Path("colours_index.json")
ARTIFACTS_DIR = Path("artifacts")

def slug_from_href(href: str) -> str:
    try:
        path = urlparse(href).path.strip("/")
        parts = path.split("/")
        if len(parts) >= 2 and parts[0].startswith("colour"):
            return parts[1]
        return parts[-1]
    except Exception:
        return href

async def scroll_until_stable(page, list_selector: str, max_rounds: int = 20) -> int:
    await page.wait_for_selector(f"{list_selector} li")
    last_count = 0
    stable_rounds = 0
    for _ in range(max_rounds):
        count = await page.locator(f"{list_selector} li").count()
        if count == last_count:
            stable_rounds += 1
        else:
            stable_rounds = 0
            last_count = count
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        try:
            await page.wait_for_load_state("networkidle", timeout=3000)
        except PWTimeout:
            pass
        await page.wait_for_timeout(300)
        if stable_rounds >= 2:
            break
    return await page.locator(f"{list_selector} li").count()

async def collect_colours(page):
    raw = await page.locator("ul.colour-thumbs li").evaluate_all(
        """els => els.map(li => {
            const a = li.querySelector("a");
            const h5 = li.querySelector("h5");
            const href = a ? a.getAttribute("href") : null;
            const name = h5 ? h5.textContent.trim() : "";
            return href ? { href, name } : null;
        }).filter(Boolean)"""
    )
    results = []
    for item in raw:
        abs_url = urljoin(BASE_URL, item["href"])
        results.append({
            "name": item["name"],
            "url": abs_url,
            "slug": slug_from_href(abs_url),
        })
    return results

async def goto_colours_via_header(page):
    try:
        header_link = page.locator("header.header-primary .nav-priority a[href='/colours/']").first
        await header_link.click()
        await page.wait_for_url(lambda url: "/colours/" in url, timeout=10000)
        return True
    except Exception as e:
        print(f"Header click failed: {e}")
        return False

async def run():
    ARTIFACTS_DIR.mkdir(exist_ok=True)
    if not SESSION_FILE.exists():
        raise SystemExit("storage_state.json not found. Run login_polytec.py first.")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(storage_state=str(SESSION_FILE))
        page = await context.new_page()

        await page.goto(BASE_URL, wait_until="domcontentloaded")

        try:
            await page.get_by_role("button", name="Coastal Cut To Size Pty Ltd").wait_for(timeout=3000)
            print("Authenticated header detected.")
        except PWTimeout:
            print("Warning. Account header not found. Session may still be valid, proceeding.")

        ok = await goto_colours_via_header(page)
        if not ok:
            print("Falling back to direct navigation.")
            await page.goto(COLOURS_URL, wait_until="domcontentloaded")
            await page.wait_for_url(lambda url: "/colours/" in url, timeout=10000)

        await page.wait_for_selector("ul.colour-thumbs li")

        total = await scroll_until_stable(page, "ul.colour-thumbs")
        print(f"Colour tiles detected: {total}")

        colours = await collect_colours(page)
        print(f"Collected links: {len(colours)}")

        await page.screenshot(path=str(ARTIFACTS_DIR / "colours_page.png"), full_page=True)
        (ARTIFACTS_DIR / "colours_page.html").write_text(await page.content(), encoding="utf-8")

        OUT_JSON.write_text(json.dumps(colours, indent=2), encoding="utf-8")
        print(f"Wrote {OUT_JSON.resolve()}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())