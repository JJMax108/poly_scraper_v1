import asyncio
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError

LOGIN_URL = "https://www.polytec.com.au/login.php"
EMAIL = "admin@coastalcut2size.com.au"
PASSWORD = "r#MC6!MsNtNS@hS"
SESSION_FILE = Path("storage_state.json")

async def do_login():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        # Go to login
        await page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # Fill fields
        await page.locator("#UserName").fill(EMAIL)
        # blur to trigger any client hooks that look at username
        await page.locator("#UserName").press("Tab")
        await page.locator("#password").fill(PASSWORD)

        # Click login
        await page.get_by_role("button", name="Login").click()

        # The site submits via JS, then posts a hidden form to login.php
        # We wait for either a url change away from the login page
        # or for the success state indicated by the hidden session submit
        try:
            await page.wait_for_url(
                lambda url: "/login" not in url,
                timeout=20000
            )
        except TimeoutError:
            pass  # sometimes the path remains login.php for a beat

        # Give the app a moment to complete any client polls or redirects
        await page.wait_for_load_state("networkidle")

        # Optional evidence for debugging
        await page.screenshot(path="post_login.png", full_page=True)

        # Save session for reuse
        await context.storage_state(path=str(SESSION_FILE))

        # Quick sanity check for an error banner
        alert_visible = await page.locator("#alert-panel:not(.hide)").count()
        if alert_visible:
            html = await page.locator("#alert-panel").inner_html()
            print("Login alert panel showed content:")
            print(html)

        print(f"Done. Session saved to {SESSION_FILE.resolve()}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(do_login())