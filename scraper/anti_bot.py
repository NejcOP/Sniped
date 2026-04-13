import logging
import random
import time

COUNTRY_TO_DOMAIN = {
    "us": "google.com",
    "uk": "google.co.uk",
    "gb": "google.co.uk",
}

COUNTRY_TO_LOCALE = {
    "us": "en-US",
    "si": "sl-SI",
    "de": "de-DE",
    "at": "de-AT",
    "ch": "de-CH",
    "fr": "fr-FR",
    "it": "it-IT",
    "es": "es-ES",
    "pt": "pt-PT",
    "nl": "nl-NL",
    "pl": "pl-PL",
    "hr": "hr-HR",
    "rs": "sr-RS",
}

MODERN_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

try:
    from playwright_stealth import Stealth

    STEALTH_AVAILABLE = True
except Exception:
    Stealth = None
    STEALTH_AVAILABLE = False


def random_delay(min_ms: int = 250, max_ms: int = 900) -> None:
    time.sleep(random.uniform(min_ms, max_ms) / 1000.0)


def random_mouse_movements(page, count: int = 6) -> None:
    viewport = page.viewport_size or {"width": 1366, "height": 768}
    for _ in range(count):
        x = random.randint(0, max(50, viewport["width"] - 1))
        y = random.randint(0, max(50, viewport["height"] - 1))
        page.mouse.move(x, y, steps=random.randint(6, 20))
        random_delay(50, 180)


def human_type(locator, text: str) -> None:
    for char in text:
        if char == " ":
            time.sleep(random.uniform(0.04, 0.12))
        locator.type(char, delay=random.randint(40, 140))

        # Occasional micro-think pauses to avoid machine-like rhythm.
        if random.random() < 0.08:
            time.sleep(random.uniform(0.08, 0.22))


def human_like_scroll(panel_locator, steps: int = 3) -> None:
    for _ in range(steps):
        distance = random.randint(320, 780)
        panel_locator.evaluate("(el, d) => { el.scrollBy(0, d); }", distance)
        random_delay(500, 1200)


def apply_stealth(page) -> bool:
    if STEALTH_AVAILABLE and Stealth:
        try:
            Stealth().apply_stealth_sync(page)
            return True
        except Exception:
            pass

    logging.warning(
        "playwright-stealth is unavailable or failed to initialize; stealth mode skipped."
    )
    return False


def normalize_country_code(country_code: str) -> str:
    cleaned = (country_code or "us").strip().lower()
    if len(cleaned) < 2:
        return "us"
    return cleaned


def google_domain_for_country(country_code: str) -> str:
    code = normalize_country_code(country_code)
    if code in COUNTRY_TO_DOMAIN:
        return COUNTRY_TO_DOMAIN[code]
    return f"google.{code}"


def locale_for_country(country_code: str) -> str:
    code = normalize_country_code(country_code)
    if code in COUNTRY_TO_LOCALE:
        return COUNTRY_TO_LOCALE[code]
    return "en-US"
