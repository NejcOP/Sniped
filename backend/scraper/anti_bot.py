import logging
import random
import time

COUNTRY_TO_DOMAIN = {
    "ae": "google.ae",
    "ar": "google.com.ar",
    "at": "google.at",
    "au": "google.com.au",
    "be": "google.be",
    "bg": "google.bg",
    "br": "google.com.br",
    "ca": "google.ca",
    "ch": "google.ch",
    "cl": "google.cl",
    "cn": "google.com.hk",
    "co": "google.com.co",
    "cz": "google.cz",
    "de": "google.de",
    "dk": "google.dk",
    "ee": "google.ee",
    "es": "google.es",
    "fi": "google.fi",
    "fr": "google.fr",
    "gb": "google.co.uk",
    "gr": "google.gr",
    "hr": "google.hr",
    "hu": "google.hu",
    "ie": "google.ie",
    "il": "google.co.il",
    "in": "google.co.in",
    "it": "google.it",
    "jp": "google.co.jp",
    "kr": "google.co.kr",
    "lt": "google.lt",
    "lv": "google.lv",
    "mx": "google.com.mx",
    "ng": "google.com.ng",
    "nl": "google.nl",
    "no": "google.no",
    "nz": "google.co.nz",
    "pl": "google.pl",
    "pt": "google.pt",
    "ro": "google.ro",
    "rs": "google.rs",
    "ru": "google.ru",
    "sa": "google.com.sa",
    "se": "google.se",
    "sg": "google.com.sg",
    "si": "google.si",
    "sk": "google.sk",
    "tr": "google.com.tr",
    "ua": "google.com.ua",
    "uk": "google.co.uk",
    "us": "google.com",
    "za": "google.co.za",
}

COUNTRY_TO_LOCALE = {
    "ae": "ar-AE",
    "ar": "es-AR",
    "at": "de-AT",
    "au": "en-AU",
    "be": "nl-BE",
    "bg": "bg-BG",
    "br": "pt-BR",
    "ca": "en-CA",
    "ch": "de-CH",
    "cl": "es-CL",
    "cn": "zh-CN",
    "co": "es-CO",
    "cz": "cs-CZ",
    "de": "de-DE",
    "dk": "da-DK",
    "ee": "et-EE",
    "es": "es-ES",
    "fi": "fi-FI",
    "fr": "fr-FR",
    "gb": "en-GB",
    "gr": "el-GR",
    "hr": "hr-HR",
    "hu": "hu-HU",
    "ie": "en-IE",
    "il": "he-IL",
    "in": "en-IN",
    "it": "it-IT",
    "jp": "ja-JP",
    "kr": "ko-KR",
    "lt": "lt-LT",
    "lv": "lv-LV",
    "mx": "es-MX",
    "ng": "en-NG",
    "nl": "nl-NL",
    "no": "nb-NO",
    "nz": "en-NZ",
    "pl": "pl-PL",
    "pt": "pt-PT",
    "ro": "ro-RO",
    "rs": "sr-RS",
    "ru": "ru-RU",
    "sa": "ar-SA",
    "se": "sv-SE",
    "sg": "en-SG",
    "si": "sl-SI",
    "sk": "sk-SK",
    "tr": "tr-TR",
    "ua": "uk-UA",
    "uk": "en-GB",
    "us": "en-US",
    "za": "en-ZA",
}

MODERN_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Rotation pool — 15 real-world browser UAs across Chrome/Firefox/Edge/Safari.
USER_AGENT_POOL = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    # Chrome macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    # Firefox macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    # Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    # Chrome Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Chrome Android (mobile)
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
]


def random_user_agent() -> str:
    """Return a random User-Agent from the rotation pool."""
    return random.choice(USER_AGENT_POOL)

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
