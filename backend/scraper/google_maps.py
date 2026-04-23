import logging
import os
import random
import re
import time
from threading import Lock
from pathlib import Path
from typing import Callable, List, Optional
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright

try:
    from playwright_stealth import stealth_sync

    PLAYWRIGHT_STEALTH_SYNC_AVAILABLE = True
except Exception:
    stealth_sync = None
    PLAYWRIGHT_STEALTH_SYNC_AVAILABLE = False

from .anti_bot import (
    MODERN_USER_AGENT,
    apply_stealth,
    google_domain_for_country,
    human_like_scroll,
    human_type,
    locale_for_country,
    normalize_country_code,
    random_delay,
    random_mouse_movements,
    random_user_agent,
)
from .models import Lead


_SHARED_BROWSER_LOCK = Lock()
_BROWSER_LAUNCH_LOCK = Lock()
_SHARED_PLAYWRIGHT = None
_SHARED_BROWSER = None
_SHARED_BROWSER_HEADLESS: Optional[bool] = None

# Use a stable, realistic UA to reduce Google anti-bot challenge frequency.
FORCED_WINDOWS_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


class GoogleMapsScraper:
    def __init__(
        self,
        headless: bool = True,
        user_agent: str = MODERN_USER_AGENT,
        country: Optional[str] = None,
        country_code: str = "us",
        user_data_dir: str = "profiles/maps_profile",
        proxy_url: Optional[str] = None,
        proxy_urls: Optional[List[str]] = None,
    ) -> None:
        self.headless = headless
        self.user_agent = user_agent
        self.country_code = normalize_country_code(country or country_code)
        self.google_domain = google_domain_for_country(self.country_code)
        self.locale = locale_for_country(self.country_code)
        self.user_data_dir = str(Path(user_data_dir))
        # Build rotation pool: prefer proxy_urls list, fall back to single proxy_url
        pool = [p.strip() for p in (proxy_urls or []) if str(p or "").strip()]
        if not pool and proxy_url:
            pool = [proxy_url.strip()]
        self._proxy_pool: List[str] = pool
        self._proxy_pool_index: int = 0
        self.proxy_url: str = self._next_proxy()
        self._playwright = None
        self._browser = None
        self._context = None
        self.page: Optional[Page] = None
        self._using_shared_browser = False

    @staticmethod
    def _should_abort_resource(request) -> bool:
        resource_type = str(getattr(request, "resource_type", "") or "").lower()
        if resource_type in {"image", "media", "font"}:
            return True

        if str(os.environ.get("SCRAPE_BLOCK_STYLESHEETS", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}:
            if resource_type == "stylesheet":
                return True

        return False

    def _next_proxy(self) -> str:
        """Return the next proxy from the rotation pool (round-robin)."""
        if not self._proxy_pool:
            return ""
        proxy = self._proxy_pool[self._proxy_pool_index % len(self._proxy_pool)]
        self._proxy_pool_index += 1
        return proxy

    def _country_hint(self) -> str:
        mapping = {
            "us": "United States",
            "uk": "United Kingdom",
            "gb": "United Kingdom",
            "de": "Germany",
            "at": "Austria",
            "ch": "Switzerland",
            "fr": "France",
            "it": "Italy",
            "es": "Spain",
            "nl": "Netherlands",
            "pl": "Poland",
            "hr": "Croatia",
            "rs": "Serbia",
            "si": "Slovenia",
            "be": "Belgium",
            "se": "Sweden",
            "no": "Norway",
            "dk": "Denmark",
            "fi": "Finland",
            "cz": "Czech Republic",
            "sk": "Slovakia",
            "hu": "Hungary",
            "ro": "Romania",
            "bg": "Bulgaria",
            "gr": "Greece",
            "pt": "Portugal",
            "ie": "Ireland",
            "lt": "Lithuania",
            "lv": "Latvia",
            "ee": "Estonia",
            "ua": "Ukraine",
            "tr": "Turkey",
            "ru": "Russia",
            "ca": "Canada",
            "mx": "Mexico",
            "br": "Brazil",
            "ar": "Argentina",
            "cl": "Chile",
            "co": "Colombia",
            "au": "Australia",
            "nz": "New Zealand",
            "in": "India",
            "cn": "China",
            "jp": "Japan",
            "kr": "South Korea",
            "sg": "Singapore",
            "ae": "United Arab Emirates",
            "sa": "Saudi Arabia",
            "il": "Israel",
            "za": "South Africa",
            "ng": "Nigeria",
        }
        return mapping.get(self.country_code, self.country_code.upper())

    def _compose_search_query(self, keyword: str) -> str:
        base = str(keyword or "").strip()
        hint = self._country_hint()
        if not base:
            return hint
        if hint.lower() in base.lower():
            return base
        return f"{base} in {hint}"

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def start(self) -> None:
        # Keep a stable UA instead of rotating per session to avoid anti-bot challenges.
        self.user_agent = str(os.environ.get("SCRAPE_USER_AGENT", FORCED_WINDOWS_CHROME_UA) or FORCED_WINDOWS_CHROME_UA).strip()

        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-gpu",
        ]
        proxy_config = None
        if self.proxy_url:
            proxy_config = {"server": self.proxy_url}
            logging.info("Scraper: using proxy %s", self.proxy_url.split("@")[-1])

        warm_enabled = str(os.environ.get("SCRAPE_WARM_BROWSER", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        can_use_shared = bool(self.headless and warm_enabled and proxy_config is None)

        if can_use_shared:
            self._browser = self._acquire_shared_browser(headless=self.headless, launch_args=launch_args)
            self._using_shared_browser = self._browser is not None

        if self._browser is None:
            self._playwright = sync_playwright().start()
            launch_timeout_ms = max(10000, int(os.environ.get("SCRAPE_BROWSER_LAUNCH_TIMEOUT_MS", "30000") or "30000"))
            try:
                with _BROWSER_LAUNCH_LOCK:
                    self._browser = self._playwright.chromium.launch(
                        headless=self.headless,
                        args=launch_args,
                        proxy=proxy_config,
                        timeout=launch_timeout_ms,
                    )
            except Exception as exc:
                logging.exception("Playwright browser launch failed: %s", exc)
                raise RuntimeError(f"Browser launch failed after {launch_timeout_ms}ms: {exc}")
            self._using_shared_browser = False

        context_kwargs: dict = {
            "user_agent": self.user_agent,
            "viewport": {"width": 1366, "height": 768},
            "locale": self.locale,
            "extra_http_headers": {
                "Accept-Language": "en-US,en;q=0.9",
                "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not.A/Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "Upgrade-Insecure-Requests": "1",
            },
        }
        if proxy_config:
            context_kwargs["proxy"] = proxy_config

        self._context = self._browser.new_context(**context_kwargs)
        if self.headless:
            # In headless mode, block heavy assets to reduce bandwidth and page load time.
            self._context.route(
                "**/*",
                lambda route, request: route.abort() if self._should_abort_resource(request) else route.continue_(),
            )
        self.page = self._context.new_page()
        self.page.set_default_timeout(6000)
        self.page.set_default_navigation_timeout(max(10000, int(os.environ.get("SCRAPE_NAV_TIMEOUT_MS", "30000") or "30000")))
        self._apply_stealth()
        # Skip eager homepage navigation here; scrape() opens target Maps URL directly.

    @classmethod
    def _acquire_shared_browser(cls, *, headless: bool, launch_args: list[str]):
        global _SHARED_PLAYWRIGHT, _SHARED_BROWSER, _SHARED_BROWSER_HEADLESS
        with _SHARED_BROWSER_LOCK:
            if _SHARED_BROWSER is not None and _SHARED_BROWSER_HEADLESS == headless:
                return _SHARED_BROWSER

            try:
                if _SHARED_BROWSER is not None:
                    _SHARED_BROWSER.close()
            except Exception:
                pass
            try:
                if _SHARED_PLAYWRIGHT is not None:
                    _SHARED_PLAYWRIGHT.stop()
            except Exception:
                pass

            _SHARED_PLAYWRIGHT = sync_playwright().start()
            launch_timeout_ms = max(10000, int(os.environ.get("SCRAPE_BROWSER_LAUNCH_TIMEOUT_MS", "30000") or "30000"))
            with _BROWSER_LAUNCH_LOCK:
                _SHARED_BROWSER = _SHARED_PLAYWRIGHT.chromium.launch(
                    headless=headless,
                    args=launch_args,
                    timeout=launch_timeout_ms,
                )
            _SHARED_BROWSER_HEADLESS = headless
            logging.info("Warm shared scraper browser initialized.")
            return _SHARED_BROWSER

    @classmethod
    def warm_browser(cls, *, headless: bool = True) -> None:
        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-gpu",
        ]
        cls._acquire_shared_browser(headless=headless, launch_args=launch_args)

    def _restart_with_new_identity(self) -> None:
        """Close the current session and open a fresh one with a new UA and next proxy.
        Called automatically when a 403/429 block is detected.
        """
        logging.warning("Block detected — restarting browser session with new User-Agent and proxy.")
        # Advance to next proxy in pool for the restarted session
        self.proxy_url = self._next_proxy()
        try:
            self.close()
        except Exception:
            pass
        time.sleep(2)
        self.start()

    def close(self) -> None:
        # Playwright can already be closed by the page/site flow. Do not fail the whole task on cleanup.
        if self._context:
            try:
                self._context.close()
            except Exception:
                logging.debug("Browser context already closed during cleanup.")
            finally:
                self._context = None
        if self._browser:
            if not self._using_shared_browser:
                try:
                    self._browser.close()
                except Exception:
                    logging.debug("Browser already closed during cleanup.")
            self._browser = None
        if self._playwright:
            try:
                self._playwright.stop()
            except Exception:
                logging.debug("Playwright already stopped during cleanup.")
            finally:
                self._playwright = None

    def _apply_stealth(self) -> None:
        assert self.page is not None

        if PLAYWRIGHT_STEALTH_SYNC_AVAILABLE and stealth_sync is not None:
            try:
                stealth_sync(self.page)
                return
            except Exception:
                logging.warning("playwright_stealth.stealth_sync failed, trying fallback stealth adapter.")

        apply_stealth(self.page)

    def _handle_startup_consent(self) -> None:
        assert self.page is not None

        try:
            self.page.goto(f"https://{self.google_domain}", wait_until="domcontentloaded")
            self._accept_consent_if_present()
        except Exception:
            logging.debug("Startup consent check skipped due to navigation issue.")

    def scrape(
        self,
        keyword: str,
        max_results: int,
        progress_callback: Optional[Callable[[int, int, int, Optional[Lead]], None]] = None,
        max_runtime_seconds: int = 300,
        stall_timeout_seconds: int = 45,
    ) -> List[Lead]:
        if not self.page:
            raise RuntimeError("Scraper not started. Use with-context or call start().")

        self._open_maps_and_search(keyword)
        # Small human-like idle before first interaction/scroll.
        random_delay(1000, 3000)
        leads: List[Lead] = []
        seen_cards = set()
        stalled_rounds = 0
        scanned_count = 0
        started_at = time.monotonic()
        last_progress_at = started_at

        while len(leads) < max_results and stalled_rounds < 8:
            elapsed_seconds = time.monotonic() - started_at
            if elapsed_seconds >= max(30, int(max_runtime_seconds or 0)):
                logging.warning(
                    "Stopping Maps scrape after runtime limit: found=%s target=%s scanned=%s elapsed=%ss",
                    len(leads),
                    max_results,
                    scanned_count,
                    int(elapsed_seconds),
                )
                break

            cards = self.page.locator("div.Nv2PK, div[role='article']")
            count = cards.count()
            before_seen = len(seen_cards)

            for idx in range(count):
                if len(leads) >= max_results:
                    break

                card = cards.nth(idx)
                card_key = self._card_key(card, idx)
                if card_key in seen_cards:
                    continue

                seen_cards.add(card_key)
                scanned_count += 1
                last_progress_at = time.monotonic()
                if progress_callback is not None:
                    try:
                        progress_callback(len(leads), max_results, scanned_count, None)
                    except Exception:
                        logging.debug("Progress callback failed during card scan; continuing scrape.")
                if not self._open_card(card):
                    continue

                lead = self._extract_business(keyword)
                if not lead:
                    continue

                if lead.business_name and lead.address:
                    leads.append(lead)
                    last_progress_at = time.monotonic()
                    if progress_callback is not None:
                        try:
                            progress_callback(len(leads), max_results, scanned_count, lead)
                        except Exception:
                            logging.debug("Progress callback failed; continuing scrape.")

                random_mouse_movements(self.page, count=random.randint(2, 4))
                random_delay(300, 900)

            if len(seen_cards) == before_seen:
                stalled_rounds += 1
                stall_elapsed = time.monotonic() - last_progress_at
                logging.info(
                    "Maps scrape stalled round=%s/%s found=%s target=%s scanned=%s idle=%ss",
                    stalled_rounds,
                    8,
                    len(leads),
                    max_results,
                    scanned_count,
                    int(stall_elapsed),
                )

                if progress_callback is not None:
                    try:
                        progress_callback(len(leads), max_results, scanned_count, None)
                    except Exception:
                        logging.debug("Progress callback failed during stalled round heartbeat.")

                if stall_elapsed >= max(10, int(stall_timeout_seconds or 0)):
                    logging.warning(
                        "Stopping Maps scrape after stall timeout: found=%s target=%s scanned=%s idle=%ss",
                        len(leads),
                        max_results,
                        scanned_count,
                        int(stall_elapsed),
                    )
                    break
            else:
                stalled_rounds = 0

            if len(leads) < max_results:
                panel = self.page.locator("div[role='feed']").first
                try:
                    human_like_scroll(panel, steps=random.randint(2, 4))
                except PlaywrightTimeoutError:
                    logging.warning("Could not scroll results panel in this round.")

        return leads[:max_results]

    def _is_blocked_page(self) -> bool:
        """Return True if Google is showing a CAPTCHA, 403, or rate-limit page."""
        assert self.page is not None
        try:
            url = self.page.url or ""
            title = self.page.title() or ""
            content = self.page.content() or ""
        except Exception:
            return False
        block_signals = [
            "sorry/index" in url,
            "recaptcha" in url.lower(),
            "unusual traffic" in content.lower(),
            "captcha" in content.lower(),
            "403" in title,
            "429" in title,
        ]
        return any(block_signals)

    def _goto_with_retry(
        self,
        url: str,
        wait_until: str = "domcontentloaded",
        max_retries: int = 1,
        timeout_ms: int = 30000,
    ) -> None:
        """Navigate to `url`, detect blocks, and restart session on 403/429 before retrying."""
        assert self.page is not None
        for attempt in range(max_retries + 1):
            try:
                self.page.goto(url, wait_until=wait_until, timeout=max(5000, int(timeout_ms or 30000)))
            except PlaywrightTimeoutError:
                logging.warning("Navigation timeout for %s (attempt %d)", url, attempt + 1)

            if self._is_consent_page():
                self._handle_consent_gate(fallback_url=url)

            if not self._is_blocked_page():
                return

            if attempt < max_retries:
                logging.warning("Block detected on %s — restarting session (attempt %d/%d)", url, attempt + 1, max_retries)
                self._restart_with_new_identity()
                time.sleep(1.2 * (attempt + 1))
            else:
                logging.error("Still blocked after %d retries for %s — continuing anyway.", max_retries, url)

    def _is_consent_page(self) -> bool:
        assert self.page is not None
        try:
            current_url = str(self.page.url or "").lower()
            content = (self.page.content() or "").lower()
        except Exception:
            return False

        if "consent.google.com" in current_url:
            return True
        if "before you continue" in content and "google" in content:
            return True
        return False

    def _extract_consent_continue_url(self) -> str:
        assert self.page is not None
        try:
            parsed = urlparse(str(self.page.url or "").strip())
            query = parse_qs(parsed.query)
            values = query.get("continue") or []
            if not values:
                return ""
            candidate = unquote(str(values[0] or "").strip())
            if candidate.startswith("/"):
                candidate = f"https://www.google.com{candidate}"
            return candidate
        except Exception:
            return ""

    def _handle_consent_gate(self, fallback_url: Optional[str] = None) -> bool:
        assert self.page is not None
        if not self._is_consent_page():
            return True

        for _ in range(3):
            clicked = self._accept_consent_if_present()
            random_delay(450, 1200)

            if not self._is_consent_page():
                return True

            continue_url = self._extract_consent_continue_url() or str(fallback_url or "").strip()
            if continue_url:
                try:
                    self.page.goto(continue_url, wait_until="domcontentloaded", timeout=25000)
                    random_delay(350, 1000)
                except Exception:
                    pass

            if not self._is_consent_page():
                return True

            if not clicked and not continue_url:
                break

        self._capture_debug_screenshot()
        logging.error("Consent page could not be resolved automatically.")
        return False

    def _open_maps_and_search(self, keyword: str) -> None:
        assert self.page is not None
        search_query = self._compose_search_query(keyword)

        # High-priority path: inject query directly into Maps URL (no typing/clicking search box).
        if self._search_via_url_candidates(search_query):
            logging.info("Direct Maps URL search opened successfully.")
            return

        # Last-resort fallback: attempt classic search-box flow if URL mode fails.
        logging.warning("Direct Maps URL strategies failed; trying search-box fallback flow.")
        if self._search_via_searchbox_flow(search_query):
            logging.info("Search-box fallback flow opened Maps results successfully.")
            return

        diagnostic = self._build_page_diagnostic()
        raise RuntimeError(f"Google Maps results did not load via URL or search-box flow. {diagnostic}")

    def _search_via_url_candidates(self, keyword: str) -> bool:
        assert self.page is not None

        cc = (self.country_code or "us").lower()
        encoded = quote_plus(keyword)
        url_candidates = [
            f"https://www.google.com/maps/search/{encoded}?hl=en&gl={cc}",
            f"https://www.google.com/maps/search/{encoded}",
            f"https://www.google.com/maps?hl=en&gl={cc}&q={encoded}",
            f"https://{self.google_domain}/maps/search/{encoded}",
        ]

        for url in url_candidates:
            try:
                logging.info("Trying Maps URL candidate: %s", url)
                self._goto_with_retry(url)
                self._accept_consent_if_present()
                try:
                    self.page.wait_for_load_state("domcontentloaded", timeout=8000)
                except Exception:
                    pass
                random_delay(900, 1700)
                if self._wait_for_any_result_signal(timeout_ms=20000):
                    return True
            except Exception as exc:
                logging.warning("Maps URL candidate failed: %s | reason=%s", url, exc)

        return False

    def _search_via_searchbox_flow(self, keyword: str) -> bool:
        assert self.page is not None

        base_urls = [
            f"https://www.google.com/maps?hl=en&gl={(self.country_code or 'us').lower()}",
            f"https://{self.google_domain}/maps",
        ]
        for base_url in base_urls:
            try:
                self._goto_with_retry(base_url)
                self._accept_consent_if_present()
                random_mouse_movements(self.page, count=3)
                random_delay(500, 1200)

                search_box = self._get_search_box()
                if search_box is None:
                    continue

                search_box.click()
                search_box.fill("")
                human_type(search_box, keyword)
                random_delay(280, 750)
                search_box.press("Enter")
                random_delay(1000, 1900)
                self._accept_consent_if_present()

                if self._wait_for_any_result_signal(timeout_ms=20000):
                    return True
            except Exception as exc:
                logging.warning("Search-box fallback failed on %s: %s", base_url, exc)

        return False

    def _search_via_fallback_url(self, keyword: str) -> bool:
        assert self.page is not None

        # Always prefer canonical www.google.com Maps search path for stability.
        fallback_url = f"https://www.google.com/maps/search/{quote_plus(keyword)}"
        self._goto_with_retry(fallback_url)
        self._accept_consent_if_present()
        random_delay(1000, 1800)

        try:
            self.page.wait_for_load_state("domcontentloaded", timeout=8000)
        except Exception:
            pass

        return self._wait_for_any_result_signal(timeout_ms=20000)

    def _build_page_diagnostic(self) -> str:
        assert self.page is not None
        current_url = str(self.page.url or "").strip()
        try:
            title = str(self.page.title() or "").strip()
        except Exception:
            title = ""
        screenshot_path = self._capture_debug_screenshot()
        return f"url={current_url!r} title={title!r} screenshot={screenshot_path!r}"

    def _capture_debug_screenshot(self) -> str:
        assert self.page is not None
        target_path = str(os.environ.get("SCRAPE_DEBUG_SCREENSHOT_PATH", "debug_screenshot.png") or "debug_screenshot.png").strip()
        if not target_path:
            target_path = "debug_screenshot.png"
        try:
            self.page.screenshot(path=target_path, full_page=True)
            logging.error("Saved scraper debug screenshot: %s", target_path)
            return target_path
        except Exception as exc:
            logging.error("Failed to save scraper debug screenshot (%s): %s", target_path, exc)
            return ""

    def _wait_for_any_result_signal(self, timeout_ms: int = 20000) -> bool:
        """Wait for robust Maps result signals and fail fast on challenge pages."""
        assert self.page is not None

        started = time.monotonic()
        timeout_seconds = max(5.0, float(timeout_ms) / 1000.0)
        results_selectors = [
            "div[role='article']",
            "div.Nv2PK",
            "div[role='feed'] div.Nv2PK",
            "a[href*='/maps/place/']",
        ]
        phone_selectors = [
            "a[href^='tel:']",
            "a[href*='tel:']",
            "button[data-item-id^='phone:tel:']",
        ]

        challenge_tokens = [
            "unusual traffic",
            "verify you are human",
            "recaptcha",
            "sorry/index",
            "detected unusual traffic",
            "press and hold",
        ]

        while (time.monotonic() - started) < timeout_seconds:
            try:
                url = str(self.page.url or "").lower()
                content = (self.page.content() or "").lower()

                if "consent.google.com" in url:
                    if self._handle_consent_gate():
                        continue
                    return False

                if any(token in url or token in content for token in challenge_tokens):
                    self._capture_debug_screenshot()
                    logging.error("Google challenge page detected while waiting for Maps results.")
                    return False

                for selector in results_selectors:
                    if self.page.locator(selector).count() > 0:
                        return True
                for selector in phone_selectors:
                    if self.page.locator(selector).count() > 0:
                        return True
            except Exception:
                pass

            random_delay(250, 550)

        screenshot_path = self._capture_debug_screenshot()
        logging.error(
            "No Maps results detected after %sms. screenshot=%s url=%s",
            int(timeout_ms),
            screenshot_path or "<none>",
            str(self.page.url or ""),
        )
        return False

    def _get_search_box(self):
        assert self.page is not None

        selectors = [
            "input#searchboxinput",
            "input[name='q']",
            "input[aria-label*='Search Google Maps']",
            "input[aria-label*='Search']",
            "input[placeholder*='Search']",
        ]

        for selector in selectors:
            locator = self.page.locator(selector).first
            try:
                locator.wait_for(state="visible", timeout=2500)
                return locator
            except PlaywrightTimeoutError:
                continue

        return None

    def _accept_consent_if_present(self) -> bool:
        assert self.page is not None

        text_patterns = [
            "Accept all",
            "Accept all cookies",
            "I agree",
            "Agree",
            "Accept",
            "Akzeptieren",
            "Alle akzeptieren",
            "Akzeptiere alle",
            "Zustimmen",
            "Strinjam",
            "Sprejmi",
            "Sprejmi vse",
            "Aceptar",
            "Aceptar todo",
            "Acepto",
            "Accepter",
            "Tout accepter",
            "Accetta",
            "Accetta tutto",
            "Souhlasim",
            "Prihvatam",
            "Slažem se",
            "Allow all",
        ]

        selectors = [
            "button#L2AGLb",
            "button#introAgreeButton",
            "button[jsname='higCR']",
            "button[jscontroller]",
            "button[aria-label*='Sprejmi']",
            "button[aria-label*='vse']",
            "button[aria-label*='Accept']",
            "button[aria-label*='Agree']",
            "button[aria-label*='Akzeptieren']",
            "button[aria-label*='Strinjam']",
            "button[aria-label*='Consent']",
            "form [type='submit']",
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
            "button:has-text('Akzeptieren')",
            "button:has-text('Alle akzeptieren')",
            "button:has-text('Strinjam')",
            "button:has-text('Sprejmi')",
            "button:has-text('Sprejmi vse')",
            "button:has-text('Aceptar todo')",
            "button:has-text('Tout accepter')",
            "button:has-text('Accetta tutto')",
            "[role='button']:has-text('Sprejmi vse')",
        ]

        for _ in range(2):
            scopes = [self.page.main_frame, *self.page.frames]
            for scope in scopes:
                for selector in selectors:
                    try:
                        button = scope.locator(selector).first
                        if button.count() > 0 and button.is_visible(timeout=200):
                            button.click(timeout=1200)
                            random_delay(350, 900)
                            logging.info("Accepted Google consent prompt via selector.")
                            return True
                    except Exception:
                        continue

                for pattern in text_patterns:
                    try:
                        button = scope.get_by_role("button", name=re.compile(pattern, re.IGNORECASE)).first
                        if button.count() > 0 and button.is_visible(timeout=200):
                            button.click(timeout=1200)
                            random_delay(350, 900)
                            logging.info("Accepted Google consent prompt via button role.")
                            return True
                    except Exception:
                        continue

                    try:
                        link_button = scope.get_by_role("link", name=re.compile(pattern, re.IGNORECASE)).first
                        if link_button.count() > 0 and link_button.is_visible(timeout=200):
                            link_button.click(timeout=1200)
                            random_delay(350, 900)
                            logging.info("Accepted Google consent prompt via link role.")
                            return True
                    except Exception:
                        continue

                for pattern in text_patterns:
                    try:
                        generic = scope.locator(f"text=/{pattern}/i").first
                        if generic.count() > 0 and generic.is_visible(timeout=200):
                            generic.click(timeout=1200)
                            random_delay(350, 900)
                            logging.info("Accepted Google consent prompt via text locator.")
                            return True
                    except Exception:
                        continue

            random_delay(300, 800)

        return False

    def _open_card(self, card) -> bool:
        assert self.page is not None

        try:
            card.scroll_into_view_if_needed(timeout=3000)
            random_delay(120, 450)
            card.click(timeout=3000)
        except PlaywrightTimeoutError:
            try:
                card.locator("a.hfpxzc").first.click(timeout=3000)
            except PlaywrightTimeoutError:
                return False

        random_delay(800, 1600)

        try:
            self.page.locator("h1.DUwDvf").first.wait_for(state="visible", timeout=5000)
        except PlaywrightTimeoutError:
            return False

        return True

    def _extract_business(self, keyword: str) -> Optional[Lead]:
        assert self.page is not None

        name = self._safe_text(self.page.locator("h1.DUwDvf").first)
        address = self._extract_address()

        if not name or not address:
            return None

        website = self._extract_website() or "None"
        phone = self._extract_phone() or "None"

        rating_text = self._safe_text(self.page.locator("span.MW4etd").first)
        if not rating_text:
            rating_text = self._safe_text(self.page.locator("div.F7nice span").first)

        review_text = self._safe_text(self.page.locator("span.UY7F9").first)
        if not review_text:
            review_text = self._safe_text(
                self.page.locator("button[jsaction*='pane.reviewChart.moreReviews'] span").first
            )

        return Lead(
            business_name=name,
            website_url=website,
            phone_number=phone,
            rating=self._parse_float(rating_text),
            review_count=self._parse_int(review_text),
            address=address,
            search_keyword=keyword,
            google_claimed=self._extract_google_claimed_status(),
        )

    def _extract_google_claimed_status(self) -> Optional[bool]:
        assert self.page is not None

        claim_selectors = [
            "button:has-text('Claim this business')",
            "button:has-text('Own this business?')",
            "button:has-text('Is this your business?')",
            "text='Claim this business'",
            "text='Own this business?'",
            "text='Is this your business?'",
        ]

        for selector in claim_selectors:
            try:
                locator = self.page.locator(selector).first
                if locator.count() > 0 and locator.is_visible(timeout=500):
                    return False
            except Exception:
                continue

        try:
            content = (self.page.content() or "").lower()
        except Exception:
            content = ""

        if any(token in content for token in ["claim this business", "own this business", "is this your business"]):
            return False
        return True

    def _extract_website(self) -> Optional[str]:
        assert self.page is not None

        website_link = self.page.locator("a[data-item-id='authority']").first
        if website_link.count() > 0:
            href = website_link.get_attribute("href")
            if href:
                return href.strip()

        alternative = self.page.locator("a[aria-label*='Website'], a:has-text('Website')").first
        if alternative.count() > 0:
            href = alternative.get_attribute("href")
            if href:
                return href.strip()

        return None

    def _extract_phone(self) -> Optional[str]:
        assert self.page is not None

        candidates = [
            self.page.locator("button[data-item-id^='phone:tel:']").first,
            self.page.locator("button[aria-label^='Phone:']").first,
            self.page.locator("button[data-tooltip='Copy phone number']").first,
        ]

        for locator in candidates:
            if locator.count() == 0:
                continue

            label = locator.get_attribute("aria-label") or ""
            text = self._safe_text(locator) or ""
            blob = f"{label} {text}".strip()

            match = re.search(r"\+?[\d][\d\s().-]{6,}", blob)
            if match:
                return match.group(0).strip()

        return None

    def _extract_address(self) -> Optional[str]:
        assert self.page is not None

        address_button = self.page.locator("button[data-item-id='address']").first
        if address_button.count() > 0:
            label = address_button.get_attribute("aria-label")
            if label and "Address:" in label:
                return label.split("Address:", 1)[1].strip()

            text = self._safe_text(address_button)
            if text:
                return text

        return None

    def _card_key(self, card, idx: int) -> str:
        link = None
        title = None

        try:
            link = card.locator("a.hfpxzc").first.get_attribute("href")
        except PlaywrightTimeoutError:
            link = None

        try:
            title = self._safe_text(card.locator(".qBF1Pd").first)
        except PlaywrightTimeoutError:
            title = None

        return link or title or f"card-{idx}"

    @staticmethod
    def _safe_text(locator) -> Optional[str]:
        try:
            text = locator.inner_text(timeout=2000)
        except PlaywrightTimeoutError:
            return None

        text = text.strip()
        return text or None

    @staticmethod
    def _parse_float(value: Optional[str]) -> Optional[float]:
        if not value:
            return None

        match = re.search(r"\d+[.,]?\d*", value)
        if not match:
            return None

        try:
            return float(match.group(0).replace(",", "."))
        except ValueError:
            return None

    @staticmethod
    def _parse_int(value: Optional[str]) -> Optional[int]:
        if not value:
            return None

        digits = re.sub(r"[^\d]", "", value)
        if not digits:
            return None

        try:
            return int(digits)
        except ValueError:
            return None
