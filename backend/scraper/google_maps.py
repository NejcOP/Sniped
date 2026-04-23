import logging
import os
import random
import re
import time
import json
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
GOOGLE_CONSENT_COOKIE = "YES+cb.20240101-00-p0.en+FX+123"


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
        pool = [self._normalize_proxy_url(p) for p in (proxy_urls or []) if self._normalize_proxy_url(p)]
        if not pool and proxy_url:
            pool = [self._normalize_proxy_url(proxy_url)]
        random.shuffle(pool)
        self._proxy_pool: List[str] = pool
        self._proxy_pool_index: int = 0
        self.proxy_url: str = self._next_proxy()
        self._playwright = None
        self._browser = None
        self._context = None
        self.page: Optional[Page] = None
        self._using_shared_browser = False
        self._first_result_snapshot_written = False

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

    @staticmethod
    def _normalize_proxy_url(raw_proxy: Optional[str]) -> str:
        value = str(raw_proxy or "").strip()
        if not value:
            return ""
        if "://" in value:
            return value

        parts = value.split(":")
        if len(parts) == 4:
            host, port, username, password = [part.strip() for part in parts]
            if host and port and username and password:
                return f"http://{username}:{password}@{host}:{port}"
        return value

    @staticmethod
    def _playwright_proxy_config(proxy_url: Optional[str]) -> Optional[dict]:
        value = str(proxy_url or "").strip()
        if not value:
            return None
        parsed = urlparse(value)
        if not parsed.scheme or not parsed.hostname or not parsed.port:
            return {"server": value}

        config = {"server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"}
        if parsed.username:
            config["username"] = unquote(parsed.username)
        if parsed.password:
            config["password"] = unquote(parsed.password)
        return config

    def _reset_launch_state(self) -> None:
        if self._context:
            try:
                self._context.close()
            except Exception:
                pass
            finally:
                self._context = None
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            finally:
                self._browser = None
        self.page = None

    def _initialize_context_and_page(self, proxy_config: Optional[dict], nav_timeout_ms: int) -> None:
        assert self._browser is not None

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
        self._seed_google_consent_cookies()
        if self.headless:
            self._context.route(
                "**/*",
                lambda route, request: route.abort() if self._should_abort_resource(request) else route.continue_(),
            )
        self.page = self._context.new_page()
        self.page.set_default_timeout(12000)
        self.page.set_default_navigation_timeout(nav_timeout_ms)
        self._apply_stealth()
        self._prime_google_consent_state()

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
        self.headless = True

        launch_args = [
            "--disable-blink-features=AutomationControlled",
            "--no-default-browser-check",
            "--disable-extensions",
            "--disable-gpu",
            "--no-sandbox",
        ]
        proxy_candidates = [candidate for candidate in [self.proxy_url, *self._proxy_pool] if str(candidate or "").strip()]
        if proxy_candidates:
            deduped_candidates: list[str] = []
            seen_candidates: set[str] = set()
            for candidate in proxy_candidates:
                if candidate in seen_candidates:
                    continue
                seen_candidates.add(candidate)
                deduped_candidates.append(candidate)
            proxy_candidates = deduped_candidates

        warm_enabled = str(os.environ.get("SCRAPE_WARM_BROWSER", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        can_use_shared = bool(self.headless and warm_enabled and not proxy_candidates)

        if can_use_shared:
            self._browser = self._acquire_shared_browser(headless=self.headless, launch_args=launch_args)
            self._using_shared_browser = self._browser is not None

        if self._browser is None:
            self._playwright = sync_playwright().start()
            launch_timeout_ms = max(5000, int(os.environ.get("SCRAPE_PROXY_LAUNCH_TIMEOUT_MS", "15000") or "15000"))
            nav_timeout_ms = max(5000, int(os.environ.get("SCRAPE_PROXY_NAV_TIMEOUT_MS", "15000") or "15000"))
            launch_targets = proxy_candidates or [""]
            last_exc: Optional[Exception] = None
            for proxy_candidate in launch_targets:
                proxy_config = self._playwright_proxy_config(proxy_candidate)
                if proxy_candidate:
                    self.proxy_url = proxy_candidate
                    logging.info("Scraper: using proxy %s", proxy_candidate.split("@")[-1])
                try:
                    with _BROWSER_LAUNCH_LOCK:
                        self._browser = self._playwright.chromium.launch(
                            headless=self.headless,
                            args=launch_args,
                            proxy=proxy_config,
                            timeout=launch_timeout_ms,
                        )
                    self._initialize_context_and_page(proxy_config, nav_timeout_ms)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    self._reset_launch_state()
                    if proxy_candidate:
                        logging.warning("Proxy launch failed for %s: %s", proxy_candidate.split("@")[-1], exc)
                    else:
                        logging.exception("Playwright browser launch failed without proxy: %s", exc)
            if self._browser is None:
                raise RuntimeError(f"Browser launch failed after trying {len(launch_targets)} proxy option(s): {last_exc}")
            self._using_shared_browser = False
        if self._browser is not None and self.page is None:
            self._initialize_context_and_page(None, max(5000, int(os.environ.get("SCRAPE_NAV_TIMEOUT_MS", "15000") or "15000")))
        # Skip eager homepage navigation here; scrape() opens target Maps URL directly.

    def _seed_google_consent_cookies(self) -> None:
        assert self._context is not None
        cookie_domains = [
            ".google.com",
            "www.google.com",
            "consent.google.com",
        ]
        cookies = []
        for domain in cookie_domains:
            cookies.append(
                {
                    "name": "CONSENT",
                    "value": GOOGLE_CONSENT_COOKIE,
                    "domain": domain,
                    "path": "/",
                    "httpOnly": False,
                    "secure": True,
                    "sameSite": "None",
                }
            )
        try:
            self._context.add_cookies(cookies)
            logging.info("Injected Google CONSENT cookie into browser context.")
        except Exception as exc:
            logging.warning("Failed to inject Google CONSENT cookie: %s", exc)

    def _prime_google_consent_state(self) -> None:
        assert self.page is not None
        warmup_urls = [
            "https://www.google.com/?hl=en",
            "https://consent.google.com/?hl=en",
        ]
        for warmup_url in warmup_urls:
            try:
                self.page.goto(warmup_url, wait_until="domcontentloaded", timeout=15000)
                self._accept_consent_if_present()
                random_delay(350, 900)
                if not self._is_consent_page():
                    return
            except Exception:
                continue

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
        last_keep_alive_at = started_at

        while len(leads) < max_results and stalled_rounds < 8:
            now = time.monotonic()
            if now - last_keep_alive_at >= 5.0:
                keepalive = (
                    f"KEEP_ALIVE maps-scrape found={len(leads)} target={max_results} "
                    f"scanned={scanned_count} elapsed={int(now - started_at)}s"
                )
                print(keepalive, flush=True)
                logging.info(keepalive)
                last_keep_alive_at = now

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

            if scanned_count == 0:
                self._log_results_container_preview()

            cards = self.page.locator("a[href*='/maps/place/']")
            count = cards.count()
            before_seen = len(seen_cards)

            for idx in range(count):
                if len(leads) >= max_results:
                    break

                card = cards.nth(idx)
                card_key = self._card_key(card, idx)
                if card_key in seen_cards:
                    continue

                card_title = self._card_title(card)
                logging.info("Scanned business candidate idx=%s title=%r", idx, card_title)

                seen_cards.add(card_key)
                scanned_count += 1
                last_progress_at = time.monotonic()
                if progress_callback is not None:
                    try:
                        progress_callback(len(leads), max_results, scanned_count, None)
                    except Exception:
                        logging.debug("Progress callback failed during card scan; continuing scrape.")
                if not self._open_card(card):
                    screenshot_path = self._capture_debug_screenshot(reason=f"open_card_failed_{card_title or idx}")
                    logging.warning(
                        "Open-card failed after click idx=%s title=%r url=%s screenshot=%s",
                        idx,
                        card_title,
                        str(self.page.url or ""),
                        screenshot_path or "<none>",
                    )
                    self._log_card_preview(card, idx, reason="open_card_failed")
                    continue

                lead = self._extract_business(keyword)
                if not lead:
                    self._log_card_preview(card, idx, reason="extract_business_empty")
                    self._dump_first_result_html(reason="extract_business_empty")
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
            f"https://www.google.com/maps/search/{encoded}?hl=en&authuser=0&gl={cc}",
            f"https://www.google.com/maps/search/{encoded}?hl=en&authuser=0",
            f"https://www.google.com/maps?hl=en&authuser=0&gl={cc}&q={encoded}",
            f"https://{self.google_domain}/maps/search/{encoded}?hl=en&authuser=0",
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
            f"https://www.google.com/maps?hl=en&authuser=0&gl={(self.country_code or 'us').lower()}",
            f"https://{self.google_domain}/maps?hl=en&authuser=0",
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
        screenshot_path = self._capture_debug_screenshot(reason="page_diagnostic")
        return f"url={current_url!r} title={title!r} screenshot={screenshot_path!r}"

    @staticmethod
    def _sanitize_debug_token(value: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_-]+", "_", str(value or "")).strip("_") or "debug"

    def _capture_debug_screenshot(self, reason: str = "debug") -> str:
        assert self.page is not None
        configured_dir = str(os.environ.get("SCRAPE_DEBUG_DIR", "debug") or "debug").strip() or "debug"
        debug_dir = Path(configured_dir)
        debug_dir.mkdir(parents=True, exist_ok=True)
        filename = f"maps_{self._sanitize_debug_token(reason)}_{int(time.time() * 1000)}.png"
        target_path = str(debug_dir / filename)
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
        timeout_seconds = max(10.0, float(timeout_ms) / 1000.0)
        results_selectors = [
            "div[role='article']",
            "div[role='feed'] a[href*='/maps/place/']",
            "a[href*='/maps/place/']",
            "text=/Directions/i",
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

        screenshot_path = self._capture_debug_screenshot(reason="maps_result_wait_timeout")
        logging.error(
            "No Maps results detected after %sms. screenshot=%s url=%s",
            int(timeout_ms),
            screenshot_path or "<none>",
            str(self.page.url or ""),
        )
        return False

    def _log_results_container_preview(self) -> None:
        assert self.page is not None
        containers = [
            "div[role='feed']",
            "div[role='main']",
            "body",
        ]
        for selector in containers:
            try:
                locator = self.page.locator(selector).first
                if locator.count() == 0:
                    continue
                html = locator.inner_html(timeout=2000)
                snippet = str(html or "").strip().replace("\n", " ").replace("\r", " ")[:500]
                if snippet:
                    logging.info("Maps results container preview (%s): %s", selector, snippet)
                    return
            except Exception:
                continue

    def _log_card_preview(self, card, idx: int, reason: str) -> None:
        try:
            html = card.inner_html(timeout=2000)
        except Exception:
            html = ""
        snippet = str(html or "").strip().replace("\n", " ").replace("\r", " ")[:500]
        logging.info("Maps card preview idx=%s reason=%s html=%s", idx, reason, snippet or "<empty>")

    def _dump_first_result_html(self, reason: str) -> None:
        if self._first_result_snapshot_written:
            return
        assert self.page is not None

        try:
            first_result = self.page.locator("a[href*='/maps/place/']").first
            if first_result.count() == 0:
                return
            html = first_result.evaluate("el => el.outerHTML")
            content = str(html or "").strip()
            if not content:
                return

            dump_dir = Path(str(os.environ.get("SCRAPE_DEBUG_DUMP_DIR", "runtime/logs") or "runtime/logs"))
            dump_dir.mkdir(parents=True, exist_ok=True)
            safe_reason = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(reason or "unknown")).strip("_") or "unknown"
            file_path = dump_dir / f"maps_first_result_{safe_reason}_{int(time.time())}.html"
            file_path.write_text(content, encoding="utf-8")
            self._first_result_snapshot_written = True
            logging.warning("Saved first-result HTML snapshot: %s", file_path)
        except Exception as exc:
            logging.warning("Failed to dump first-result HTML snapshot: %s", exc)

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
            "Accept everything",
            "I agree",
            "Agree",
            "Accept",
            "Yes, I'm in",
            "Continue",
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
            "Aceptar todo",
            "Tout accepter",
            "Zaakceptuj wszystko",
            "Aanvaard alles",
        ]

        selectors = [
            "button#L2AGLb",
            "button#introAgreeButton",
            "button[jsname='higCR']",
            "button[jscontroller]",
            "button[aria-label*='Accept all']",
            "button[aria-label*='I agree']",
            "button[aria-label*='Continue']",
            "button[aria-label*='Sprejmi']",
            "button[aria-label*='vse']",
            "button[aria-label*='Accept']",
            "button[aria-label*='Agree']",
            "button[aria-label*='Akzeptieren']",
            "button[aria-label*='Strinjam']",
            "button[aria-label*='Consent']",
            "[role='button'][aria-label*='Accept']",
            "[role='button'][aria-label*='agree']",
            "form [type='submit']",
            "input[type='submit']",
            "button:has-text('Accept all')",
            "button:has-text('Accept all cookies')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
            "button:has-text('Continue')",
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
                            button.scroll_into_view_if_needed(timeout=1000)
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

        for attempt in range(2):
            self._accept_consent_if_present()

            heading_before = self._panel_heading_text()
            panel_before = self._normalized_text(self._panel_text())
            url_before = str(self.page.url or "")

            # Step 1: Scroll element fully into view, including inner anchor.
            try:
                card.scroll_into_view_if_needed(timeout=3000)
            except Exception:
                pass
            try:
                anchor = card.locator("a[href*='/maps/place/']").first
                if anchor.count() > 0:
                    anchor.evaluate("el => el.scrollIntoView({block: 'center', inline: 'nearest'})")
                else:
                    card.evaluate("el => el.scrollIntoView({block: 'center', inline: 'nearest'})")
            except Exception:
                pass

            random_delay(400, 900)

            # Step 2: Click.
            try:
                anchor = card.locator("a[href*='/maps/place/']").first
                if anchor.count() > 0:
                    anchor.click(timeout=6000)
                else:
                    card.click(timeout=6000)
            except PlaywrightTimeoutError:
                self._accept_consent_if_present()
                try:
                    card.locator("a[href*='/maps/place/']").first.click(timeout=6000)
                except PlaywrightTimeoutError:
                    if attempt == 1:
                        return False
                    continue

            random_mouse_movements(self.page, count=random.randint(1, 2))

            # Step 3: Wait for a real panel state change, not just any visible main container.
            if self._wait_for_business_panel_ready(url_before, heading_before, panel_before, timeout_ms=12000):
                return True

            logging.warning("Business panel not ready after click attempt=%s url=%s", attempt, self.page.url)
            if attempt == 1:
                return False

        return False

    @staticmethod
    def _normalized_text(value: str) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def _panel_heading_text(self) -> str:
        assert self.page is not None

        heading_candidates = self.page.locator("div[role='main'] h1, div[role='main'] h2, div[role='main'] [role='heading']")
        try:
            total = min(heading_candidates.count(), 8)
        except Exception:
            total = 0

        blocked_terms = {"directions", "website", "call", "overview", "reviews", "photos", "about", "menu"}
        for idx in range(total):
            text = self._safe_text(heading_candidates.nth(idx))
            if not text:
                continue
            lowered = text.strip().lower()
            if lowered in blocked_terms or len(lowered) < 2:
                continue
            return text.strip()

        return ""

    @staticmethod
    def _find_phone_match(blob: str) -> Optional[str]:
        patterns = [
            re.compile(r"(?:\+?\d{1,3}[\s().-]*)?(?:\(?\d{2,4}\)?[\s().-]*){2,}\d{2,4}"),
            re.compile(r"\+?[\d][\d\s().-]{6,}"),
        ]
        for pattern in patterns:
            match = pattern.search(str(blob or ""))
            if match:
                return match.group(0).strip()
        return None

    def _collect_panel_metadata(self, limit: int = 30) -> List[str]:
        assert self.page is not None

        values: List[str] = []
        seen: set[str] = set()
        selectors = [
            "div[role='main'] [aria-label]",
            "div[role='main'] [data-item-id]",
            "div[role='main'] a[href]",
            "div[role='main'] button",
        ]
        for selector in selectors:
            try:
                locator = self.page.locator(selector)
                total = min(locator.count(), limit)
            except Exception:
                total = 0
            for idx in range(total):
                node = locator.nth(idx)
                for attr in ("aria-label", "data-item-id", "href"):
                    try:
                        raw = str(node.get_attribute(attr) or "").strip()
                    except Exception:
                        raw = ""
                    if raw and raw not in seen:
                        seen.add(raw)
                        values.append(raw)
                text = self._safe_text(node) or ""
                text = text.strip()
                if text and text not in seen:
                    seen.add(text)
                    values.append(text)
        return values

    def _wait_for_business_panel_ready(self, url_before: str, heading_before: str, panel_before: str, timeout_ms: int = 12000) -> bool:
        assert self.page is not None

        started = time.monotonic()
        timeout_seconds = max(8.0, float(timeout_ms) / 1000.0)
        while (time.monotonic() - started) < timeout_seconds:
            self._accept_consent_if_present()

            current_url = str(self.page.url or "")
            heading_now = self._panel_heading_text()
            panel_now = self._normalized_text(self._panel_text())
            metadata_now = "\n".join(self._collect_panel_metadata(limit=18))

            url_changed = current_url != url_before and "/maps/place/" in current_url
            heading_changed = bool(heading_now and heading_now != heading_before)
            panel_changed = bool(panel_now and panel_now != panel_before and len(panel_now) >= 60)
            has_phone = bool(self._find_phone_match(f"{panel_now}\n{metadata_now}"))
            has_address = bool(
                self.page.locator(
                    "div[role='main'] [data-item-id*='address'], div[role='main'] [aria-label*='Address'], div[role='main'] button[data-item-id='address']"
                ).count()
            )
            has_website = bool(self._find_external_website_candidate(limit=25))

            if (url_changed or heading_changed or panel_changed) and (heading_now or has_phone or has_address or has_website):
                return True

            random_delay(350, 650)

        screenshot_path = self._capture_debug_screenshot(reason="panel_not_ready")
        logging.warning(
            "Timed out waiting for business panel readiness | url=%s screenshot=%s panel=%r",
            str(self.page.url or ""),
            screenshot_path or "<none>",
            self._panel_text()[:400],
        )
        return False

    @staticmethod
    def _is_external_website_candidate(candidate: str) -> bool:
        lowered = str(candidate or "").strip().lower()
        if not lowered.startswith(("http://", "https://")):
            return False
        blocked_tokens = ["google.", "gstatic.", "/maps/", "consent.google.com", "accounts.google.com"]
        return not any(token in lowered for token in blocked_tokens)

    def _find_external_website_candidate(self, limit: int = 60) -> Optional[str]:
        assert self.page is not None

        try:
            anchors = self.page.locator("a[href]")
            total = min(anchors.count(), limit)
        except Exception:
            total = 0

        for idx in range(total):
            try:
                href = str(anchors.nth(idx).get_attribute("href") or "").strip()
            except Exception:
                href = ""
            if self._is_external_website_candidate(href):
                return href.rstrip(".,)")

        return None

    def _extract_business(self, keyword: str) -> Optional[Lead]:
        assert self.page is not None

        name = self._extract_business_name()
        address = self._extract_address()
        website = self._extract_website() or "None"
        phone = self._extract_phone() or "None"

        json_ld = self._extract_from_json_ld()
        if not name:
            name = str(json_ld.get("name") or "").strip() or None
        if not address:
            address = str(json_ld.get("address") or "").strip() or None
        if (not website or website == "None") and str(json_ld.get("website") or "").strip():
            website = str(json_ld.get("website") or "").strip()
        if (not phone or phone == "None") and str(json_ld.get("phone") or "").strip():
            phone = str(json_ld.get("phone") or "").strip()

        if not name or not address:
            screenshot_path = self._capture_debug_screenshot(reason=f"empty_business_{name or 'unknown'}")
            logging.warning(
                "Business extraction incomplete | name=%r address=%r phone=%r website=%r url=%s screenshot=%s panel=%r",
                name,
                address,
                phone,
                website,
                str(self.page.url or ""),
                screenshot_path or "<none>",
                self._panel_text()[:400],
            )
            return None

        panel_text = self._panel_text()
        rating_text = self._extract_rating_from_text(panel_text)
        review_text = self._extract_review_count_from_text(panel_text)

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

        candidate = self._find_external_website_candidate(limit=100)
        if candidate:
            return candidate

        for raw in self._collect_panel_metadata(limit=30):
            for match in re.findall(r"https?://[^\s)\]>\"']+", str(raw or ""), flags=re.IGNORECASE):
                candidate = str(match or "").strip().rstrip(".,)")
                if self._is_external_website_candidate(candidate):
                    return candidate

        try:
            panel_text = self.page.locator("div[role='main']").first.inner_text(timeout=4000)
        except Exception:
            panel_text = ""
        for match in re.findall(r"https?://[^\s)\]>\"']+", str(panel_text or ""), flags=re.IGNORECASE):
            candidate = str(match or "").strip().rstrip(".,)")
            if self._is_external_website_candidate(candidate):
                return candidate

        return None

    def _extract_phone(self) -> Optional[str]:
        assert self.page is not None

        candidates = [
            self.page.locator("button[data-item-id^='phone:tel:']").first,
            self.page.locator("button[aria-label^='Phone:']").first,
            self.page.locator("button[aria-label*='Call']").first,
            self.page.locator("button[data-tooltip='Copy phone number']").first,
            self.page.locator("a[href^='tel:']").first,
            self.page.locator("[aria-label*='phone' i]").first,
        ]

        for locator in candidates:
            if locator.count() == 0:
                continue

            label = locator.get_attribute("aria-label") or ""
            text = self._safe_text(locator) or ""
            blob = f"{label} {text}".strip()

            match = self._find_phone_match(blob)
            if match:
                return match

        metadata_blob = "\n".join(self._collect_panel_metadata(limit=40))
        match = self._find_phone_match(metadata_blob)
        if match:
            return match

        try:
            main_text = self.page.locator("div[role='main']").first.inner_text(timeout=4000)
        except Exception:
            main_text = ""
        match = self._find_phone_match(str(main_text or ""))
        if not match:
            try:
                page_text = self.page.locator("body").inner_text(timeout=5000)
            except Exception:
                page_text = ""
            match = self._find_phone_match(str(page_text or ""))
        if match:
            return match

        return None

    def _extract_address(self) -> Optional[str]:
        assert self.page is not None

        candidates = [
            self.page.locator("button[data-item-id='address']").first,
            self.page.locator("button[data-item-id*='address']").first,
            self.page.locator("button[aria-label*='Address']").first,
            self.page.locator("[aria-label^='Address:']").first,
            self.page.locator("div[role='main'] [data-item-id*='address']").first,
            self.page.locator("div[role='main'] button").filter(has_text=re.compile(r"address", re.IGNORECASE)).first,
        ]
        for address_button in candidates:
            if address_button.count() == 0:
                continue
            label = address_button.get_attribute("aria-label")
            if label and "Address:" in label:
                return label.split("Address:", 1)[1].strip()

            text = self._safe_text(address_button)
            if text:
                return text

        for raw in self._collect_panel_metadata(limit=40):
            normalized = str(raw or "").strip()
            if normalized.lower().startswith("address:"):
                return normalized.split(":", 1)[1].strip()
            if len(normalized) >= 8 and re.search(r"\d", normalized) and re.search(
                r"\b(st|street|ave|avenue|rd|road|blvd|boulevard|dr|drive|ln|lane|way|suite|ste|unit|#)\b",
                normalized,
                flags=re.IGNORECASE,
            ):
                return normalized

        try:
            buttons = self.page.locator("div[role='main'] button")
            total = min(buttons.count(), 12)
            for idx in range(total):
                text = self._safe_text(buttons.nth(idx))
                if text and len(text) >= 8 and any(token in text.lower() for token in ["street", "ave", "road", "blvd", "suite", ","]):
                    return text
        except Exception:
            pass

        panel_text = self._panel_text()
        for raw_line in str(panel_text or "").splitlines():
            line = str(raw_line or "").strip()
            if len(line) < 8:
                continue
            if re.search(r"\d", line) and re.search(r"\b(st|street|ave|avenue|rd|road|blvd|boulevard|dr|drive|ln|lane|way|suite|ste|unit|#)\b", line, flags=re.IGNORECASE):
                return line
            if re.search(r"\b[A-Z]{2}\s+\d{5}(?:-\d{4})?\b", line):
                return line

        return None

    def _card_key(self, card, idx: int) -> str:
        link = None
        title = None

        try:
            link = card.locator("a[href*='/maps/place/']").first.get_attribute("href")
        except PlaywrightTimeoutError:
            link = None
        except Exception:
            link = None

        if not link:
            try:
                link = card.get_attribute("href")
            except Exception:
                link = None

        try:
            title = card.get_attribute("aria-label")
        except PlaywrightTimeoutError:
            title = None
        except Exception:
            title = None

        if not title:
            try:
                title = self._safe_text(card)
            except Exception:
                title = None

        return link or title or f"card-{idx}"

    def _extract_business_name(self) -> Optional[str]:
        assert self.page is not None

        heading_text = self._panel_heading_text()
        if heading_text:
            return heading_text

        panel_text = self._panel_text()
        for raw_line in str(panel_text or "").splitlines():
            line = str(raw_line or "").strip()
            if len(line) < 2:
                continue
            lower_line = line.lower()
            if any(token in lower_line for token in ["directions", "website", "call", "reviews", "open", "closed", "address"]):
                continue
            if re.search(r"\d", line):
                continue
            return line

        return None

    def _panel_text(self) -> str:
        assert self.page is not None
        try:
            return str(self.page.locator("div[role='main']").first.inner_text(timeout=4500) or "")
        except Exception:
            return ""

    @staticmethod
    def _extract_rating_from_text(panel_text: str) -> Optional[str]:
        blob = str(panel_text or "")
        match = re.search(r"\b([0-5](?:[.,]\d)?)\s*(?:stars?)?\b", blob, flags=re.IGNORECASE)
        if not match:
            match = re.search(r"\b([0-5](?:[.,]\d)?)\b", blob)
        return match.group(1) if match else None

    @staticmethod
    def _extract_review_count_from_text(panel_text: str) -> Optional[str]:
        blob = str(panel_text or "")
        match = re.search(r"([\d.,]+)\s+reviews?", blob, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r"\(([\d.,]+)\)", blob)
        return match.group(1) if match else None

    def _card_title(self, card) -> str:
        try:
            title = self._safe_text(card)
            if title:
                return str(title)
        except Exception:
            pass
        try:
            aria = card.get_attribute("aria-label")
            if aria:
                return str(aria).strip()
        except Exception:
            pass
        try:
            href = card.get_attribute("href")
            if href:
                return str(href).strip()
        except Exception:
            pass
        return ""

    def _extract_from_json_ld(self) -> dict:
        assert self.page is not None
        scripts = self.page.locator("script[type='application/ld+json']")
        total = min(scripts.count(), 10)
        for idx in range(total):
            try:
                raw = scripts.nth(idx).inner_text(timeout=1500)
            except Exception:
                continue
            if not str(raw or "").strip():
                continue
            try:
                parsed = json.loads(raw)
            except Exception:
                continue

            stack = [parsed]
            while stack:
                item = stack.pop()
                if isinstance(item, list):
                    stack.extend(item)
                    continue
                if not isinstance(item, dict):
                    continue

                kind = str(item.get("@type") or "").lower()
                if kind in {"localbusiness", "organization", "place"}:
                    address_obj = item.get("address")
                    if isinstance(address_obj, dict):
                        address_val = ", ".join(
                            str(address_obj.get(part) or "").strip()
                            for part in ["streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"]
                            if str(address_obj.get(part) or "").strip()
                        )
                    else:
                        address_val = str(address_obj or "").strip()

                    return {
                        "name": str(item.get("name") or "").strip(),
                        "phone": str(item.get("telephone") or "").strip(),
                        "website": str(item.get("url") or item.get("sameAs") or "").strip(),
                        "address": address_val,
                    }

                for value in item.values():
                    if isinstance(value, (dict, list)):
                        stack.append(value)

        return {}

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
