import logging
import random
import re
from pathlib import Path
from typing import List, Optional
from urllib.parse import quote_plus

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
)
from .models import Lead

# ── Opportunity filter thresholds ─────────────────────────────────────────────
# Skip over-established businesses that show no sales gap.
_SKIP_MIN_REVIEWS: int = 150   # skip if review_count exceeds this…
_SKIP_MIN_RATING: float = 4.8  # …AND rating is above this (dominant player)
# Sweet-spot: businesses actively needing a boost.
_SWEET_MIN_REVIEWS: int = 2
_SWEET_MAX_REVIEWS: int = 40


class GoogleMapsScraper:
    def __init__(
        self,
        headless: bool = True,
        user_agent: str = MODERN_USER_AGENT,
        country_code: str = "us",
        user_data_dir: str = "profiles/maps_profile",
    ) -> None:
        self.headless = headless
        self.user_agent = user_agent
        self.country_code = normalize_country_code(country_code)
        self.google_domain = google_domain_for_country(self.country_code)
        self.locale = locale_for_country(self.country_code)
        self.user_data_dir = str(Path(user_data_dir))
        self._playwright = None
        self._browser = None
        self._context = None
        self.page: Optional[Page] = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def start(self) -> None:
        self._playwright = sync_playwright().start()

        self._browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-default-browser-check",
            ],
        )

        self._context = self._browser.new_context(
            user_agent=self.user_agent,
            viewport={"width": 1366, "height": 768},
            locale=self.locale,
        )

        self.page = self._context.new_page()
        self._apply_stealth()
        self._handle_startup_consent()

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
            try:
                self._browser.close()
            except Exception:
                logging.debug("Browser already closed during cleanup.")
            finally:
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

    def scrape(self, keyword: str, max_results: int) -> List[Lead]:
        if not self.page:
            raise RuntimeError("Scraper not started. Use with-context or call start().")

        self._open_maps_and_search(keyword)
        leads: List[Lead] = []
        seen_cards = set()
        stalled_rounds = 0

        while len(leads) < max_results and stalled_rounds < 8:
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
                if not self._open_card(card):
                    continue

                lead = self._extract_business(keyword)
                if not lead:
                    continue

                # ── Opportunity filter ────────────────────────────────────────
                reviews = lead.review_count or 0
                rating = lead.rating or 0.0

                if reviews > _SKIP_MIN_REVIEWS and rating > _SKIP_MIN_RATING:
                    logging.debug(
                        "[opportunity-filter] Skipping dominant player '%s' "
                        "(reviews=%s, rating=%.1f).",
                        lead.business_name, reviews, rating,
                    )
                    seen_cards.add(card_key)  # don't revisit
                    continue

                if not (_SWEET_MIN_REVIEWS <= reviews <= _SWEET_MAX_REVIEWS):
                    logging.debug(
                        "[opportunity-filter] '%s' outside sweet-spot "
                        "(reviews=%s) — lower priority, still included.",
                        lead.business_name, reviews,
                    )
                # ─────────────────────────────────────────────────────────────

                if lead.business_name and lead.address:
                    leads.append(lead)

                random_mouse_movements(self.page, count=random.randint(2, 4))
                random_delay(300, 900)

            if len(seen_cards) == before_seen:
                stalled_rounds += 1
            else:
                stalled_rounds = 0

            if len(leads) < max_results:
                panel = self.page.locator("div[role='feed']").first
                try:
                    human_like_scroll(panel, steps=random.randint(2, 4))
                except PlaywrightTimeoutError:
                    logging.warning("Could not scroll results panel in this round.")

        return leads[:max_results]

    def _open_maps_and_search(self, keyword: str) -> None:
        assert self.page is not None

        base_url = f"https://{self.google_domain}/maps"
        self.page.goto(base_url, wait_until="domcontentloaded")
        self._accept_consent_if_present()
        random_mouse_movements(self.page, count=4)
        random_delay(500, 1400)

        search_box = self._get_search_box()
        if search_box is None:
            logging.warning("Search box not found on base maps URL, trying direct search URL fallback.")
            if self._search_via_fallback_url(keyword):
                return
            raise RuntimeError("Google Maps search box was not found after consent handling.")

        search_box.click()
        search_box.fill("")
        human_type(search_box, keyword)
        random_delay(300, 800)
        search_box.press("Enter")
        random_delay(1200, 2200)
        self._accept_consent_if_present()

        # If a business profile opens directly, we can still parse it.
        try:
            self.page.locator("div[role='feed']").first.wait_for(state="visible", timeout=8000)
        except PlaywrightTimeoutError:
            logging.info("Results feed not visible yet, trying direct search URL fallback.")
            self._search_via_fallback_url(keyword)

    def _search_via_fallback_url(self, keyword: str) -> bool:
        assert self.page is not None

        fallback_url = f"https://www.google.com/maps/search/{quote_plus(keyword)}"
        self.page.goto(fallback_url, wait_until="domcontentloaded")
        self._accept_consent_if_present()
        random_delay(1000, 1800)

        try:
            self.page.locator("div[role='feed']").first.wait_for(state="visible", timeout=9000)
            return True
        except PlaywrightTimeoutError:
            return False

    def _get_search_box(self):
        assert self.page is not None

        selectors = [
            "input#searchboxinput",
            "input[aria-label*='Search Google Maps']",
            "input[aria-label*='Search']",
            "input[placeholder*='Search']",
        ]

        for selector in selectors:
            locator = self.page.locator(selector).first
            try:
                locator.wait_for(state="visible", timeout=6000)
                return locator
            except PlaywrightTimeoutError:
                continue

        return None

    def _accept_consent_if_present(self) -> bool:
        assert self.page is not None

        text_patterns = [
            "Accept all",
            "I agree",
            "Agree",
            "Accept",
            "Akzeptieren",
            "Alle akzeptieren",
            "Zustimmen",
            "Strinjam",
            "Sprejmi",
            "Aceptar",
            "Acepto",
            "Accepter",
            "Accetta",
            "Souhlasim",
            "Prihvatam",
            "Slažem se",
        ]

        selectors = [
            "button#L2AGLb",
            "button#introAgreeButton",
            "button[aria-label*='Accept']",
            "button[aria-label*='Agree']",
            "button[aria-label*='Akzeptieren']",
            "button[aria-label*='Strinjam']",
            "form [type='submit']",
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
            "button:has-text('Akzeptieren')",
            "button:has-text('Alle akzeptieren')",
            "button:has-text('Strinjam')",
            "button:has-text('Sprejmi')",
        ]

        for _ in range(3):
            scopes = [self.page.main_frame, *self.page.frames]
            for scope in scopes:
                for selector in selectors:
                    try:
                        button = scope.locator(selector).first
                        if button.count() > 0 and button.is_visible(timeout=700):
                            button.click(timeout=1800)
                            random_delay(350, 900)
                            return True
                    except Exception:
                        continue

                for pattern in text_patterns:
                    try:
                        button = scope.get_by_role("button", name=re.compile(pattern, re.IGNORECASE)).first
                        if button.count() > 0 and button.is_visible(timeout=700):
                            button.click(timeout=1800)
                            random_delay(350, 900)
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
        )

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
