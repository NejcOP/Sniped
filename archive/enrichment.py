import argparse
import csv
import logging
import re
import sqlite3
from typing import Iterable, List, Optional
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

from scraper.anti_bot import MODERN_USER_AGENT, apply_stealth, random_delay, random_mouse_movements
from scraper.db import init_db

EMAIL_REGEX = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b")


class LeadEnricher:
    def __init__(self, db_path: str = "leads.db", headless: bool = True, max_google_links: int = 3) -> None:
        self.db_path = db_path
        self.headless = headless
        self.max_google_links = max_google_links

        init_db(db_path=self.db_path)
        self._ensure_enrichment_columns()

    def run(self, limit: Optional[int] = None) -> tuple[int, int]:
        leads = self._fetch_leads_without_email(limit=limit)
        if not leads:
            logging.info("No leads without e-mail found.")
            return 0, 0

        processed = 0
        with_email = 0

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=self.headless,
                args=["--disable-blink-features=AutomationControlled"],
            )
            context = browser.new_context(
                user_agent=MODERN_USER_AGENT,
                viewport={"width": 1366, "height": 768},
                locale="en-US",
            )
            page = context.new_page()
            apply_stealth(page)

            for lead in leads:
                email = None
                insecure_site = False

                website_url = self._normalize_website(lead["website_url"])
                if website_url:
                    email, insecure_site = self._find_email_on_website(page=page, website_url=website_url)
                else:
                    city = self._extract_city(lead["address"])
                    email = self._find_email_via_google(
                        page=page,
                        context=context,
                        business_name=lead["business_name"],
                        city=city,
                    )

                shortcoming = self._infer_main_shortcoming(
                    website_url=lead["website_url"],
                    rating=lead["rating"],
                    review_count=lead["review_count"],
                    insecure_site=insecure_site,
                )

                self._update_lead_enrichment(
                    lead_id=lead["id"],
                    email=email,
                    insecure_site=insecure_site,
                    main_shortcoming=shortcoming,
                )

                processed += 1
                if email:
                    with_email += 1

                random_delay(250, 700)

            context.close()
            browser.close()

        return processed, with_email

    def export_ai_mailer_ready(self, output_csv: str = "ai_mailer_ready.csv") -> int:
        rows = self._fetch_ai_mailer_rows()

        with open(output_csv, "w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["business_name", "email", "main_shortcoming"])
            for row in rows:
                writer.writerow([row["business_name"], row["email"], row["main_shortcoming"]])

        return len(rows)

    def _ensure_enrichment_columns(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(leads)").fetchall()
            }

            if "email" not in columns:
                conn.execute("ALTER TABLE leads ADD COLUMN email TEXT")

            if "insecure_site" not in columns:
                conn.execute("ALTER TABLE leads ADD COLUMN insecure_site INTEGER DEFAULT 0")

            if "main_shortcoming" not in columns:
                conn.execute("ALTER TABLE leads ADD COLUMN main_shortcoming TEXT")

            if "enriched_at" not in columns:
                conn.execute("ALTER TABLE leads ADD COLUMN enriched_at TEXT")

            conn.commit()

    def _fetch_leads_without_email(self, limit: Optional[int] = None) -> List[sqlite3.Row]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            query = """
                SELECT
                    id,
                    business_name,
                    website_url,
                    rating,
                    review_count,
                    address
                FROM leads
                WHERE email IS NULL OR email = ''
                ORDER BY id ASC
            """

            if limit and limit > 0:
                query += " LIMIT ?"
                return conn.execute(query, (limit,)).fetchall()

            return conn.execute(query).fetchall()

    def _update_lead_enrichment(
        self,
        lead_id: int,
        email: Optional[str],
        insecure_site: bool,
        main_shortcoming: str,
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                UPDATE leads
                SET
                    email = ?,
                    insecure_site = ?,
                    main_shortcoming = ?,
                    enriched_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    email,
                    1 if insecure_site else 0,
                    main_shortcoming,
                    lead_id,
                ),
            )
            conn.commit()

    def _fetch_ai_mailer_rows(self) -> List[sqlite3.Row]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    business_name,
                    email,
                    COALESCE(main_shortcoming, 'No clear shortcoming identified') AS main_shortcoming
                FROM leads
                WHERE email IS NOT NULL AND email != ''
                ORDER BY business_name ASC
                """
            ).fetchall()

        return rows

    def _find_email_on_website(self, page, website_url: str) -> tuple[Optional[str], bool]:
        try:
            page.goto(website_url, wait_until="domcontentloaded", timeout=15000)
        except PlaywrightTimeoutError:
            logging.warning("Timeout while loading website: %s", website_url)
            return None, not website_url.lower().startswith("https://")
        except Exception as exc:
            logging.warning("Could not load website %s (%s)", website_url, exc)
            return None, not website_url.lower().startswith("https://")

        random_mouse_movements(page, count=3)
        random_delay(250, 700)

        final_url = page.url or website_url
        insecure_site = not final_url.lower().startswith("https://")

        emails = self._extract_emails(page.content())
        if emails:
            return self._pick_best_email(emails), insecure_site

        contact_page = self._discover_contact_page(page=page, base_url=final_url)
        if contact_page:
            try:
                page.goto(contact_page, wait_until="domcontentloaded", timeout=12000)
                random_delay(250, 600)
                emails = self._extract_emails(page.content())
                if emails:
                    return self._pick_best_email(emails), insecure_site
            except Exception:
                return None, insecure_site

        return None, insecure_site

    def _find_email_via_google(self, page, context, business_name: str, city: str) -> Optional[str]:
        query = f"contact {business_name} {city}".strip()
        search_url = f"https://www.google.com/search?q={quote_plus(query)}"

        try:
            page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        except Exception:
            return None

        self._accept_google_consent(page)
        random_mouse_movements(page, count=2)
        random_delay(300, 900)

        immediate_emails = self._extract_emails(page.content())
        if immediate_emails:
            return self._pick_best_email(immediate_emails)

        links = self._extract_google_result_links(page=page)
        for link in links:
            sub_page = context.new_page()
            apply_stealth(sub_page)
            try:
                sub_page.goto(link, wait_until="domcontentloaded", timeout=12000)
                random_delay(250, 700)
                emails = self._extract_emails(sub_page.content())
                if emails:
                    return self._pick_best_email(emails)
            except Exception:
                continue
            finally:
                sub_page.close()

        return None

    def _extract_google_result_links(self, page) -> List[str]:
        blocked_domains = {
            "google.com",
            "webcache.googleusercontent.com",
            "youtube.com",
            "facebook.com",
            "instagram.com",
            "linkedin.com",
        }

        links: List[str] = []
        seen = set()

        anchors = page.locator("a[href]")
        total = min(anchors.count(), 140)

        for idx in range(total):
            href = anchors.nth(idx).get_attribute("href")
            if not href:
                continue

            candidate = self._normalize_google_href(href)
            if not candidate:
                continue

            parsed = urlparse(candidate)
            domain = parsed.netloc.lower().replace("www.", "")
            if not domain or domain in blocked_domains:
                continue

            if candidate in seen:
                continue

            seen.add(candidate)
            links.append(candidate)

            if len(links) >= self.max_google_links:
                break

        return links

    @staticmethod
    def _normalize_google_href(href: str) -> Optional[str]:
        if href.startswith("/url?"):
            parsed = urlparse(href)
            q = parse_qs(parsed.query).get("q")
            if q and q[0].startswith("http"):
                return q[0]
            return None

        if href.startswith("http"):
            return href

        return None

    @staticmethod
    def _accept_google_consent(page) -> None:
        candidates = [
            "button:has-text('Accept all')",
            "button:has-text('I agree')",
            "button:has-text('Accept')",
        ]

        for selector in candidates:
            try:
                button = page.locator(selector).first
                if button.count() > 0:
                    button.click(timeout=1500)
                    random_delay(200, 500)
                    return
            except Exception:
                continue

    @staticmethod
    def _discover_contact_page(page, base_url: str) -> Optional[str]:
        anchors = page.locator("a[href]")
        total = min(anchors.count(), 100)

        for idx in range(total):
            anchor = anchors.nth(idx)
            text = (anchor.inner_text(timeout=800) or "").strip().lower()
            href = (anchor.get_attribute("href") or "").strip()
            blob = f"{text} {href}".lower()

            if "contact" not in blob:
                continue

            if href.startswith("mailto:"):
                continue

            return urljoin(base_url, href)

        return None

    @staticmethod
    def _extract_emails(text: str) -> List[str]:
        found = set(match.lower() for match in EMAIL_REGEX.findall(text or ""))
        invalid_suffixes = (".png", ".jpg", ".jpeg", ".svg", ".webp", ".gif")

        filtered = []
        for email in found:
            if any(email.endswith(suffix) for suffix in invalid_suffixes):
                continue
            if email.endswith("@example.com"):
                continue
            filtered.append(email)

        return sorted(filtered)

    @staticmethod
    def _pick_best_email(emails: Iterable[str]) -> Optional[str]:
        pool = list(emails)
        if not pool:
            return None

        priorities = ["info", "contact", "hello", "office", "sales", "support"]
        for token in priorities:
            for email in pool:
                local = email.split("@", 1)[0].lower()
                if token in local:
                    return email

        return sorted(pool)[0]

    @staticmethod
    def _normalize_website(website_url: Optional[str]) -> Optional[str]:
        if not website_url:
            return None

        cleaned = website_url.strip()
        if not cleaned or cleaned.lower() == "none":
            return None

        if cleaned.startswith("http://") or cleaned.startswith("https://"):
            return cleaned

        return f"https://{cleaned}"

    @staticmethod
    def _extract_city(address: Optional[str]) -> str:
        if not address:
            return ""

        parts = [part.strip() for part in address.split(",") if part.strip()]
        if not parts:
            return ""

        for part in reversed(parts):
            if re.search(r"\d", part):
                continue
            if len(part) <= 2 and part.isupper():
                continue
            return part

        if len(parts) >= 2:
            return parts[-2]

        return parts[0]

    @staticmethod
    def _infer_main_shortcoming(
        website_url: Optional[str],
        rating: Optional[float],
        review_count: Optional[int],
        insecure_site: bool,
    ) -> str:
        if not website_url or str(website_url).strip().lower() == "none":
            return "Missing website"

        if insecure_site:
            return "Website not using HTTPS"

        if rating is None:
            return "No visible Google rating"

        if rating < 3.5:
            return f"Low Google rating ({rating:.1f})"

        if review_count is None or review_count < 15:
            return "Low review count"

        return "No clear shortcoming identified"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enrich leads with e-mail discovery and website security status."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging verbosity.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    enrich_cmd = subparsers.add_parser("enrich", help="Populate e-mail and site security fields.")
    enrich_cmd.add_argument("--db", default="leads.db", help="SQLite DB path.")
    enrich_cmd.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max number of leads to enrich in one run (0 = all).",
    )
    enrich_cmd.add_argument(
        "--headless",
        action="store_true",
        help="Run Playwright in headless mode.",
    )
    enrich_cmd.add_argument(
        "--output",
        default="ai_mailer_ready.csv",
        help="Output CSV path for AI Mailer format.",
    )
    enrich_cmd.add_argument(
        "--skip-export",
        action="store_true",
        help="Skip exporting AI Mailer CSV after enrichment.",
    )

    export_cmd = subparsers.add_parser("export-ai", help="Export AI Mailer ready CSV.")
    export_cmd.add_argument("--db", default="leads.db", help="SQLite DB path.")
    export_cmd.add_argument(
        "--output",
        default="ai_mailer_ready.csv",
        help="Output CSV path for AI Mailer format.",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    if args.command == "enrich":
        enricher = LeadEnricher(db_path=args.db, headless=args.headless)
        limit = args.limit if args.limit and args.limit > 0 else None
        processed, with_email = enricher.run(limit=limit)

        print(f"Processed: {processed}")
        print(f"Found e-mails: {with_email}")

        if not args.skip_export:
            exported = enricher.export_ai_mailer_ready(output_csv=args.output)
            print(f"AI Mailer export rows: {exported} -> {args.output}")

    elif args.command == "export-ai":
        enricher = LeadEnricher(db_path=args.db, headless=True)
        exported = enricher.export_ai_mailer_ready(output_csv=args.output)
        print(f"AI Mailer export rows: {exported} -> {args.output}")


if __name__ == "__main__":
    main()
