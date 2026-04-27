import asyncio
import logging
import re
from typing import Any, Callable, Optional
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from .models import Lead

EMAIL_REGEX = re.compile(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}\b")
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _normalize_website_url(raw_url: str) -> str:
    value = str(raw_url or "").strip()
    if not value or value.lower() == "none":
        return ""
    if not value.startswith(("http://", "https://")):
        value = f"https://{value}"
    return value


def _extract_social_links(html: str, base_url: str) -> dict[str, Optional[str]]:
    soup = BeautifulSoup(html or "", "html.parser")
    found: dict[str, Optional[str]] = {
        "facebook": None,
        "instagram": None,
        "linkedin": None,
        "tiktok": None,
        "twitter": None,
    }

    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        domain = str(parsed.netloc or "").lower()
        normalized = absolute.strip()

        if "facebook.com" in domain and not found["facebook"]:
            found["facebook"] = normalized
        elif "instagram.com" in domain and not found["instagram"]:
            found["instagram"] = normalized
        elif "linkedin.com" in domain and not found["linkedin"]:
            found["linkedin"] = normalized
        elif "tiktok.com" in domain and not found["tiktok"]:
            found["tiktok"] = normalized
        elif ("twitter.com" in domain or domain == "x.com" or domain.endswith(".x.com")) and not found["twitter"]:
            found["twitter"] = normalized

    return found


def _extract_emails(html: str) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()

    for match in re.findall(r"mailto:([^\"'\s>?#]+)", html or "", flags=re.IGNORECASE):
        email = str(match or "").strip().split("?")[0].lower()
        if not email or email in seen:
            continue
        seen.add(email)
        unique.append(email)

    for match in EMAIL_REGEX.findall(html or ""):
        email = str(match or "").strip().lower()
        if not email or email in seen:
            continue
        seen.add(email)
        unique.append(email)
    return unique


def _guess_email_from_domain(website_url: str, business_name: str) -> Optional[str]:
    domain = str(urlparse(str(website_url or "")).netloc or "").strip().lower()
    if not domain:
        return None
    if domain.startswith("www."):
        domain = domain[4:]
    if not re.search(r"\.[a-z]{2,}$", domain):
        return None

    sanitized_name = re.sub(r"[^a-z0-9]+", "", str(business_name or "").lower())
    prefixes = ["info", "contact", "hello", "sales", "support"]
    if sanitized_name and len(sanitized_name) >= 4:
        prefixes.insert(0, sanitized_name[:24])

    for prefix in prefixes:
        if prefix:
            return f"{prefix}@{domain}"
    return None


def _detect_tech_stack(html: str) -> list[str]:
    text = (html or "").lower()
    stack: list[str] = []

    if "wp-content" in text or "wordpress" in text:
        stack.append("WordPress")
    if "shopify" in text:
        stack.append("Shopify")
    if "wix.com" in text or "wixstatic.com" in text:
        stack.append("Wix")
    if "webflow" in text:
        stack.append("Webflow")
    if "squarespace" in text:
        stack.append("Squarespace")
    if "react" in text and "react-dom" in text:
        stack.append("React")
    if "vue" in text and "__vue" in text:
        stack.append("Vue")

    if not stack:
        stack.append("Unknown")

    return stack


def _has_meta_pixel(html: str) -> bool:
    text = (html or "").lower()
    return any(token in text for token in ["fbq(", "connect.facebook.net/en_us/fbevents", "meta pixel", "facebook pixel"])


def _is_outdated_site(html: str, website_url: str, tech_stack: list[str]) -> bool:
    text = (html or "").lower()
    url = str(website_url or "").strip().lower()

    has_viewport = "name=\"viewport\"" in text or "name='viewport'" in text
    old_jquery = bool(re.search(r"jquery[^\n]{0,40}(1\.[0-9]+|2\.[0-9]+)", text))
    insecure_url = url.startswith("http://")
    unknown_stack = "Unknown" in tech_stack

    signals = [not has_viewport, old_jquery, insecure_url, unknown_stack]
    return sum(1 for signal in signals if signal) >= 2


def _compute_qualification_score(
    social_links: dict[str, Optional[str]],
    has_pixel: bool,
    outdated_site: bool,
) -> float:
    score = 0.0

    if not social_links.get("facebook"):
        score += 1.5
    if not social_links.get("instagram"):
        score += 1.5
    if not social_links.get("linkedin"):
        score += 1.0
    if not social_links.get("tiktok"):
        score += 0.5

    if not has_pixel:
        score += 1.5

    if outdated_site:
        score += 2.5

    return round(min(10.0, score), 2)


async def _fetch_website_html(session: aiohttp.ClientSession, website_url: str) -> list[tuple[str, str]]:
    normalized = _normalize_website_url(website_url)
    if not normalized:
        return []

    targets = [normalized]
    if normalized.startswith("https://"):
        targets.append(normalized.replace("https://", "http://", 1))

    page_paths = ["", "/contact", "/contact-us", "/about", "/about-us", "/kontakt"]
    pages: list[tuple[str, str]] = []
    seen_urls: set[str] = set()

    for target in targets:
        for path in page_paths:
            crawl_target = target if not path else urljoin(target.rstrip("/") + "/", path.lstrip("/"))
            try:
                async with session.get(crawl_target, allow_redirects=True) as response:
                    if response.status >= 400:
                        continue
                    body = await response.text(errors="ignore")
                    final_url = str(response.url)
                    if not body or final_url in seen_urls:
                        continue
                    seen_urls.add(final_url)
                    pages.append((final_url, body))
            except Exception:
                continue

    return pages


async def _enrich_single_lead(
    lead: Lead,
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
) -> bool:
    website = _normalize_website_url(str(getattr(lead, "website_url", "") or ""))
    if not website:
        return False

    async with semaphore:
        pages = await _fetch_website_html(session, website)

    if not pages:
        return False

    merged_html = "\n".join(str(item[1] or "") for item in pages)
    canonical_url = str(pages[0][0] or website)

    social_links: dict[str, Optional[str]] = {
        "facebook": None,
        "instagram": None,
        "linkedin": None,
        "tiktok": None,
        "twitter": None,
    }
    for page_url, page_html in pages:
        detected = _extract_social_links(page_html, page_url)
        for key in social_links.keys():
            if not social_links.get(key) and detected.get(key):
                social_links[key] = detected.get(key)

    emails = _extract_emails(merged_html)
    tech_stack = _detect_tech_stack(merged_html)
    has_pixel = _has_meta_pixel(merged_html)
    outdated_site = _is_outdated_site(merged_html, canonical_url, tech_stack)
    qualification_score = _compute_qualification_score(social_links, has_pixel, outdated_site)

    lead.facebook_url = lead.facebook_url or social_links.get("facebook")
    lead.instagram_url = lead.instagram_url or social_links.get("instagram")
    lead.linkedin_url = lead.linkedin_url or social_links.get("linkedin")
    lead.tiktok_url = social_links.get("tiktok")

    lead.fb_link = social_links.get("facebook")
    lead.ig_link = social_links.get("instagram")
    lead.has_pixel = int(bool(has_pixel))
    lead.tech_stack = ", ".join(tech_stack)
    lead.qualification_score = qualification_score

    if emails and not str(getattr(lead, "email", "") or "").strip():
        lead.email = emails[0]
    elif not str(getattr(lead, "email", "") or "").strip():
        guessed_email = _guess_email_from_domain(canonical_url, str(getattr(lead, "business_name", "") or ""))
        if guessed_email:
            lead.email = guessed_email

    return True


async def enrich_leads_full_data(
    leads: list[Lead],
    *,
    concurrency: int = 6,
    timeout_seconds: int = 12,
    progress_callback: Optional[Callable[[int, int, Optional[str]], None]] = None,
) -> dict[str, int]:
    if not leads:
        return {"crawled": 0, "eligible": 0}

    eligible = [lead for lead in leads if _normalize_website_url(str(getattr(lead, "website_url", "") or ""))]
    if not eligible:
        return {"crawled": 0, "eligible": 0}

    timeout = aiohttp.ClientTimeout(total=max(4, int(timeout_seconds or 12)))
    connector = aiohttp.TCPConnector(limit=max(2, int(concurrency or 6) * 2), ssl=False)
    semaphore = asyncio.Semaphore(max(1, int(concurrency or 6)))

    completed = 0
    crawled = 0
    lock = asyncio.Lock()

    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        async def _runner(lead: Lead) -> None:
            nonlocal completed, crawled
            ok = False
            try:
                ok = await _enrich_single_lead(lead, session, semaphore)
            except Exception as exc:
                logging.debug("Deep crawl failed for %s: %s", getattr(lead, "business_name", "lead"), exc)

            async with lock:
                completed += 1
                if ok:
                    crawled += 1
                if progress_callback is not None:
                    try:
                        progress_callback(completed, len(eligible), str(getattr(lead, "business_name", "") or "") or None)
                    except Exception:
                        logging.debug("Deep crawl progress callback failed.")

        await asyncio.gather(*[_runner(lead) for lead in eligible])

    return {"crawled": int(crawled), "eligible": int(len(eligible))}
