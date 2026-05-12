#!/usr/bin/env python3
"""
Clean and fix existing website URLs in the database.

This script:
1. Extracts actual URLs from Google redirect links
2. Removes Yelp/Facebook/Instagram as primary website URLs
3. Validates all URLs are clean and direct

Usage:
    python clean_website_urls.py
"""

import logging
import os
import sys
from typing import Optional
from urllib.parse import parse_qs, urlparse

from supabase import create_client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)


def _extract_url_from_redirect(redirect_url: str) -> Optional[str]:
    """Extract actual URL from Google redirect (e.g., /url?q=https://example.com)."""
    try:
        if "google." not in redirect_url.lower() or "/url?" not in redirect_url.lower():
            return None
        
        parsed = urlparse(redirect_url)
        if not parsed.query:
            return None
        
        qs = parse_qs(parsed.query)
        if "q" in qs and qs["q"]:
            actual_url = qs["q"][0]
            if actual_url.startswith(("http://", "https://")):
                return actual_url
    except Exception as e:
        logger.warning(f"Failed to extract URL from redirect '{redirect_url}': {e}")
    
    return None


def _is_social_media_url(url: str) -> bool:
    """Check if URL is a social media or tracking domain."""
    if not url:
        return False
    
    lower_url = url.lower()
    blocked_domains = ["yelp.com", "facebook.com", "instagram.com", "google.com/url"]
    return any(domain in lower_url for domain in blocked_domains)


def clean_website_urls():
    """Clean and fix existing website URLs in the database."""
    # Initialize Supabase client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
    
    if not supabase_url or not supabase_key:
        logger.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY environment variables")
        sys.exit(1)
    
    supabase = create_client(supabase_url, supabase_key)
    
    logger.info("Starting website URL cleanup...")
    
    # Fetch all leads with website_url
    try:
        response = supabase.table("leads").select("id, website_url").execute()
        leads = response.data or []
        logger.info(f"Found {len(leads)} leads with data")
    except Exception as e:
        logger.error(f"Failed to fetch leads: {e}")
        sys.exit(1)
    
    updates_needed = []
    google_redirect_fixed = 0
    social_media_removed = 0
    
    for lead in leads:
        lead_id = lead.get("id")
        website_url = lead.get("website_url", "").strip()
        
        if not website_url:
            continue
        
        new_url = website_url
        changed = False
        
        # Try to extract actual URL from Google redirects
        if "/url?" in website_url and "google." in website_url.lower():
            extracted = _extract_url_from_redirect(website_url)
            if extracted:
                new_url = extracted
                changed = True
                google_redirect_fixed += 1
                logger.info(f"Lead {lead_id}: Fixed Google redirect")
                logger.debug(f"  Old: {website_url}")
                logger.debug(f"  New: {new_url}")
        
        # Check if URL is a social media domain (after extraction)
        if _is_social_media_url(new_url):
            new_url = None
            changed = True
            social_media_removed += 1
            logger.info(f"Lead {lead_id}: Removed social media URL: {website_url}")
        
        # Ensure URL starts with http:// or https://
        if new_url and not new_url.startswith(("http://", "https://")):
            new_url = f"https://{new_url}"
            changed = True
            logger.info(f"Lead {lead_id}: Added https:// prefix")
        
        if changed:
            updates_needed.append({"id": lead_id, "website_url": new_url})
    
    logger.info(f"\nCleanup Summary:")
    logger.info(f"  Google redirects fixed: {google_redirect_fixed}")
    logger.info(f"  Social media URLs removed: {social_media_removed}")
    logger.info(f"  Total updates needed: {len(updates_needed)}")
    
    if not updates_needed:
        logger.info("No updates needed!")
        return
    
    # Apply updates
    logger.info("\nApplying updates...")
    batch_size = 50
    for i in range(0, len(updates_needed), batch_size):
        batch = updates_needed[i : i + batch_size]
        try:
            for update in batch:
                supabase.table("leads").update({"website_url": update["website_url"]}).eq(
                    "id", update["id"]
                ).execute()
            logger.info(f"Updated batch {i // batch_size + 1} ({len(batch)} records)")
        except Exception as e:
            logger.error(f"Failed to update batch {i // batch_size + 1}: {e}")
            return False
    
    logger.info("✓ Website URL cleanup complete!")
    return True


if __name__ == "__main__":
    success = clean_website_urls()
    sys.exit(0 if success else 1)
