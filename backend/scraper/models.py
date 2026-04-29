from dataclasses import dataclass
from typing import Optional


@dataclass
class Lead:
    business_name: str
    website_url: str
    phone_number: str
    rating: Optional[float]
    review_count: Optional[int]
    address: str
    search_keyword: str
    maps_url: Optional[str] = None
    google_claimed: Optional[int] = None
    linkedin_url: Optional[str] = None
    instagram_url: Optional[str] = None
    facebook_url: Optional[str] = None
    tiktok_url: Optional[str] = None
    twitter_url: Optional[str] = None
    youtube_url: Optional[str] = None
    ig_link: Optional[str] = None
    fb_link: Optional[str] = None
    has_pixel: Optional[int] = None
    insecure_site: Optional[int] = None
    is_ads_client: Optional[int] = 0
    is_website_client: Optional[int] = 0
    follow_up_count: Optional[int] = 0
    open_count: Optional[int] = 0
    campaign_step: Optional[int] = 1
    tech_stack: Optional[str] = None
    email: Optional[str] = None
    qualification_score: Optional[float] = None
