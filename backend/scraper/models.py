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
    google_claimed: Optional[bool] = None
    linkedin_url: Optional[str] = None
    instagram_url: Optional[str] = None
    facebook_url: Optional[str] = None
    tiktok_url: Optional[str] = None
    ig_link: Optional[str] = None
    fb_link: Optional[str] = None
    has_pixel: Optional[bool] = None
    tech_stack: Optional[str] = None
    email: Optional[str] = None
    qualification_score: Optional[float] = None
