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
