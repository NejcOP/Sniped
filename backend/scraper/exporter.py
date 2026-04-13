import csv
from typing import Optional

from .db import fetch_target_leads


def export_target_leads(
    db_path: str = "leads.db",
    output_csv: str = "target_leads.csv",
    min_score: float = 7.0,
    user_id: Optional[str] = None,
) -> int:
    rows = fetch_target_leads(db_path=db_path, min_score=min_score, user_id=user_id)

    with open(output_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "business_name",
                "ai_score",
                "email",
                "phone_number",
                "website_url",
                "rating",
                "review_count",
                "main_shortcoming",
                "address",
                "search_keyword",
                "status",
                "enriched_at",
                "scraped_at",
            ]
        )

        for row in rows:
            writer.writerow(
                [
                    row["business_name"],
                    row["ai_score"],
                    row["email"],
                    row["phone_number"],
                    row["website_url"],
                    row["rating"],
                    row["review_count"],
                    row["main_shortcoming"],
                    row["address"],
                    row["search_keyword"],
                    row["status"],
                    row["enriched_at"],
                    row["scraped_at"],
                ]
            )

    return len(rows)
