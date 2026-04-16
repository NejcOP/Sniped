import csv

from .db import fetch_target_leads


def export_target_leads(
    db_path: str = "runtime-db", output_csv: str = "target_leads.csv", min_rating: float = 3.5
) -> int:
    rows = fetch_target_leads(db_path=db_path, min_rating=min_rating)

    with open(output_csv, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "business_name",
                "website_url",
                "phone_number",
                "rating",
                "review_count",
                "address",
                "search_keyword",
                "scraped_at",
            ]
        )

        for row in rows:
            writer.writerow(
                [
                    row["business_name"],
                    row["website_url"],
                    row["phone_number"],
                    row["rating"],
                    row["review_count"],
                    row["address"],
                    row["search_keyword"],
                    row["scraped_at"],
                ]
            )

    return len(rows)
