import os
import random
from locust import HttpUser, between, task


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, str(default)) or str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


AUTH_TOKEN = str(os.getenv("LOCUST_AUTH_TOKEN", "") or "").strip()
SCRAPE_ENABLED = str(os.getenv("LOCUST_ENABLE_SCRAPE", "0") or "0").strip() == "1"
LEADS_LIMIT = max(1, min(_env_int("LOCUST_LEADS_LIMIT", 50), 200))
SEARCH_TERMS = [term.strip() for term in str(os.getenv("LOCUST_SEARCH_TERMS", "roof,dental,law,clinic")).split(",") if term.strip()]
STATUS_FILTERS = ["all", "pending", "queued_mail", "paid"]
SORT_MODES = ["recent", "best", "name"]
QUICK_FILTERS = ["all", "qualified", "not_qualified", "mailed", "opened", "replied"]


class SnipedUser(HttpUser):
    wait_time = between(0.2, 1.2)

    def on_start(self) -> None:
        self.auth_headers = {"Authorization": f"Bearer {AUTH_TOKEN}"} if AUTH_TOKEN else {}

    @task(2)
    def health(self):
        self.client.get("/api/health", name="GET /api/health")

    @task(5)
    def leads(self):
        page = random.randint(1, 5)
        params = {
            "limit": str(LEADS_LIMIT),
            "page": str(page),
            "sort": random.choice(SORT_MODES),
            "quick_filter": random.choice(QUICK_FILTERS),
            "include_blacklisted": "0",
            "_ts": str(random.randint(1, 10_000_000)),
        }
        status = random.choice(STATUS_FILTERS)
        if status != "all":
            params["status"] = status

        if SEARCH_TERMS and random.random() < 0.7:
            params["search"] = random.choice(SEARCH_TERMS)

        self.client.get(
            "/api/leads",
            params=params,
            headers=self.auth_headers,
            name="GET /api/leads",
        )

    @task(2)
    def workers(self):
        self.client.get(
            "/api/workers",
            headers=self.auth_headers,
            name="GET /api/workers",
        )

    @task(1)
    def config(self):
        self.client.get(
            "/api/config",
            headers=self.auth_headers,
            name="GET /api/config",
        )

    @task(1)
    def scrape_list(self):
        self.client.get(
            "/api/scrape",
            params={"limit": "20", "page": "1", "sort": "recent"},
            headers=self.auth_headers,
            name="GET /api/scrape",
        )

    @task(1)
    def jobs(self):
        self.client.get(
            "/api/jobs",
            params={"limit": "20"},
            headers=self.auth_headers,
            name="GET /api/jobs",
        )

    @task(0)
    def scrape_post(self):
        if not SCRAPE_ENABLED:
            return
        payload = {
            "keyword": random.choice(SEARCH_TERMS) if SEARCH_TERMS else "roofing dallas",
            "results": 10,
            "country": "US",
            "headless": True,
            "export_targets": False,
            "speed_mode": True,
        }
        self.client.post(
            "/api/scrape",
            json=payload,
            headers={"Content-Type": "application/json", **self.auth_headers},
            name="POST /api/scrape",
        )
