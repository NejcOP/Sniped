import unittest
from types import SimpleNamespace
from unittest.mock import patch

from backend.services.enrichment_service import LeadEnricher


class LeadEnricherSqlRegressionTests(unittest.TestCase):
    def test_check_idempotency_uses_text_safe_process_status(self) -> None:
        captured = {}

        service = LeadEnricher.__new__(LeadEnricher)
        service.user_id = "user-1"

        def fake_fetchone(statement, params=None):
            captured["sql"] = str(statement)
            captured["params"] = dict(params or {})
            return {"process_status": "PROCESSING"}

        service._fetchone = fake_fetchone

        result = LeadEnricher.check_idempotency(service, 42)

        self.assertTrue(result)
        self.assertIn("COALESCE(process_status::text, 'PENDING') AS process_status", captured["sql"])
        self.assertNotIn("UPPER(process_status)", captured["sql"])
        self.assertEqual(captured["params"]["lead_id"], 42)
        self.assertEqual(captured["params"]["user_id"], "user-1")

    def test_fetch_leads_for_enrichment_uses_text_safe_candidate_filter(self) -> None:
        captured = {}

        service = LeadEnricher.__new__(LeadEnricher)
        service.user_id = "user-1"

        def fake_fetchall(statement, params=None):
            captured["sql"] = str(statement)
            captured["params"] = dict(params or {})
            return []

        service._fetchall = fake_fetchall

        fake_engine = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        with patch("backend.services.enrichment_service.get_engine", return_value=fake_engine):
            rows = LeadEnricher._fetch_leads_for_enrichment(service, limit=5)

        self.assertEqual(rows, [])
        self.assertIn("WITH candidate AS (", captured["sql"])
        self.assertIn("COALESCE(process_status::text, 'PENDING') IN ('PENDING', 'FAILED')", captured["sql"])
        self.assertNotIn("UPPER(process_status)", captured["sql"])
        self.assertEqual(captured["params"]["user_id"], "user-1")
        self.assertEqual(captured["params"]["limit"], 5)


if __name__ == "__main__":
    unittest.main()