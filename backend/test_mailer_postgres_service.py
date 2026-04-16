import unittest
from unittest.mock import patch

from backend.services.ai_mailer_service import AIMailer


class AIMailerPostgresServiceTests(unittest.TestCase):
    def test_load_accounts_reads_user_settings_first(self) -> None:
        with patch.object(AIMailer, "_load_config", return_value={}), patch.object(AIMailer, "_ensure_mailer_columns"), patch("backend.services.ai_mailer_service.init_db"), patch.object(
            AIMailer,
            "_load_user_settings_smtp_accounts",
            return_value=[
                {
                    "email": "sender@example.com",
                    "password": "pw",
                    "host": "smtp.example.com",
                    "port": 587,
                    "use_tls": True,
                }
            ],
        ):
            with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False):
                mailer = AIMailer(user_id="user-1")

        self.assertEqual(len(mailer.accounts), 1)
        self.assertEqual(mailer.accounts[0].email, "sender@example.com")

    def test_mark_emailed_updates_postgres_lead_status(self) -> None:
        executed = []

        def fake_execute(self, statement, params=None):
            executed.append((str(statement), dict(params or {})))

        with patch.object(AIMailer, "_load_config", return_value={}), patch.object(AIMailer, "_ensure_mailer_columns"), patch("backend.services.ai_mailer_service.init_db"), patch.object(AIMailer, "_load_user_settings_smtp_accounts", return_value=[]), patch.object(
            AIMailer,
            "_fetchone",
            return_value={"id": 5, "email": "lead@example.com", "user_id": "user-1"},
        ), patch.object(AIMailer, "_execute", new=fake_execute):
            with patch.dict("os.environ", {"OPENAI_API_KEY": "test-key"}, clear=False):
                mailer = AIMailer(user_id="user-1")
                mailer.mark_emailed(
                    lead_id=5,
                    status="sent",
                    sender_email="sender@example.com",
                    generated_email_body="hello",
                    subject_line="Subject",
                )

        self.assertTrue(any("UPDATE leads" in sql for sql, _ in executed))
        update_params = next(params for sql, params in executed if "UPDATE leads" in sql)
        self.assertEqual(update_params["status"], "sent")
        self.assertEqual(update_params["lead_id"], 5)


if __name__ == "__main__":
    unittest.main()