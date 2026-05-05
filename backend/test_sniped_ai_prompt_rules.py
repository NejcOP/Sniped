from backend.services.ai_mailer_service import AIMailer
from backend.services.prompt_service import PromptFactory


def test_sniped_ai_prompts_and_cta_match_new_outreach_rules() -> None:
    enrichment_prompt = PromptFactory.get_enrichment_system_prompt("Lead Gen Agency")
    email_prompt = PromptFactory.get_email_generation_system_prompt("Lead Gen Agency", "English")
    cta = AIMailer.ensure_two_minute_cta("")

    assert "Sniped AI" in enrichment_prompt
    assert "employee_count" in enrichment_prompt
    assert "lead_score_100" in enrichment_prompt
    assert "lead_priority" in enrichment_prompt
    assert "personalized_hook" in enrichment_prompt
    assert "reasoning" in enrichment_prompt

    assert "The Hook" in email_prompt
    assert "The Bridge" in email_prompt
    assert "Would you be against me sending" in email_prompt or "2-minute video" in email_prompt
    assert "Zoom call" not in cta
    assert "2-minute" in cta or "short video" in cta
