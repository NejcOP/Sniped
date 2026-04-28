# ============================================================
# Sniped – Prompt Factory Service
# 
# CENTRALIZED AI PROMPT MANAGEMENT
# Single source of truth for all system prompts, niche configs,
# and temperature settings across the application.
#
# Usage:
#   factory = PromptFactory()
#   system = factory.get_system_prompt('Paid Ads Agency')
#   opening = factory.generate_opening_line_prompt('Paid Ads Agency', prospect_data)
#
# To change application-wide AI behavior:
#   Edit MASTER_AI_CONFIG or NICHE_CONFIGS below.
#   All calls to factory methods automatically use the new behavior.
# ============================================================

import json
from typing import Optional, Dict, Any


# ── MASTER AI CONFIGURATION ──
# These settings apply globally unless overridden per-prompt-type
MASTER_AI_CONFIG = {
    "default_niche": "B2B Service Provider",
    "default_model": "gpt-4o-mini",
    "default_max_tokens": 150,
    "default_temperature": 0.7,  # Professional + creative balance
    "openai_timeout_seconds": 15,
}


# ── NICHE-SPECIFIC CONFIGURATIONS ──
# Maps each niche to pain points, focus areas, and positioning
NICHE_CONFIGS = {
    "Paid Ads Agency": {
        "pain_points": [
            "Missing Meta Pixel or GTM implementation",
            "Traffic goes to a leaky bucket without retargeting",
            "No conversion tracking across platforms",
            "Wasted ad spend due to poor funnel optimization",
        ],
        "focus_areas": [
            "Pixel implementation",
            "Conversion tracking",
            "Retargeting strategy",
            "ROAS optimization",
        ],
        "positioning": "You help agencies track and retarget lost traffic.",
        "call_to_action": "Schedule a quick Zoom to review your retargeting gaps.",
    },
    "Web Design & Dev": {
        "pain_points": [
            "Website load speed >3 seconds on mobile",
            "Non-responsive design that doesn't work on phones",
            "Old-school UI pushing customers to competitors",
            "Missing trust signals (SSL, testimonials, social proof)",
        ],
        "focus_areas": [
            "Mobile-first design",
            "Page speed optimization",
            "Modern UX/UI",
            "Trust-building elements",
        ],
        "positioning": "You help build fast, beautiful websites that convert.",
        "call_to_action": "Let me show you how your competitors are beating you on speed.",
    },
    "SEO & Content": {
        "pain_points": [
            "Missing meta titles and meta descriptions",
            "No blog posts in the last 6 months",
            "Google Maps profile incomplete or missing",
            "Invisible for their main service keywords",
        ],
        "focus_areas": [
            "Keyword research and targeting",
            "Content strategy",
            "Technical SEO",
            "Local search optimization",
        ],
        "positioning": "You help businesses dominate Google for their keywords.",
        "call_to_action": "I've identified 5 keywords you should own but don't.",
    },
    "Lead Gen Agency": {
        "pain_points": [
            "Manual lead generation using outdated methods",
            "No automated pipeline",
            "LinkedIn stagnant or not used effectively",
            "Inconsistent lead flow and quality",
        ],
        "focus_areas": [
            "Pipeline automation",
            "LinkedIn strategy",
            "Outbound sequences",
            "Lead scoring and qualification",
        ],
        "positioning": "You help agencies scale from hope-based to data-driven lead gen.",
        "call_to_action": "Show me 3 months of consistent lead flow.",
    },
    "B2B Service Provider": {
        "pain_points": [
            "Outdated contact methods or poor online presence",
            "CEO/Founder wasting time on operational inefficiencies",
            "No scalable systems for client onboarding",
            "Missing opportunities due to lack of visibility",
        ],
        "focus_areas": [
            "Business process optimization",
            "Online visibility and presence",
            "Client journey automation",
            "Operational efficiency",
        ],
        "positioning": "You help B2B service providers save time and scale operations.",
        "call_to_action": "Let me show you how you can reclaim 10 hours of your time weekly.",
    },
}


# ── NICHE CATEGORY DESCRIPTIONS ──
# Human-readable profile for each niche — inserted into the enrichment prompt
# as {{ category_description }} so the AI understands the operator's business context.
NICHE_CATEGORY_DESCRIPTIONS = {
    "Paid Ads Agency": (
        "You run a Paid Ads Agency. You help businesses grow revenue through Meta Ads, Google Ads, "
        "and paid social campaigns. Your clients pay for measurable results — clicks, leads, and conversions."
    ),
    "Web Design & Dev": (
        "You run a Web Design & Development agency. You build fast, modern, mobile-first websites "
        "that convert visitors into paying customers. You specialise in UX, performance, and trust signals."
    ),
    "SEO & Content": (
        "You run an SEO & Content agency. You help businesses rank on the first page of Google, "
        "outrank competitors, and attract organic traffic through strategic keyword targeting and content."
    ),
    "Lead Gen Agency": (
        "You run a Lead Generation Agency. You help B2B companies build automated outbound pipelines "
        "that consistently deliver qualified leads using LinkedIn, cold email, and CRM automation."
    ),
    "B2B Service Provider": (
        "You are a B2B Service Provider offering strategic consulting, process optimisation, or "
        "specialised expertise. You help business owners save time, reduce inefficiencies, and scale operations."
    ),
}


class PromptFactory:
    """
    Factory for generating AI prompts with consistent tone, structure, and behavior.
    
    All prompts enforce:
    - ONE SENTENCE ONLY rule (where applicable)
    - NO INTRODUCTIONS (no 'Hi', 'I hope', 'I noticed')
    - Professional, confident, no-BS tone
    - Language consistency (English by default, can be overridden)
    - JSON response format where applicable
    """
    
    @staticmethod
    def _validate_niche(niche: Optional[str]) -> str:
        """Validate niche and return it, or fallback to default."""
        if not niche or niche.strip() not in NICHE_CONFIGS:
            return MASTER_AI_CONFIG["default_niche"]
        return niche.strip()
    
    @staticmethod
    def get_system_prompt(user_niche: Optional[str]) -> str:
        """
        Get the master system prompt for a given niche.
        
        This prompt defines the AI's role, tone, and core instructions.
        It is used as the foundation for all niche-specific AI calls.
        
        Args:
            user_niche: The user's selected niche (or None for fallback)
            
        Returns:
            A comprehensive system prompt (str) that guides the AI behavior
        """
        niche = PromptFactory._validate_niche(user_niche)
        config = NICHE_CONFIGS.get(niche, NICHE_CONFIGS["B2B Service Provider"])
        
        pain_points_str = "\n".join([f"  - {pp}" for pp in config["pain_points"]])
        focus_areas_str = "\n".join([f"  - {fa}" for fa in config["focus_areas"]])
        
        system_prompt = f"""You are a Sniped B2B Sales Expert specializing in {niche}.

YOUR ROLE:
{config['positioning']}

YOUR NICHE CONTEXT:
- Primary Pain Points:
{pain_points_str}

- Key Focus Areas:
{focus_areas_str}

CORE BEHAVIORAL RULES (DO NOT BREAK):
1. Write directly and confidently. No fluff, no corporate jargon.
2. If writing sentences: ONE SENTENCE ONLY per idea. Keep it tight.
3. NEVER start with greetings like 'Hi', 'Hello', 'I hope', or 'I noticed'. Get straight to the point.
4. Always write in English unless explicitly instructed otherwise.
5. Be specific and data-driven. Reference real numbers, metrics, or behaviors when possible.
6. No AI-sounding phrases: avoid "I wanted to reach out", "I'm writing to inform you", etc.
7. Always assume the reader is busy and skeptical. Prove value in 2-3 sentences.

TONE: Professional, confident, slightly provocative, no-BS.

{niche.upper()} FOCUS:
Focus your analysis and recommendations on: {', '.join(config['focus_areas'][:3])}.
"""
        return system_prompt.strip()
    
    @staticmethod
    def generate_opening_line_prompt(
        user_niche: Optional[str],
        prospect_data: str,
        pack_mode: Optional[str] = None,
    ) -> tuple:
        """
        Generate system + user prompts for a cold email opening line.

        Psychology framework (always applied):
          - Visual Proof:  Sender has already done concrete work for this lead.
          - Rule of 100:   Deliver 100 % of value upfront — don't hint, show it.

        Pack modifiers (optional, layered on top of niche psychology):
          - "local_first":  Heavier local context — city, map visibility, local competitors.
          - "aggressive":   Sharper pain framing — stronger commercial angle, bolder claim.

        Args:
            user_niche:    The user's selected niche
            prospect_data: Free-form prospect description
            pack_mode:     Optional tone modifier — "local_first" | "aggressive" | None

        Returns:
            (system_prompt, user_prompt) tuple ready for OpenAI API
        """
        niche = PromptFactory._validate_niche(user_niche)

        # ── Base niche hook templates ───────────────────────────────────────────
        hook_templates = {
            "Paid Ads Agency": (
                "I analysed your competitors and found you're paying roughly 30 % more per click than "
                "the top 3 in your space — and I've already built a new campaign structure that fixes it."
            ),
            "Web Design & Dev": (
                "I didn't want to come empty-handed, so I already put together a wireframe redesign of "
                "your landing page hero section — faster load time, modern layout, more conversions."
            ),
            "SEO & Content": (
                "Your top competitor is outranking you for a keyword with 5,000 monthly searches, and "
                "I've already written the content outline that would get you back to position 1 in 14 days."
            ),
            "Lead Gen Agency": (
                "I've already found 10 companies in your target market that are actively looking for "
                "exactly the service you offer — verified decision-maker emails included."
            ),
            "B2B Service Provider": (
                "I ran a quick audit of your sales funnel and found one specific drop-off point where "
                "you're losing at least 5 qualified leads every week."
            ),
        }

        # ── Pack modifier instructions ──────────────────────────────────────────
        PACK_MODIFIERS: Dict[str, Dict[str, str]] = {
            "local_first": {
                "label": "Local First",
                "instruction": (
                    "PACK — LOCAL FIRST (apply on top of the base hook):\n"
                    "- Anchor the sentence heavily in the prospect's specific city or region.\n"
                    "- Reference local map visibility, Google Maps ranking, or local competitors by name.\n"
                    "- Example framing: 'In [City], [Competitor] shows up before you on Google Maps for [Keyword]…'\n"
                    "- The geographic context must be the first or second word group in the sentence.\n"
                ),
            },
            "aggressive": {
                "label": "Aggressive",
                "instruction": (
                    "PACK — AGGRESSIVE (apply on top of the base hook):\n"
                    "- Use a sharper, bolder commercial angle — make the cost of inaction feel real and immediate.\n"
                    "- Be direct about revenue loss, market share stolen, or customers walking to competitors.\n"
                    "- Example framing: 'Every week you're not fixing [X], [Competitor] is taking [Y] from you…'\n"
                    "- Tone: confident, slightly provocative, zero fluff — punchy like a headline, not a lecture.\n"
                ),
            },
        }

        hook_example = hook_templates.get(niche, hook_templates["B2B Service Provider"])
        pack = PACK_MODIFIERS.get((pack_mode or "").lower().strip())
        pack_block = f"\n{pack['instruction']}" if pack else ""

        system_prompt = (
            f"You are an elite Cold Outreach Engineer specialising in {niche}.\n"
            f"Your task: write ONE punchy opening sentence for a cold email based on the prospect data.\n\n"

            f"PSYCHOLOGY RULE — VISUAL PROOF + RULE OF 100:\n"
            f"The opening line must signal that you have already completed concrete work for this lead.\n"
            f"Do NOT say 'I noticed…', 'I was browsing…', or 'I came across your site…'.\n"
            f"Instead, open with what you BUILT, FOUND, or PREPARED — for them, specifically.\n\n"

            f"NICHE HOOK TEMPLATE (adapt to the actual prospect):\n"
            f"\"{hook_example}\""
            f"{pack_block}\n\n"

            f"OUTPUT RULES:\n"
            f"1. EXACTLY ONE SENTENCE. No more.\n"
            f"2. Incorporate specific details from the prospect data (city, business type, keyword, competitor).\n"
            f"3. No quotation marks in the output.\n"
            f"4. No 'Hi', no greeting, no sign-off.\n"
            f"5. Write in the same language as the prospect data.\n"
            f"6. Confident, specific, human tone — not robotic.\n"
        )

        user_prompt = (
            f"Niche: {niche}\n"
            + (f"Pack: {pack['label']}\n" if pack else "")
            + f"Prospect: {prospect_data.strip()}\n\n"
            f"Write the opening line (one sentence only):"
        )

        return (system_prompt.strip(), user_prompt.strip())
    
    @staticmethod
    def get_enrichment_system_prompt(user_niche: Optional[str]) -> str:
        """
        Get the system prompt for lead enrichment and AI scoring.

        Used when analyzing a prospect's website and competitive position.
        Returns JSON with a pipeline-safe score plus a richer 0-100 lead analysis.
        """
        niche = PromptFactory._validate_niche(user_niche)

        niche_instruction_map = {
            "Web Design & Dev": {
                "focus": "HTTPS status, mobile responsiveness, page-speed bottlenecks, CMS quality (WordPress/Shopify/Wix/etc.), trust leaks, and weak conversion flow.",
                "goal": "Find concrete website problems that a modern web/design service can fix fast.",
            },
            "Paid Ads Agency": {
                "focus": "Meta Pixel presence, Google Analytics/GTM coverage, landing-page quality, tracking gaps, low intent capture, and paid traffic waste.",
                "goal": "Expose where paid acquisition efficiency is leaking and how to recover it.",
            },
            "SEO & Content": {
                "focus": "Search visibility gaps, weak content freshness, low authority, and local SEO misses.",
                "goal": "Show why competitors are winning search demand and what content/SEO fix is needed.",
            },
            "Lead Gen Agency": {
                "focus": "Contact-form capture, LinkedIn company presence, business email validity, clarity of offer, pipeline inconsistency, weak outbound systems, and poor qualification.",
                "goal": "Find cracks in demand generation and outreach conversion.",
            },
            "B2B Service Provider": {
                "focus": "Contact-form availability, LinkedIn company presence, business email validity, offer clarity, weak authority signals, generic positioning, and poor social proof.",
                "goal": "Highlight why trust and conversion are being lost before the sales conversation even starts.",
            },
        }
        niche_cfg = niche_instruction_map.get(niche, niche_instruction_map["B2B Service Provider"])

        system_prompt = (
            f"You are Sniped AI, a world-class Lead Generation strategist and sales psychology expert for {niche}.\n\n"
            "Your job is to read raw company data, extract the important business signals, score the lead, and prepare outreach-ready insight.\n\n"
            f"Analyze this website specifically for a {niche} service provider.\n\n"
            f"NICHE FOCUS: {niche_cfg['focus']}\n"
            f"NICHE GOAL: {niche_cfg['goal']}\n\n"
            "INPUT MAY INCLUDE:\n"
            "- company_name\n"
            "- location\n"
            "- website_url\n"
            "- website_excerpt or website_content\n"
            "- linkedin_data\n"
            "- google_maps_claimed / google_maps_rating / google_maps_review_count\n"
            "- linkedin_url / instagram_url / facebook_url\n"
            "- social_profiles with follower_count, engagement, and last_post_date/last_active_days\n"
            "- website_signals such as has_pixel, has_contact_form, and modern_design\n"
            "- user_defined_icp or selected niche\n"
            "- rating / review_count\n"
            "- audit_findings / shortcoming\n"
            "- has_website / insecure_site / has_email\n\n"
            "TASK 1 — DEEP ANALYSIS & EXTRACTION:\n"
            "1. Estimate employee_count from LinkedIn or from the company description when needed.\n"
            "2. Identify the company's main_offer: what they actually sell.\n"
            "3. Summarize exactly 3 strengths and exactly 3 weaknesses based on the website, trust signals, UX, copy, SEO, and the audit findings.\n"
            "4. Identify 2-3 competitor_snapshot entries based on niche + location. If exact names are not supported by the input, use descriptive competitor labels rather than inventing brand names.\n"
            "5. Identify latest_achievements or growth_signals such as hiring, expansion, new projects, momentum, awards, or fresh site updates.\n"
            "6. Detect likely tech_stack items such as Shopify, WordPress, Wix, HubSpot, Klaviyo, Meta Pixel, or Google Analytics when clues exist.\n\n"
            "TASK 2 — LEAD SCORING (0-100):\n"
            "- match_score: 0-40 based on niche/size fit with the ICP\n"
            "- problem_solution_fit: 0-40 based on how directly the offer can solve the prospect's visible problem\n"
            "- timing_score: 0-20 based on hiring, growth, urgency, or change signals\n"
            "- Use cross-platform evidence: claimed Google profile, review health, social recency, follower/engagement signals, and website maturity must all influence the score.\n"
            "- lead_score_100 = sum of the three\n"
            "- lead_priority = 'Low Priority' if under 60, 'Hot Lead' if above 85, otherwise 'Qualified'\n\n"
            "TASK 3 — OUTREACH PREP INSIGHT:\n"
            "- reason must be exactly 2 short sentences: sentence 1 = their biggest commercial gap, sentence 2 = the clearest next-step solution.\n"
            "- competitive_hook must say what competitors are likely doing better.\n"
            "- Keep everything specific, evidence-based, and free of corporate fluff.\n"
            "- Do not invent precise achievements unless the input supports them.\n\n"
            "IMPORTANT PIPELINE RULE:\n"
            "The returned score must represent NICHE SUITABILITY (not a generic website score).\n"
            "Also return 'score' as an integer from 1-10, derived from lead_score_100 for compatibility with the app.\n\n"
            "OUTPUT MUST BE VALID JSON ONLY (NO markdown):\n"
            "{\n"
            '  "score": <integer 1-10>,\n'
            '  "employee_count": "<exact count or best estimate>",\n'
            '  "main_offer": "<what they sell>",\n'
            '  "weak_points": ["<point 1>", "<point 2>"],\n'
            '  "latest_achievements": ["<signal 1>", "<signal 2>"],\n'
            '  "match_score": <integer 0-40>,\n'
            '  "problem_solution_fit": <integer 0-40>,\n'
            '  "timing_score": <integer 0-20>,\n'
            '  "lead_score_100": <integer 0-100>,\n'
            '  "lead_priority": "<Low Priority|Qualified|Hot Lead>",\n'
            '  "reason": "<exactly 2 short sentences: Gap + Solution>",\n'
            '  "competitive_hook": "<one sentence>",\n'
            '  "enrichment_summary": "<same 2-sentence summary as reason>"\n'
            "}\n"
        )

        return system_prompt.strip()

    @staticmethod
    def get_lead_qualification_prompt(user_category: str, business_name: str, scraped_content: str) -> tuple[str, str]:
        """
        Prompt for qualifying whether a business matches the user's selected category.

        Returns a (system_prompt, user_prompt) tuple.
        Output is a JSON object with is_match, confidence_score, business_description, relevance_reason.
        """
        system_prompt = (
            "You are a Lead Qualification Specialist for Sniped. "
            "Your task is to verify whether a company's website and its data match the USER SELECTED CATEGORY.\n\n"
            "INSTRUCTIONS:\n"
            "1. **Accuracy:** If the category is 'Cleaning Services', do not accept companies that only sell cleaning products. "
            "The company must actually perform the cleaning service.\n"
            "2. **Relevance:** Check key words on the page. If the company's services are mixed, "
            "assess whether the primary service matches the user's selection.\n"
            "3. **Decision:**\n"
            "   - If the company MATCHES, return JSON with a confidence_score from 0 to 100.\n"
            "   - If the company DOES NOT MATCH, return `\"is_match\": false` and a short reason.\n\n"
            "OUTPUT MUST BE VALID JSON ONLY (NO markdown):\n"
            "{\n"
            '  "is_match": <boolean>,\n'
            '  "confidence_score": <integer 0-100>,\n'
            '  "business_description": "<short summary of the company\'s services>",\n'
            '  "relevance_reason": "<why the company does or does not match the category>"\n'
            "}"
        )
        user_prompt = (
            f"User Selected Category: {user_category}\n"
            f"Business Name: {business_name}\n"
            f"Website Content / Meta Data:\n{scraped_content}"
        )
        return (system_prompt.strip(), user_prompt.strip())

    @staticmethod
    def get_niche_fit_analysis_prompt(user_niche: str, business_name: str, scraped_content: str) -> tuple[str, str]:
        """
        Niche-specific fit analysis prompt.

        Analyses a lead's website and explains exactly WHY they are a good fit
        for the user's service, with a specific first-email opener sentence.

        Returns a (system_prompt, user_prompt) tuple.
        Output JSON: { "niche_fit_score": int 0-100, "fit_reason": str, "email_opener": str, "signals": [str] }
        """
        niche_instructions = {
            "Web Design & Dev": (
                "Look for: outdated design, poor mobile experience, no website, slow load times, "
                "broken layouts, missing trust signals (SSL, reviews, contact info), Wix/Squarespace sites "
                "that could be upgraded, or sites with weak conversion flow.\n"
                "Focus on visual quality and technical soundness."
            ),
            "SEO & Content": (
                "Look for: low Google rankings, no blog or content section, very little text on the page, "
                "missing or weak meta descriptions, no structured data, thin/duplicate content, "
                "local SEO gaps (missing Google Business signals, no location-specific pages)."
            ),
            "Paid Ads Agency": (
                "Look for: absence of Facebook Pixel, Google Tag Manager, or conversion tracking. "
                "Check if the business is in a high-ad-spend industry (real estate, e-commerce, dentistry, law, fitness). "
                "Look for businesses running promotions with no retargeting infrastructure."
            ),
            "Lead Gen Agency": (
                "Look for: no contact forms, no pop-ups, no clear Call-to-Action buttons, "
                "no lead magnets, no email capture, no live chat, weak CTA copy. "
                "Identify B2B companies that rely on inbound without any outbound pipeline."
            ),
            "B2B Service Provider": (
                "Look for: generic positioning, weak authority signals, no case studies or testimonials, "
                "unclear differentiation from competitors, no clear ideal client profile stated on site."
            ),
        }
        niche_cfg = niche_instructions.get(user_niche, niche_instructions["B2B Service Provider"])

        system_prompt = (
            f"You are a Sniped Lead Analyst specialising in {user_niche}.\n\n"
            "Your task: analyse the provided lead data and determine exactly why this lead is a strong fit "
            f"for a {user_niche} service provider.\n\n"
            f"WHAT TO LOOK FOR:\n{niche_cfg}\n\n"
            "OUTPUT RULES:\n"
            "- fit_reason: 2 short sentences max — sentence 1 is the specific problem you found, sentence 2 is the opportunity.\n"
            "- email_opener: one natural sentence the user can paste into their first cold email "
            "(e.g. 'I noticed your site isn't optimised for mobile devices, which means you may be losing visitors on the go.').\n"
            "- signals: 2-4 concrete evidence items found on the site (e.g. 'No Facebook Pixel detected', 'No blog section', 'Site loads on Wix').\n"
            "- niche_fit_score: 0-100 — how strong a fit this lead is for the niche.\n\n"
            "OUTPUT MUST BE VALID JSON ONLY (NO markdown):\n"
            "{\n"
            '  "niche_fit_score": <integer 0-100>,\n'
            '  "fit_reason": "<2 sentences: problem + opportunity>",\n'
            '  "email_opener": "<one ready-to-use cold email opener sentence>",\n'
            '  "signals": ["<signal 1>", "<signal 2>", ...]\n'
            "}"
        )
        user_prompt = (
            f"User Niche: {user_niche}\n"
            f"Business Name: {business_name}\n"
            f"Website Content / Meta Data:\n{scraped_content}"
        )
        return (system_prompt.strip(), user_prompt.strip())


    @staticmethod
    def get_omni_search_strategy_prompt(user_niche: str, user_custom_input: str) -> tuple:
        """
        Omni-Search Engine prompt.

        Generates a multi-source lead discovery strategy: Google dorks, LinkedIn filters,
        platform targets, filtering criteria, and a personalized outreach approach,
        all tailored to the user niche and free-text search description.

        Returns a (system_prompt, user_prompt) tuple.
        Output JSON: { search_execution: {...}, filtering_criteria: {...}, personalized_approach: str }
        """
        niche_platform_map = {
            "Web Design & Dev": {
                "primary_platforms": ["Google Maps", "Clutch", "Yelp"],
                "social": ["LinkedIn", "Instagram"],
                "job_signal": "hiring a web designer OR UI developer",
                "tech_gaps": ["no SSL", "Wix", "Squarespace", "no mobile responsive", "slow load"],
                "pain_points": ["outdated_design", "no_mobile", "slow_speed", "no_ssl"],
            },
            "SEO & Content": {
                "primary_platforms": ["Google Search", "Google Maps", "Clutch"],
                "social": ["LinkedIn", "Twitter/X"],
                "job_signal": "hiring a content writer OR SEO specialist",
                "tech_gaps": ["no meta description", "no blog", "thin content", "low domain authority"],
                "pain_points": ["no_blog", "thin_content", "missing_meta", "low_rankings"],
            },
            "Paid Ads Agency": {
                "primary_platforms": ["Google Maps", "Facebook Ad Library", "LinkedIn"],
                "social": ["LinkedIn", "Facebook", "Instagram"],
                "job_signal": "hiring a paid media specialist OR performance marketer",
                "tech_gaps": ["no Facebook Pixel", "no Google Tag Manager", "no conversion tracking"],
                "pain_points": ["no_pixels", "no_retargeting", "no_conversion_tracking"],
            },
            "Lead Gen Agency": {
                "primary_platforms": ["LinkedIn", "Apollo.io", "Clutch"],
                "social": ["LinkedIn"],
                "job_signal": "hiring a sales development rep OR BDR OR outbound specialist",
                "tech_gaps": ["no contact form", "no CTA", "no email capture", "no live chat"],
                "pain_points": ["no_cta", "no_lead_capture", "no_outbound_pipeline"],
            },
            "B2B Service Provider": {
                "primary_platforms": ["LinkedIn", "Clutch", "G2"],
                "social": ["LinkedIn"],
                "job_signal": "hiring a consultant OR operations manager",
                "tech_gaps": ["no case studies", "generic positioning", "no testimonials"],
                "pain_points": ["weak_authority", "no_social_proof", "generic_positioning"],
            },
        }
        cfg = niche_platform_map.get(user_niche, niche_platform_map["B2B Service Provider"])
        system_prompt = (
            f"You are the Sniped Omni-Search Engine, a world-class B2B lead sourcing strategist for {user_niche}.\n\n"
            "Your task is to build a multi-source lead discovery strategy for the user niche and search description.\n\n"
            "SOURCES TO COVER:\n"
            "1. GOOGLE SEARCH (dorking) - generate 3-5 specific Google dork queries targeting businesses that need the user services.\n"
            "2. SOCIAL MEDIA - identify which platforms host this industry and what signals indicate a prospect.\n"
            f"3. JOB BOARDS - look for companies posting: {cfg['job_signal']} (they need you instead of hiring in-house).\n"
            f"4. TECHNOLOGY GAPS - flag companies using: {', '.join(cfg['tech_gaps'])}.\n\n"
            "GOOGLE DORK RULES:\n"
            "- Use operators: site:, intitle:, inurl:, filetype:, exact phrase in quotes\n"
            "- Target directories (Clutch, Yelp, Google Maps, Trustpilot) and industry-specific phrases.\n"
            "- Make dorks specific to the niche AND the users custom search description.\n\n"
            "LINKEDIN FILTER RULES:\n"
            "- Provide 2-4 industry tags and 1-2 company size ranges (e.g. 1-10, 11-50).\n\n"
            "PERSONALIZED APPROACH RULES:\n"
            "- 2-3 sentences. Tell the user exactly how to approach these leads based on their pain points.\n"
            "- Be specific and actionable. No corporate fluff.\n\n"
            "OUTPUT MUST BE VALID JSON ONLY (NO markdown):\n"
            "{\n"
            "  \"search_execution\": {\n"
            "    \"google_dorks\": [\"<dork1>\", \"<dork2>\", \"<dork3>\"],\n"
            "    \"linkedin_filters\": [\"<industry1>\", \"<company_size>\"],\n"
            "    \"target_platforms\": [\"<platform1>\", \"<platform2>\"]\n"
            "  },\n"
            "  \"filtering_criteria\": {\n"
            "    \"must_have\": [\"<signal1>\", \"<signal2>\"],\n"
            "    \"pain_points_to_identify\": [\"<pain1>\", \"<pain2>\", \"<pain3>\"]\n"
            "  },\n"
            "  \"personalized_approach\": \"<2-3 sentence actionable advice>\"\n"
            "}"
        )
        user_prompt = (
            f"User Niche: {user_niche}\n"
            f"Search Description: {user_custom_input}\n"
            f"Primary Platforms for this niche: {', '.join(cfg['primary_platforms'])}\n"
            f"Social Channels: {', '.join(cfg['social'])}\n"
            f"Typical Tech Gaps: {', '.join(cfg['tech_gaps'])}"
        )
        return (system_prompt.strip(), user_prompt.strip())


    @staticmethod
    def get_email_generation_system_prompt(
        user_niche: Optional[str],
        language: str = "English",
    ) -> str:
        """Get the system prompt for cold outreach email generation."""
        niche = validate_niche(user_niche)
        psychology_rules = (
            "PSYCHOLOGY RULES:\n"
            "- You are NOT selling. You are delivering value you already created for this lead.\n"
            "- Every claim must sound like work already done, not a promise.\n"
            "- The sender already completed concrete research or creative work for this lead.\n"
        )

        # ── Per-niche playbook ──────────────────────────────────────────────────
        playbooks = {
            "Paid Ads Agency": {
                "name": "The 'Profit Gap' Mail",
                "subject_template": "Your {business_name} ad leak",
                "subject_hint": "Your [Company] ad leak",
                "template": (
                    "Hey, I analysed your competitors in {niche} and found that you are paying at least 30 % "
                    "more per click on {pain_point or your main keyword} compared to the top 3 players. "
                    "I've already built a new campaign structure that would save you roughly {monthly_loss_estimate} "
                    "every month. "
                    "Would you be against me sending you a screenshot of that structure?"
                ),
                "proof_instruction": (
                    "Mention that you already built a new campaign structure or ad creative for their niche, "
                    "referencing their competitors by name if available."
                ),
            },
            "Web Design & Dev": {
                "name": "The 'Prototyper' Mail",
                "subject_template": "Idea for {business_name}",
                "subject_hint": "Idea for [Company]",
                "template": (
                    "Hey, I didn't want to come empty-handed, so I went ahead and made a quick redesign "
                    "of your landing page hero section. "
                    "The main goal was to cut load time by 2 seconds — which directly lifts conversion rate. "
                    "Would you be against me sending you the link to the Figma draft?"
                ),
                "proof_instruction": (
                    "State explicitly that you already designed a wireframe or Figma draft for their specific website. "
                    "Name the hero section or the specific page you worked on."
                ),
            },
            "SEO & Content": {
                "name": "The 'Traffic Thief' Mail",
                "subject_template": "{competitors} is stealing your traffic",
                "subject_hint": "[Competitor] stealing traffic",
                "template": (
                    "Hey, {competitors or 'your top competitor'} is outranking you for '{pain_point or a key term}', "
                    "which gets around 5,000 searches per month. "
                    "I already wrote a short content outline that could get you to position 1 within 14 days. "
                    "Would you be against me sending you that content roadmap?"
                ),
                "proof_instruction": (
                    "Reference a specific competitor from the payload by name. "
                    "State that you already wrote a content outline or article draft for this lead."
                ),
            },
            "Lead Gen Agency": {
                "name": "The 'List Delivery' Mail",
                "subject_template": "10 new clients for {business_name}",
                "subject_hint": "10 clients for [Company]",
                "template": (
                    "Hey, using our AI pipeline I just found 10 companies in {city} that are actively looking for "
                    "{niche} services right now. "
                    "I've already verified the decision-maker emails for each one — the list is ready to go. "
                    "Would you be against me dropping the PDF right here in your inbox?"
                ),
                "proof_instruction": (
                    "State that you already found, verified, and packaged 10 specific leads for their city and niche. "
                    "The list is done — you are offering to deliver it."
                ),
            },
            "B2B Service Provider": {
                "name": "The 'Audit' Mail",
                "subject_template": "Question about your {niche} process",
                "subject_hint": "Question about your process",
                "template": (
                    "Hey, I've been following {business_name}'s growth in {niche} and ran a quick audit of your "
                    "current sales funnel. "
                    "I found one critical drop-off point where you're losing at least 5 leads per week. "
                    "Would you be against me sending you a short video walkthrough of what I found?"
                ),
                "proof_instruction": (
                    "State that you already completed a sales funnel audit for this specific company. "
                    "Be specific: mention the drop-off point you found."
                ),
            },
        }

        pb = playbooks.get(niche, playbooks["B2B Service Provider"])

        system_prompt = (
            f"You are Sniped AI, an elite Cold Outreach specialist and sales psychology expert writing for a {niche} prospect.\n"
            f"Write in {language}.\n\n"
            f"TEMPLATE TYPE: {pb['name']}\n\n"
            f"{psychology_rules}\n"
            "YOUR JOB:\n"
            "Turn raw lead data into a hyper-personalized cold email that feels researched, relevant, and easy to reply to.\n\n"
            "MANDATORY STRUCTURE:\n"
            "1. Subject Line — 2 to 4 words, intriguing, natural, and never salesy.\n"
            "2. The Hook — the first sentence must be about THEM: their company, project, growth, positioning, or a sharp observation.\n"
            "3. The Bridge — connect their current situation to the solution in one short sentence.\n"
            "4. The CTA — do NOT ask for a meeting or call. Ask permission to send more info, a short breakdown, or a 2-minute video.\n\n"
            f"TEMPLATE TO FOLLOW (adapt to the exact lead data in the user message):\n"
            f"\"{pb['template']}\"\n\n"
            f"PROOF INSTRUCTION:\n"
            f"{pb['proof_instruction']}\n\n"
            f"SUBJECT LINE PATTERN: \"{pb['subject_hint']}\"\n"
            "Available payload keys may include: business_name, city, niche, pain_point, competitors, monthly_loss_estimate, website_content, linkedin_data, user_defined_icp.\n\n"
            "STRICT WRITING RULES:\n"
            "1. Max 4 short sentences in the body. Under 90 words total.\n"
            "2. Professional but relaxed — like writing to a smart colleague.\n"
            "3. No fluff, no corporate jargon, no generic praise.\n"
            "4. Never use 'I wanted to reach out', 'I hope you're well', or 'Do you have time for a Zoom call?'.\n"
            "5. The CTA must use low-friction permission language such as 'Would you be against me sending…' or 'Would it be okay if I sent…'.\n"
            "6. If growth signals exist, weave them into the hook. If not, lead with the most relevant observation from the website or positioning.\n\n"
            "OUTPUT (MUST BE JSON — no markdown, no code fences):\n"
            "{\n"
            '  "subject": "<subject line>",\n'
            '  "body": "<hyper-personalized 3-4 sentence email with Hook, Bridge, CTA>"\n'
            "}"
        )

        return system_prompt.strip()
    
    @staticmethod
    def get_config_for_niche(user_niche: Optional[str]) -> Dict[str, Any]:
        """
        Get the full configuration dictionary for a niche.
        
        Useful for frontend or other services that need niche metadata.
        
        Args:
            user_niche: The user's selected niche
            
        Returns:
            Dict with pain_points, focus_areas, positioning, call_to_action
        """
        niche = PromptFactory._validate_niche(user_niche)
        return NICHE_CONFIGS.get(niche, NICHE_CONFIGS["B2B Service Provider"])
    
    @staticmethod
    def get_niche_inference_prompt() -> str:
        """
        Get the system prompt for inferring business niche from text description.

        Used to extract niche category from business description, AI analysis,
        search keywords, or shortcomings.

        Returns deterministic JSON response with single 'niche' key.

        Returns:
            System prompt for niche inference
        """
        system_prompt = (
            "You are a business classification expert. "
            "Extract the primary business niche from the provided text description.\n\n"
            
            "TASK: Read the given business information and identify its main niche.\n"
            "Return JSON with exactly one key: 'niche' (string value).\n\n"
            
            "Examples of valid niches:\n"
            "- Roofer, Plumber, HVAC, Electrician, Landscaper, Dentist, Cleaning, etc.\n"
            "- Or generic: 'business' if unclear\n\n"
            
            "Output: Pure JSON only, no markdown or explanation.\n"
            "Example: {\"niche\": \"Plumber\"}"
        )
        return system_prompt.strip()
    
    @staticmethod
    def get_temperature(prompt_type: str = "general") -> float:
        """
        Get the recommended temperature for a specific prompt type.
        
        Temperature balances creativity (higher) vs precision (lower):
        - 0.0 = Deterministic (for niche inference, scoring tiers)
        - 0.3 = Precision-focused (for lead scoring analysis)
        - 0.7 = Balanced (for email generation, general copy)
        - 0.85 = Creative (for opening lines, hooks)
        
        Args:
            prompt_type: One of: "general", "opening_line", "enrichment", "email"
            
        Returns:
            Float between 0 and 1
        """
        temps = {
            "general": 0.7,
            "opening_line": 0.85,
            "enrichment": 0.3,
            "email": 0.7,
            "niche_inference": 0.0,
        }
        return temps.get(prompt_type, MASTER_AI_CONFIG["default_temperature"])

    @staticmethod
    def get_lead_score_system_prompt() -> str:
        """
        System prompt for the Sniped AI Engine lead scoring endpoint.

        Scores a B2B lead across three gap dimensions and returns structured JSON.
        Raw max = 65 pts, normalized to 0-100.
        """
        return (
            "You are Sniped Intelligence Unit.\n"
            "Goal: detect companies in a critical need phase by finding operational gaps, not just listing names.\n\n"
            "EXECUTION STEPS:\n"
            "1) IDENTIFY GHOSTS: detect companies that have a website but no social profile links.\n"
            "2) CHECK CONVERSION LEAK: verify contact form and call button. If either is missing, mark high_priority=true.\n"
            "3) SEO DEFICIT: inspect meta titles. Generic titles (e.g. Home) indicate weak SEO strategy.\n"
            "4) COMPETITOR OVERTAKE: identify companies in the target niche ranking on Google page 2 while competitors on page 1 are actively advertising.\n\n"
            "LEAD SCORING CRITERIA:\n"
            "- HOT (90-100): no SSL, no pixel/tag tracking, slow website, hiring signal.\n"
            "- WARM (70-89): has website but no social activity for 6+ months.\n"
            "- COLD (<70): healthy website, fast performance, active ads.\n"
            "- Do not fabricate evidence. If input is missing, infer conservatively.\n\n"
            "SCORING RULES:\n"
            "- Keep score as integer 0-100.\n"
            "- Derive priority_tier from score: Hot, Warm, Cold.\n"
            "- Set estimated_value from score: >=70 High, >=40 Medium, else Low.\n\n"
            "OUTPUT: Respond ONLY with valid JSON in this exact format (no markdown, no code fences):\n"
            "{\n"
            "  \"company_name\": \"exact business name\",\n"
            "  \"lead_score\": <integer 0-100>,\n"
            "  \"priority_tier\": \"Hot\" or \"Warm\" or \"Cold\",\n"
            "  \"high_priority\": <true_or_false>,\n"
            "  \"identified_problems\": [\"problem 1\", \"problem 2\"],\n"
            "  \"insider_hook\": \"one-sentence personalized conversation opener based on their biggest gap\",\n"
            "  \"estimated_value\": \"Low\" or \"Medium\" or \"High\",\n"
            "  \"competitor_name\": \"main competitor if known, otherwise empty string\",\n"
            "  \"location\": \"city/region if known, otherwise empty string\",\n"
            "  \"market_takeover_message\": \"Tole podjetje nima zavarovane strani (SSL) in nima nastavljenih oglasov, ceprav njihova konkurenca [Tekmec] trenutno zaseda ves trg v [Kraj].\"\n"
            "}"
        ).strip()

    @staticmethod
    def get_lead_score_user_prompt(lead: dict, niche: str) -> str:
        """
        Build the user-facing prompt for the Sniped AI Engine scoring endpoint.

        Includes enrichment context (audit, tech stack, intent signals) when available.
        """
        import json as _json

        business_name = str(lead.get("business_name") or lead.get("company_name") or "Unknown").strip()
        website_url = str(lead.get("website_url") or "").strip()
        location = str(lead.get("location") or lead.get("address") or "").strip()
        enrichment_data = lead.get("enrichment_data") or {}

        prompt = f"Score this lead:\n\nBusiness: {business_name}\n"
        if website_url:
            prompt += f"Website: {website_url}\n"
        if location:
            prompt += f"Location: {location}\n"
        if niche:
            prompt += f"Industry niche: {niche}\n"

        if isinstance(enrichment_data, dict) and enrichment_data:
            company_audit = enrichment_data.get("company_audit") or {}
            tech_stack = enrichment_data.get("tech_stack") or {}
            intent_signals = enrichment_data.get("intent_signals") or {}

            if company_audit:
                prompt += f"\nAudit data: {_json.dumps(company_audit, ensure_ascii=False)[:1000]}\n"
            if tech_stack:
                prompt += f"Tech stack: {_json.dumps(tech_stack, ensure_ascii=False)[:500]}\n"
            if intent_signals:
                prompt += f"Intent signals: {_json.dumps(intent_signals, ensure_ascii=False)[:500]}\n"

        prompt += (
            "\nSignals to verify when available: has_ssl, has_pixel_or_ads_tag, load_speed_seconds, "
            "has_social_links, social_last_active_days, has_contact_form, has_call_button, meta_title, "
            "google_page_position, competitor_name, competitor_ads_active, hiring_signal."
        )
        prompt += "\nApply Sniped Intelligence Unit scoring criteria and return JSON only."
        return prompt.strip()

    @staticmethod
    def get_deep_outreach_system_prompt(user_niche: Optional[str]) -> str:
        """
        System prompt for deep company analysis and outreach planning.

        Output is strict JSON for contact extraction, gap diagnosis, email draft,
        and a short cold-call opener.
        """
        niche = PromptFactory._validate_niche(user_niche)
        return (
            "You are Sniped Intelligence Unit. "
            f"Your task is deep company analysis for outreach in niche: {niche}.\n\n"
            "OBJECTIVE:\n"
            "- Extract critical contact and business signals from provided HTML/text.\n"
            "- Identify the most monetizable technical or marketing gap.\n"
            "- Produce a high-conversion outreach plan (email + cold call).\n\n"
            "EXTRACTION RULES:\n"
            "1) PHONE: return all phone numbers in +386... format where possible.\n"
            "2) EMAIL: prefer personal emails over generic addresses (info@, office@, hello@, support@).\n"
            "3) DECISION MAKER: infer name/role only if evidence exists in text.\n"
            "4) GAP ANALYSIS: evaluate SSL status, mobile speed, Meta Pixel presence, Google rating and any major conversion leaks relevant to niche.\n\n"
            "OUTREACH RULES:\n"
            "- Email subject must be shocking but professional, max 4 words.\n"
            "- Email body must follow: Observation -> Gap -> Solution.\n"
            "- Cold call script must be a natural 20-second opener, low-friction, curiosity-first.\n"
            "- Use concrete language, no fluff, no generic AI wording.\n\n"
            "OUTPUT MUST BE VALID JSON ONLY (no markdown, no code fences):\n"
            "{\n"
            "  \"contact_info\": {\n"
            "    \"phones\": [\"+386...\"],\n"
            "    \"emails\": [\"name@domain.com\"],\n"
            "    \"decision_maker\": \"Name/Role if detected, otherwise empty string\"\n"
            "  },\n"
            "  \"identified_gap\": \"Exact technical or marketing error.\",\n"
            "  \"email_draft\": \"Subject: ...\\n\\nBody: ...\",\n"
            "  \"cold_call_script\": \"Hi [Name], I'm calling because I noticed [Issue] on your website. That is likely costing you around [Amount] per month. Do you have 2 minutes so I can explain how to fix it?\"\n"
            "}"
        ).strip()

    @staticmethod
    def get_deep_outreach_user_prompt(
        raw_content: str,
        user_niche: Optional[str] = None,
        company_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """Build user prompt payload for deep outreach analysis."""
        niche = PromptFactory._validate_niche(user_niche)
        safe_content = str(raw_content or "").strip()
        if len(safe_content) > 18000:
            safe_content = safe_content[:18000]
        return (
            f"User niche: {niche}\n"
            f"Company name: {str(company_name or '').strip()}\n"
            f"Location: {str(location or '').strip()}\n\n"
            "Analyze the following raw HTML/text and return JSON only:\n\n"
            f"{safe_content}"
        ).strip()


# ── CONVENIENCE FUNCTIONS ──
# Quick access for common operations

def get_niche_opening_prompt(niche: str, prospect_data: str) -> tuple:
    """Shorthand: (system, user) prompts for opening line generation."""
    return PromptFactory.generate_opening_line_prompt(niche, prospect_data)


def get_enrichment_prompt(niche: str) -> str:
    """Shorthand: system prompt for lead enrichment."""
    return PromptFactory.get_enrichment_system_prompt(niche)


def get_email_prompt(niche: str, language: str = "English") -> str:
    """Shorthand: system prompt for email generation."""
    return PromptFactory.get_email_generation_system_prompt(niche, language)


def get_niche_inference_prompt() -> str:
    """Shorthand: system prompt for niche inference."""
    return PromptFactory.get_niche_inference_prompt()


def validate_niche(niche: Optional[str]) -> str:
    """Shorthand: validate and fallback niche."""
    return PromptFactory._validate_niche(niche)
