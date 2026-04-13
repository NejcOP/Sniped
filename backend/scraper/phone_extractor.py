"""
Lightning-Fast Contact Data Extractor
Regex-based phone number normalization with mobile/office classification
and international E.164 formatting. No external libraries required.
"""

from __future__ import annotations

import re
from typing import Optional

# ---------------------------------------------------------------------------
# Country dial-code prefixes (for auto-prepending when no country code found)
# ---------------------------------------------------------------------------
COUNTRY_DIAL_CODES: dict[str, str] = {
    "AE": "+971",
    "AL": "+355",
    "AR": "+54",
    "AT": "+43",
    "AU": "+61",
    "BA": "+387",
    "BE": "+32",
    "BG": "+359",
    "BR": "+55",
    "CA": "+1",
    "CH": "+41",
    "CL": "+56",
    "CN": "+86",
    "CO": "+57",
    "CZ": "+420",
    "DE": "+49",
    "DK": "+45",
    "EE": "+372",
    "ES": "+34",
    "FI": "+358",
    "FR": "+33",
    "GB": "+44",
    "GR": "+30",
    "HR": "+385",
    "HU": "+36",
    "IE": "+353",
    "IL": "+972",
    "IN": "+91",
    "IT": "+39",
    "JP": "+81",
    "KR": "+82",
    "LT": "+370",
    "LV": "+371",
    "ME": "+382",
    "MK": "+389",
    "MX": "+52",
    "NG": "+234",
    "NL": "+31",
    "NO": "+47",
    "NZ": "+64",
    "PL": "+48",
    "PT": "+351",
    "RO": "+40",
    "RS": "+381",
    "RU": "+7",
    "SA": "+966",
    "SE": "+46",
    "SG": "+65",
    "SI": "+386",
    "SK": "+421",
    "TR": "+90",
    "UA": "+380",
    "US": "+1",
    "ZA": "+27",
}

# Known country code prefixes (longest-first for greedy matching)
_KNOWN_COUNTRY_CODE_PREFIXES = sorted(
    set(COUNTRY_DIAL_CODES.values()), key=lambda x: -len(x)
)

# ---------------------------------------------------------------------------
# Mobile-number prefix patterns per country code  (digits only, no +)
# ---------------------------------------------------------------------------
_MOBILE_PREFIXES: dict[str, list[str]] = {
    "386": ["030", "031", "040", "041", "050", "051", "064", "065", "068", "069",
            "31", "40", "41", "51", "64", "65", "68", "69"],   # SI
    "49":  ["015", "016", "017"],                               # DE
    "43":  ["064", "065", "066", "067", "068"],                # AT
    "41":  ["075", "076", "077", "078", "079"],                # CH
    "385": ["091", "092", "095", "098", "099",
            "91", "92", "95", "98", "99"],                     # HR
    "44":  ["074", "075", "076", "077", "078", "079"],         # GB
    "1":   [],                                                   # US — too many; rely on keywords
}

# Context keywords that hint at mobile vs office
_MOBILE_KEYWORDS = frozenset([
    "mobile", "mobil", "mob", "cell", "gsm", "handy", "sms",
    "direct", "direkt", "whatsapp", "viber", "telegram",
])
_OFFICE_KEYWORDS = frozenset([
    "office", "büro", "pisarna", "main", "reception", "fax",
    "tel:", "telephone", "telefon", "phone:",
])

# Broad regex to find raw phone candidates in arbitrary text
_PHONE_CANDIDATE_RE = re.compile(
    r"(?<!\d)(\+?[0-9][0-9 ()./-]{5,}[0-9])(?!\d)"
)


# ---------------------------------------------------------------------------
# PhoneExtractor
# ---------------------------------------------------------------------------

class PhoneExtractor:
    """
    Extract, normalize, and classify phone numbers from unstructured text.

    Usage::

        pe = PhoneExtractor()
        result = pe.extract("+386 41 123 456")
        # {
        #   "phone_found": True,
        #   "primary_number": "+38641123456",
        #   "display": "+386 41 123 456",
        #   "type": "mobile",
        #   "confidence": 9,
        # }
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(
        self,
        text: str,
        country_hint: Optional[str] = None,
    ) -> dict:
        """
        Find the best phone number in *text*.

        Args:
            text: Raw string that may contain one or more phone numbers.
            country_hint: ISO-3166-1 alpha-2 code (e.g. ``"SI"``, ``"DE"``)
                          used when the number has no international prefix.

        Returns a dict with keys:
            phone_found (bool), primary_number (str | None),
            display (str | None), type (str), confidence (int 1-10).
        """
        if not text:
            return self._empty()

        candidates = self._find_candidates(text)
        if not candidates:
            return self._empty()

        scored = []
        for raw, context in candidates:
            norm = self._normalize(raw, country_hint)
            if norm is None:
                continue
            phone_type = self._classify_type(raw, context)
            conf = self._confidence(norm, phone_type)
            scored.append((conf, phone_type, norm, raw))

        if not scored:
            return self._empty()

        # Priority: highest confidence first; within same conf prefer mobile
        scored.sort(key=lambda x: (x[0], 0 if x[1] == "mobile" else 1), reverse=True)
        conf, phone_type, norm, raw = scored[0]

        return {
            "phone_found": True,
            "primary_number": norm,
            "display": self._format_display(norm),
            "type": phone_type,
            "confidence": conf,
        }

    def normalize(self, raw: str, country_hint: Optional[str] = None) -> Optional[str]:
        """Normalize a single raw phone string to E.164 (e.g. ``"+38641123456"``)."""
        return self._normalize(raw, country_hint)

    def classify(self, raw: str, context: str = "") -> str:
        """Return ``"mobile"``, ``"office"``, or ``"unknown"`` for *raw*."""
        return self._classify_type(raw, context)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_candidates(self, text: str) -> list[tuple[str, str]]:
        """Return list of (raw_number, context_snippet) pairs."""
        results = []
        for m in _PHONE_CANDIDATE_RE.finditer(text):
            raw = m.group(1)
            digits = re.sub(r"\D", "", raw)
            if len(digits) < 7:
                continue
            # Grab up to 30 chars around the match as context for keyword check
            start = max(0, m.start() - 30)
            end = min(len(text), m.end() + 30)
            context = text[start:end].lower()
            results.append((raw, context))
        return results

    def _normalize(self, raw: str, country_hint: Optional[str]) -> Optional[str]:
        """Strip formatting and return E.164 string or None if implausible."""
        # Strip common embedded labels, e.g. "Tel: +386…"
        raw = re.sub(r"^[A-Za-z .:/]+", "", raw).strip()
        digits_with_plus = re.sub(r"[^\d+]", "", raw)

        # Attempt to detect an international prefix from known codes
        if digits_with_plus.startswith("+"):
            candidate = digits_with_plus
        else:
            # Check if a leading zero should be stripped and country code prepended
            dial = COUNTRY_DIAL_CODES.get((country_hint or "").upper())
            if dial:
                digits_only = digits_with_plus.lstrip("0")
                candidate = dial + digits_only
            else:
                # No hint — keep as-is; mark as local
                candidate = "+" + digits_with_plus if not digits_with_plus.startswith("+") else digits_with_plus

        # Validate length: E.164 is 7–15 digits after +
        digits_only = re.sub(r"\D", "", candidate)
        if not (7 <= len(digits_only) <= 15):
            return None

        return "+" + digits_only

    def _classify_type(self, raw: str, context: str) -> str:
        """Classify as mobile/office/unknown using prefix tables and keywords."""
        ctx = context.lower()

        # Keyword-based (strongest signal)
        if any(kw in ctx for kw in _MOBILE_KEYWORDS):
            return "mobile"
        if any(kw in ctx for kw in _OFFICE_KEYWORDS):
            return "office"

        # Prefix-based — strip +/spaces, get country code prefix
        digits = re.sub(r"\D", "", raw).lstrip("0")
        for cc, prefixes in _MOBILE_PREFIXES.items():
            if digits.startswith(cc) and prefixes:
                local_part = digits[len(cc):]
                if any(local_part.startswith(p.lstrip("0")) for p in prefixes):
                    return "mobile"

        return "unknown"

    def _confidence(self, normalized: str, phone_type: str) -> int:
        """Score 1-10: higher for longer/typed numbers, lower for ambiguous."""
        digits = re.sub(r"\D", "", normalized)
        score = 5

        # Reward length bands
        if len(digits) >= 11:
            score += 2
        elif len(digits) >= 9:
            score += 1

        # Reward definite type
        if phone_type == "mobile":
            score += 2
        elif phone_type == "office":
            score += 1

        # Reward well-formed international prefix
        if normalized.startswith("+"):
            score += 1

        return min(score, 10)

    def _format_display(self, e164: str) -> str:
        """Return a human-friendly spaced version of an E.164 number."""
        if not e164.startswith("+"):
            return e164
        digits = e164[1:]

        # Try to match a known country code and format accordingly
        for prefix in _KNOWN_COUNTRY_CODE_PREFIXES:
            cc_digits = prefix[1:]  # strip the leading "+"
            if digits.startswith(cc_digits):
                local = digits[len(cc_digits):]
                # Group local part in blocks of 3
                groups = [local[i:i+3] for i in range(0, len(local), 3)]
                return f"{prefix} {' '.join(groups)}"

        # Fallback: space every 3 digits after first 2
        return e164

    @staticmethod
    def _empty() -> dict:
        return {
            "phone_found": False,
            "primary_number": None,
            "display": None,
            "type": "unknown",
            "confidence": 0,
        }
