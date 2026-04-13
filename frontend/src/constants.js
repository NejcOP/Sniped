// ============================================================
// Sniped – Global Constants (Single Source of Truth)
// All frontend components must import from this file.
// ============================================================

export const ALLOWED_NICHES = [
  'Paid Ads Agency',
  'Web Design & Dev',
  'SEO & Content',
  'Lead Gen Agency',
  'B2B Service Provider',
]

export const NICHE_DESCRIPTIONS = {
  'Paid Ads Agency': 'Meta, Google Ads, TikTok campaigns',
  'Web Design & Dev': 'Websites, landing pages, web apps',
  'SEO & Content': 'Organic growth, blog, search visibility',
  'Lead Gen Agency': 'Pipeline automation, outbound systems',
  'B2B Service Provider': 'Consulting, coaching, B2B services',
}

export const NICHE_HINTS = {
  'Paid Ads Agency': 'e.g. "Dental clinic in London, running Facebook ads but no Meta Pixel installed"',
  'Web Design & Dev': 'e.g. "Auto mechanic in Chicago, website from 2015, doesn\'t work on mobile"',
  'SEO & Content': 'e.g. "Law firm in Austin, no blog, missing meta descriptions on all pages"',
  'Lead Gen Agency': 'e.g. "Construction company, LinkedIn inactive for 6 months, 45 followers"',
  'B2B Service Provider': 'e.g. "Accounting firm, contact only by email, no call button on their site"',
}

export const ACCOUNT_TYPES = ['entrepreneur', 'freelancer', 'agency', 'company']

export const ACCOUNT_TYPE_LABELS = {
  entrepreneur: 'Entrepreneur',
  freelancer: 'Freelancer',
  agency: 'Agency',
  company: 'Company',
}

export const ACCOUNT_TYPE_OPTIONS = ACCOUNT_TYPES.map((value) => ({
  value,
  label: ACCOUNT_TYPE_LABELS[value] || value,
}))

// Validation helper
export function isValidNiche(niche) {
  return ALLOWED_NICHES.includes(niche)
}

export function isValidAccountType(accountType) {
  return ACCOUNT_TYPES.includes(accountType)
}
