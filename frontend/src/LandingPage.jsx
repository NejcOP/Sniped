import { useCallback, useEffect, useState } from 'react'
import { AnimatePresence, motion as Motion } from 'framer-motion'
import { useLocation } from 'react-router-dom'
import { getStoredValue } from './authStorage.js'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

const API_BASE = ''
const DEMO_VIDEO_EMBED_URL = String(import.meta.env.VITE_DEMO_VIDEO_URL || 'https://www.youtube.com/embed/ysz5S6PUM-U?rel=0').trim()

// Default A/B variant. Can be overridden with ?hero=control|aggressive|benefit
const DEFAULT_HERO_VARIANT = 'control'

const HERO_VARIANTS = {
  control: {
    headline: 'Find leads with a reason to reply',
    subheadline:
      'Sniped finds businesses with visible gaps - no website, weak SEO, slow pages, competitor gaps - and turns them into audit-backed cold emails your prospects actually care about.',
  },
  aggressive: {
    headline: 'Stop sending cold emails with no reason. Sniped writes the outreach angle for you.',
    subheadline:
      'Sniped finds businesses with visible gaps - no website, weak SEO, slow pages, competitor gaps - and turns them into audit-backed cold emails your prospects actually care about.',
  },
  benefit: {
    headline: 'Start 3.4x more conversations with audit-backed outbound.',
    subheadline:
      'Sniped finds businesses with visible gaps - no website, weak SEO, slow pages, competitor gaps - and turns them into audit-backed cold emails your prospects actually care about.',
  },
}

const HOW_IT_WORKS_CONTAINER_VARIANTS = {
  hidden: {},
  visible: {
    transition: {
      staggerChildren: 0.14,
      delayChildren: 0.08,
    },
  },
}

const HOW_IT_WORKS_ITEM_VARIANTS = {
  hidden: { opacity: 0, x: -24, y: 20 },
  visible: {
    opacity: 1,
    x: 0,
    y: 0,
    transition: {
      duration: 0.5,
      ease: [0.22, 1, 0.36, 1],
    },
  },
}

function trackLandingEvent(eventName, payload = {}) {
  if (typeof window === 'undefined') {
    return
  }

  if (Array.isArray(window.dataLayer)) {
    window.dataLayer.push({ event: eventName, ...payload })
  }

  if (typeof window.gtag === 'function') {
    window.gtag('event', eventName, payload)
  }
}

const TRUST_METRICS = [
  { value: '12M+', label: 'Businesses scanned for visible gaps' },
  { value: '89%', label: 'Teams report higher reply quality' },
  { value: '3.4x', label: 'Reply lift with audit-backed intros' },
  { value: '195+', label: 'Countries with active Sniped users' },
]

const HOW_IT_WORKS_STEPS = [
  {
    title: 'Step 1: Find the gap',
    text: 'Sniped scans niches and locations to find businesses with visible conversion or visibility problems.',
  },
  {
    title: 'Step 2: Build the audit',
    text: 'AI turns each gap into a simple explanation, proof, and recommended outreach angle.',
  },
  {
    title: 'Step 3: Send better outreach',
    text: 'Launch cold emails that start with relevance instead of generic sales pitches.',
  },
]

const EMAIL_COMPARISON = {
  generic: 'Hi, we help businesses grow online. Do you need a new website?',
  sniped:
    'Hi, I was checking {Niche} businesses in {City} and could not find a clear website for {BusinessName}. That usually means ready-to-buy customers choose competitors that look easier to trust. Want me to send a quick homepage concept?',
}

const AUDIT_USE_CASES = [
  {
    title: 'Web Design',
    text: 'Find businesses with no website or slow mobile UX.',
    badge: 'Missing Trust Layer',
  },
  {
    title: 'SEO',
    text: 'Find companies being outranked by competitors.',
    badge: 'Visibility Gap',
  },
  {
    title: 'Paid Ads',
    text: 'Find prospects where paid clicks leak due to weak landing pages.',
    badge: 'Leaking Ad Spend',
  },
]

const APP_PREVIEW_PANELS = [
  {
    title: 'Gap Scanner',
    stat: '248 audited leads',
    note: 'Matched by niche, city, and conversion signals',
  },
  {
    title: 'Gold Audit Builder',
    stat: '92% relevance score',
    note: 'Proof points pulled from website + competitor signals',
  },
  {
    title: 'Outbound Engine',
    stat: '41% open rate',
    note: 'Cold emails launched from audit-backed angles',
  },
]

const PRICING_PLANS = [
  {
    name: 'Starter',
    planId: 'hustler',
    subtitle: 'Gold Audit Foundations',
    price: '$49.99',
    period: '/mo',
    valueProp: 'Launch targeted outreach with core audit signals.',
    credits: '2,000 Credits/mo',
    trigger: 'First outbound system',
    features: [
      'Gap-based lead discovery by niche + city',
      'Core Gold Audit explanations and outreach angles',
      'Verified contact discovery',
      'CSV export for outreach workflows',
    ],
    accent: 'blue',
  },
  {
    name: 'Growth',
    planId: 'growth',
    subtitle: 'Audit-Led Pipeline',
    price: '$79.99',
    period: '/mo',
    valueProp: 'Scale reply-focused campaigns with deeper proof.',
    credits: '7,000 Credits/mo',
    trigger: 'Most popular',
    features: [
      'Expanded Gold Audit signal coverage',
      'Priority AI processing for faster list delivery',
      'Advanced niche + geo filtering',
      'Campaign-ready email personalization',
      'Bulk export + CRM handoff',
    ],
    accent: 'neon',
    popular: true,
  },
  {
    name: 'Agency',
    planId: 'scale',
    subtitle: 'Team Audit Operations',
    price: '$99.99',
    period: '/mo',
    valueProp: 'Run multi-client outbound from one audit engine.',
    credits: '20,000 Credits/mo',
    trigger: 'Agency throughput',
    features: [
      'Multi-user collaboration and assignment',
      'Automation workflows for repeated outreach',
      'AI lead scoring and segment prioritization',
      'Reporting dashboards for account performance',
      'Webhook + integrations for agency stack',
    ],
    accent: 'indigo',
  },
  {
    name: 'Elite',
    planId: 'empire',
    subtitle: 'Enterprise Audit Command',
    price: '$149.99',
    period: '/mo',
    valueProp: 'Maximum audit volume for serious outbound teams.',
    credits: '100,000 Credits/mo',
    trigger: 'High-volume precision',
    features: [
      'Up to 100k credits for large prospect pools',
      'Premium model routing for high-fit outreach',
      'Priority support and launch guidance',
      'Advanced deliverability controls',
      'Early access to advanced outbound features',
    ],
    accent: 'cyan',
  },
]

const VALID_PAID_PLAN_IDS = new Set(
  PRICING_PLANS
    .map((plan) => String(plan?.planId || '').trim().toLowerCase())
    .filter((planId) => planId && planId !== 'free'),
)

async function fetchJson(path, options) {
  const apiBase = String(import.meta.env.VITE_API_BASE_URL || '').trim().replace(/\/$/, '')
  const token = getStoredValue('lf_token')
  const headers = {
    ...(options?.headers || {}),
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }

  const requestUrl = /^https?:\/\//i.test(String(path || ''))
    ? String(path)
    : apiBase && String(path || '').startsWith('/api')
      ? `${apiBase}${path}`
      : `${API_BASE}${path}`
  const response = await fetch(requestUrl, { ...(options || {}), headers })
  const text = await response.text()
  let data = null

  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = { detail: text }
    }
  }

  if (!response.ok) {
    const message = data?.detail || `Request failed (${response.status})`
    throw new Error(message)
  }

  return data || {}
}

export default function LandingPage() {
  const location = useLocation()
  const [loadingPlanId, setLoadingPlanId] = useState('')
  const [isVideoModalOpen, setIsVideoModalOpen] = useState(false)
  const requestedHeroVariant = String(new URLSearchParams(location.search).get('hero') || '').trim().toLowerCase()
  const activeHeroVariant = Object.prototype.hasOwnProperty.call(HERO_VARIANTS, requestedHeroVariant)
    ? requestedHeroVariant
    : DEFAULT_HERO_VARIANT
  const heroCopy = HERO_VARIANTS[activeHeroVariant] || HERO_VARIANTS.control

  const closeVideoModal = useCallback(() => {
    trackLandingEvent('landing_demo_modal_closed', {
      source: 'landing',
      hero_variant: activeHeroVariant,
    })
    setIsVideoModalOpen(false)
  }, [activeHeroVariant])

  const startPlanCheckout = useCallback(async (planId) => {
    const normalizedPlanId = String(planId || '').trim().toLowerCase()
    const token = getStoredValue('lf_token')

    if (!normalizedPlanId || normalizedPlanId === 'free') {
      window.location.assign(token ? '/app' : '/get-started')
      return
    }

    if (!VALID_PAID_PLAN_IDS.has(normalizedPlanId)) {
      window.alert('Invalid subscription plan selected.')
      return
    }

    if (!token) {
      window.location.assign(`/get-started?plan=${encodeURIComponent(normalizedPlanId)}`)
      return
    }

    setLoadingPlanId(normalizedPlanId)
    try {
      const data = await fetchJson('/api/stripe/create-subscription-session', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ plan_id: normalizedPlanId }),
      })
      const checkoutUrl = String(data?.url || '').trim()
      if (checkoutUrl) {
        try {
          window.localStorage.setItem('lf_pending_checkout_plan', normalizedPlanId)
        } catch {
          // Ignore storage failures.
        }
        window.location.assign(checkoutUrl)
        return
      }
      throw new Error('Could not open Stripe checkout.')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Could not open Stripe checkout.'
      window.alert(message)
    } finally {
      setLoadingPlanId('')
    }
  }, [])

  useEffect(() => {
    const forceStayOnLanding = new URLSearchParams(window.location.search).get('stay') === '1'
    const viewingPricingRoute = location.pathname === '/pricing'

    if (getStoredValue('lf_token') && !forceStayOnLanding && !viewingPricingRoute) {
      window.location.assign('/app')
      return
    }

    if (viewingPricingRoute) {
      window.requestAnimationFrame(() => {
        const pricingSection = document.getElementById('pricing')
        if (pricingSection) {
          pricingSection.scrollIntoView({ behavior: 'auto', block: 'start' })
        }
      })
    }
  }, [location.pathname, location.search])

  useEffect(() => {
    if (!isVideoModalOpen) {
      return undefined
    }

    const previousOverflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'

    function onKeyDown(event) {
      if (event.key === 'Escape') {
        closeVideoModal()
      }
    }

    window.addEventListener('keydown', onKeyDown)
    return () => {
      document.body.style.overflow = previousOverflow
      window.removeEventListener('keydown', onKeyDown)
    }
  }, [isVideoModalOpen, closeVideoModal])

  return (
    <div className="min-h-screen text-white" style={{ background: '#020617' }}>
      <MarketingNavbar />

      <section className="pt-32 pb-20 px-6">
        <div className="max-w-5xl mx-auto text-center">
          <p className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full border border-yellow-500/30 bg-yellow-500/10 text-yellow-400 text-xs font-semibold mb-8">
            Gold Audit Outbound Engine
          </p>
          <h1 className="text-4xl sm:text-5xl lg:text-6xl font-extrabold leading-tight tracking-tight mb-6">
            {heroCopy.headline}
          </h1>
          <p className="text-lg text-slate-400 max-w-3xl mx-auto mb-10 leading-relaxed">
            {heroCopy.subheadline}
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <a
              href="/signup"
              onClick={() => {
                trackLandingEvent('landing_primary_cta_clicked', {
                  cta: 'find_my_first_50_leads',
                  destination: '/signup',
                  hero_variant: activeHeroVariant,
                })
              }}
              className="w-full sm:w-auto px-8 py-4 rounded-2xl bg-yellow-500 text-slate-900 font-bold hover:bg-yellow-400 transition-colors text-center"
            >
              Find My First 50 Leads
            </a>
            <button
              type="button"
              onClick={() => {
                trackLandingEvent('landing_demo_modal_opened', {
                  source: 'hero_secondary_cta',
                  hero_variant: activeHeroVariant,
                })
                setIsVideoModalOpen(true)
              }}
              className="w-full sm:w-auto px-6 py-4 rounded-2xl border border-slate-700 text-slate-300 hover:border-slate-500 hover:text-white transition-colors text-center"
            >
              Watch 2-Min Demo
            </button>
          </div>

          <div className="mt-12 grid grid-cols-2 lg:grid-cols-4 gap-4 text-left">
            {TRUST_METRICS.map((metric) => (
              <div key={metric.label} className="rounded-xl border border-white/10 bg-slate-900/60 p-4">
                <p className="text-2xl font-extrabold text-yellow-300">{metric.value}</p>
                <p className="mt-1 text-xs uppercase tracking-wider text-slate-400">{metric.label}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section id="how-it-works" className="py-20 px-6 bg-slate-900/40 border-y border-white/5">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">How it works</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              Problem. Mechanism. Outcome. Sniped turns visible market gaps into outreach your prospects recognize as relevant.
            </p>
          </div>

          <Motion.div
            className="grid lg:grid-cols-3 gap-5"
            variants={HOW_IT_WORKS_CONTAINER_VARIANTS}
            initial="hidden"
            whileInView="visible"
            viewport={{ once: true, amount: 0.35 }}
          >
            {HOW_IT_WORKS_STEPS.map((step) => (
              <Motion.div
                key={step.title}
                variants={HOW_IT_WORKS_ITEM_VARIANTS}
                className="rounded-2xl border border-white/10 bg-slate-900/60 p-6"
              >
                <p className="text-xs uppercase tracking-widest text-yellow-400">Workflow</p>
                <h3 className="font-bold text-white mt-2 mb-2">{step.title}</h3>
                <p className="text-sm text-slate-400 leading-relaxed">{step.text}</p>
              </Motion.div>
            ))}
          </Motion.div>
        </div>
      </section>

      <section id="email-proof" className="py-20 px-6">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">Before/After Cold Email</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              The difference is simple: generic outreach asks for attention, audit-backed outreach earns it.
            </p>
          </div>

          <div className="grid lg:grid-cols-2 gap-5">
            <Motion.div
              className="rounded-2xl border border-rose-400/30 bg-rose-950/20 p-6"
              initial={{ opacity: 0, y: 22 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, amount: 0.4 }}
              transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
            >
              <p className="text-xs uppercase tracking-widest text-rose-300">Generic</p>
              <p className="mt-3 text-sm leading-relaxed text-slate-200">
                {EMAIL_COMPARISON.generic}
              </p>
            </Motion.div>
            <Motion.div
              className="rounded-2xl border border-yellow-400/40 bg-yellow-950/20 p-6"
              initial={{ opacity: 0, y: 22, boxShadow: '0 0 0 rgba(250,204,21,0)' }}
              whileInView={{
                opacity: 1,
                y: 0,
                boxShadow: '0 0 0 1px rgba(250,204,21,0.24), 0 0 34px rgba(250,204,21,0.17)',
              }}
              viewport={{ once: true, amount: 0.4 }}
              transition={{ duration: 0.55, delay: 0.16, ease: [0.22, 1, 0.36, 1] }}
            >
              <p className="text-xs uppercase tracking-widest text-yellow-300">Sniped</p>
              <p className="mt-3 text-sm leading-relaxed text-slate-100">
                {EMAIL_COMPARISON.sniped}
              </p>
            </Motion.div>
          </div>
        </div>
      </section>

      <section className="py-20 px-6 bg-slate-900/40 border-y border-white/5">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">Built for agencies that sell through audits</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              Choose your lane, detect the right gap, and start outreach from evidence instead of assumptions.
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-5">
            {AUDIT_USE_CASES.map((card) => (
              <div key={card.title} className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
                <p className="text-xs uppercase tracking-widest text-cyan-300">{card.badge}</p>
                <h3 className="mt-2 text-xl font-bold text-white">{card.title}</h3>
                <p className="mt-3 text-sm text-slate-400 leading-relaxed">{card.text}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section id="app-preview" className="py-20 px-6 bg-slate-950/70 border-y border-yellow-500/20">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">The audit-backed outbound command center</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              Move from detected gap to outreach angle to launch-ready email flow without switching tools.
            </p>
          </div>

          <div className="rounded-3xl border border-yellow-500/30 bg-gradient-to-b from-slate-900 to-slate-950 p-5 sm:p-7 shadow-[0_0_0_1px_rgba(234,179,8,0.2),0_30px_90px_rgba(2,6,23,0.9)]">
            <div className="flex items-center justify-between border-b border-white/10 pb-4 mb-5">
              <div className="flex items-center gap-2">
                <span className="w-2.5 h-2.5 rounded-full bg-rose-400" />
                <span className="w-2.5 h-2.5 rounded-full bg-amber-300" />
                <span className="w-2.5 h-2.5 rounded-full bg-emerald-400" />
              </div>
              <p className="text-xs uppercase tracking-wider text-yellow-300">Sniped App Preview</p>
            </div>

            <div className="grid lg:grid-cols-3 gap-4">
              {APP_PREVIEW_PANELS.map((panel) => (
                <div key={panel.title} className="rounded-2xl border border-yellow-500/25 bg-slate-900/80 p-5">
                  <p className="text-xs uppercase tracking-wider text-yellow-300">{panel.title}</p>
                  <p className="mt-3 text-2xl font-extrabold text-white">{panel.stat}</p>
                  <p className="mt-2 text-sm text-slate-400">{panel.note}</p>
                  <div className="mt-5 h-20 rounded-xl border border-white/10 bg-gradient-to-r from-slate-900 via-yellow-950/40 to-slate-900 p-3">
                    <div className="h-2 rounded-full bg-yellow-500/35 w-4/5" />
                    <div className="mt-2 h-2 rounded-full bg-amber-400/30 w-3/5" />
                    <div className="mt-2 h-2 rounded-full bg-yellow-300/30 w-2/5" />
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section
        id="pricing"
        className="relative overflow-hidden border-y border-white/10 bg-[radial-gradient(circle_at_top,rgba(250,204,21,0.12),transparent_0,transparent_55%),linear-gradient(180deg,rgba(2,6,23,0.96),rgba(8,12,24,0.98))] px-6 py-24"
      >
        <div className="pointer-events-none absolute inset-0">
          <div className="absolute left-[-8rem] top-12 h-56 w-56 rounded-full bg-cyan-500/10 blur-3xl" />
          <div className="absolute right-[-6rem] top-0 h-72 w-72 rounded-full bg-yellow-400/10 blur-3xl" />
          <div className="absolute bottom-[-4rem] left-1/3 h-52 w-52 rounded-full bg-indigo-500/10 blur-3xl" />
        </div>

        <div className="relative mx-auto max-w-7xl text-center">
          <p className="inline-flex rounded-full border border-yellow-400/30 bg-yellow-400/10 px-4 py-1 text-[11px] font-semibold uppercase tracking-[0.2em] text-yellow-200">
            Pricing
          </p>
          <h2 className="mt-4 text-3xl font-extrabold tracking-tight text-white sm:text-4xl">
            Plans built for audit-backed outbound growth
          </h2>
          <p className="mx-auto mt-4 max-w-2xl text-slate-300">
            Professional tiers for teams moving from raw lead lists to high-relevance outbound.
          </p>
          <p className="mx-auto mt-3 max-w-2xl text-sm text-yellow-200/90">
            Not for spam: Sniped is built for targeted B2B outreach, not mass spam.
          </p>

          <div className="mt-12 grid gap-6 text-left sm:grid-cols-2 xl:grid-cols-4">
            {PRICING_PLANS.map((plan) => {
              const accentClass =
                plan.accent === 'neon'
                  ? 'border-yellow-300/70 bg-[linear-gradient(180deg,rgba(250,204,21,0.16),rgba(245,158,11,0.08))] shadow-[0_0_0_1px_rgba(250,204,21,0.2),0_24px_60px_rgba(250,204,21,0.16)]'
                  : plan.accent === 'blue'
                    ? 'border-cyan-400/25 bg-[linear-gradient(180deg,rgba(34,211,238,0.08),rgba(15,23,42,0.86))]'
                    : plan.accent === 'indigo'
                      ? 'border-indigo-400/25 bg-[linear-gradient(180deg,rgba(99,102,241,0.08),rgba(15,23,42,0.88))]'
                      : plan.accent === 'cyan'
                        ? 'border-sky-400/25 bg-[linear-gradient(180deg,rgba(56,189,248,0.08),rgba(15,23,42,0.88))]'
                        : 'border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.04),rgba(15,23,42,0.88))]'

              return (
                <div
                  key={plan.name}
                  className={`relative flex h-full flex-col overflow-hidden rounded-[28px] border p-6 backdrop-blur-xl ${accentClass} ${plan.popular ? 'xl:-translate-y-3' : ''}`}
                >
                  <div className="absolute inset-0 bg-gradient-to-b from-white/[0.06] via-transparent to-transparent" />
                  {plan.popular && (
                    <div className="absolute left-1/2 top-0 -translate-x-1/2 -translate-y-1/2 rounded-full bg-yellow-300 px-4 py-1.5 text-[11px] font-black uppercase tracking-[0.16em] text-slate-900 shadow-[0_8px_30px_rgba(250,204,21,0.45)]">
                      Most Popular
                    </div>
                  )}

                  <div className="relative flex h-full flex-col">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-[11px] uppercase tracking-[0.22em] text-slate-400">{plan.name}</p>
                        <h3 className="mt-2 text-2xl font-bold text-white">{plan.subtitle}</h3>
                      </div>
                      <span className="rounded-full border border-white/10 bg-slate-950/60 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-yellow-200">
                        {plan.trigger}
                      </span>
                    </div>

                    <div className="mt-6">
                      <div className="flex items-end gap-1">
                        <span className="text-5xl font-extrabold text-white">{plan.price}</span>
                        <span className="mb-1 text-base font-semibold text-slate-400">{plan.period}</span>
                      </div>
                      <p className="mt-3 text-sm font-medium leading-6 text-slate-200">{plan.valueProp}</p>
                    </div>

                    <div className="mt-5 rounded-2xl border border-white/10 bg-slate-950/55 px-3 py-3 shadow-inner shadow-black/20">
                      <p className="text-[10px] font-semibold uppercase tracking-[0.18em] text-slate-500">Credits</p>
                      <p className="mt-1 text-sm font-semibold text-cyan-200">{plan.credits}</p>
                    </div>

                    <ul className="mt-5 space-y-3 text-sm text-slate-200 flex-1">
                      {plan.features.map((feature) => (
                        <li key={feature} className="flex items-start gap-2.5 leading-relaxed">
                          <span className="mt-2 inline-block h-2 w-2 shrink-0 rounded-full bg-yellow-300 shadow-[0_0_12px_rgba(250,204,21,0.7)]" />
                          <span>{feature}</span>
                        </li>
                      ))}
                    </ul>

                    <button
                      type="button"
                      onClick={() => { void startPlanCheckout(plan.planId) }}
                      disabled={loadingPlanId === plan.planId}
                      className={`mt-6 inline-flex justify-center rounded-2xl px-4 py-3 text-sm font-semibold transition-all duration-200 disabled:cursor-not-allowed disabled:opacity-60 ${
                        plan.popular
                          ? 'bg-gradient-to-r from-yellow-300 to-amber-400 text-slate-950 shadow-[0_16px_40px_rgba(250,204,21,0.28)] hover:brightness-105'
                          : 'border border-white/10 bg-slate-800/80 text-white shadow-[0_14px_30px_rgba(15,23,42,0.3)] hover:border-yellow-300/40 hover:bg-slate-700/80'
                      }`}
                    >
                      {loadingPlanId === plan.planId ? 'Redirecting...' : `Choose ${plan.name}`}
                    </button>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </section>

      <AnimatePresence>
        {isVideoModalOpen && (
          <Motion.div
            className="fixed inset-0 z-[140] flex items-center justify-center bg-slate-950/85 px-4 py-8 backdrop-blur-sm"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            onClick={closeVideoModal}
          >
            <Motion.div
              className="w-full max-w-4xl rounded-2xl border border-white/15 bg-slate-950 p-4 sm:p-6 shadow-[0_30px_90px_rgba(2,6,23,0.9)]"
              initial={{ opacity: 0, y: 18, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: 12, scale: 0.98 }}
              transition={{ duration: 0.28, ease: [0.22, 1, 0.36, 1] }}
              onClick={(event) => event.stopPropagation()}
            >
              <div className="mb-3 flex items-center justify-between gap-3">
                <p className="text-sm font-semibold uppercase tracking-[0.16em] text-yellow-300">2-Min Demo</p>
                <button
                  type="button"
                  onClick={closeVideoModal}
                  className="rounded-lg border border-white/15 px-3 py-1.5 text-xs font-semibold text-slate-200 hover:border-yellow-300/40 hover:text-white"
                >
                  Close
                </button>
              </div>

              <div className="relative overflow-hidden rounded-xl border border-white/10 bg-slate-900" style={{ paddingTop: '56.25%' }}>
                <iframe
                  title="Sniped demo video"
                  src={DEMO_VIDEO_EMBED_URL}
                  className="absolute inset-0 h-full w-full"
                  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                  allowFullScreen
                />
              </div>
              <p className="mt-3 text-xs text-slate-400">Set VITE_DEMO_VIDEO_URL to replace this placeholder YouTube/Vimeo embed URL.</p>
            </Motion.div>
          </Motion.div>
        )}
      </AnimatePresence>

      <Footer />
    </div>
  )
}
