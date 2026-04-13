import { useCallback, useEffect, useState } from 'react'
import { useLocation } from 'react-router-dom'
import { getStoredValue } from './authStorage.js'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

const API_BASE = ''

const FEATURE_SUMMARY = [
  {
    title: 'Search and Scrape',
    desc: 'Find and extract high-fit leads with live contact data in minutes.',
  },
  {
    title: 'AI Enrichment',
    desc: 'Generate contextual lead insights and personalized openers automatically.',
  },
  {
    title: 'Email Automation',
    desc: 'Launch safe, sequence-based outreach with deliverability-focused controls.',
  },
]

const TRUST_METRICS = [
  { value: '12M+', label: 'Leads processed' },
  { value: '89%', label: 'Open-rate lift with AI enrichment' },
  { value: '3.4x', label: 'More replies in first 30 days' },
  { value: '195+', label: 'Countries covered' },
]

const PLATFORM_PILLARS = [
  {
    title: 'Acquisition Layer',
    text: 'Source high-intent companies by niche, region, and buying signals with resilient anti-bot collection.',
  },
  {
    title: 'Intelligence Layer',
    text: 'Analyze websites, offers, and positioning so every contact gets relevant outreach context in seconds.',
  },
  {
    title: 'Execution Layer',
    text: 'Run multi-step campaigns with adaptive pacing, inbox protection, and behavior-based follow-up logic.',
  },
]

const PROCESS_STEPS = [
  {
    title: 'Define ICP',
    text: 'Set location, niche, and quality constraints once. Sniped keeps your list clean and aligned with revenue goals.',
  },
  {
    title: 'Launch Search + Enrichment',
    text: 'Extract contact records, score opportunities, and generate custom openers from real business signals.',
  },
  {
    title: 'Activate Campaigns',
    text: 'Send sequences with deliverability-aware pacing and live optimization based on opens, clicks, and replies.',
  },
  {
    title: 'Close and Scale',
    text: 'Push hot leads to your CRM, assign owners automatically, and repeat winning playbooks by region or vertical.',
  },
]

const APP_PREVIEW_PANELS = [
  {
    title: 'Live Lead Pipeline',
    stat: '248 new leads',
    note: 'Synced in the last 24h',
  },
  {
    title: 'AI Personalization',
    stat: '92% relevance score',
    note: 'Context built from site + niche signals',
  },
  {
    title: 'Campaign Health',
    stat: '41% open rate',
    note: 'Adaptive send windows are active',
  },
]

const GLOBAL_REACH_STATS = [
  {
    value: '195+',
    label: 'Countries supported',
    detail: 'Teams run Sniped campaigns across Europe, US, LATAM, and APAC.',
  },
  {
    value: '4.2M+',
    label: 'Outreach emails launched',
    detail: 'Automated sequences with personalization and deliverability-safe pacing.',
  },
  {
    value: '78K+',
    label: 'Qualified meetings generated',
    detail: 'Booked calls from AI-scored leads and niche-focused targeting.',
  },
]

const PRICING_PLANS = [
  {
    name: 'Free',
    planId: 'free',
    subtitle: 'The Starter',
    price: '$0',
    period: '/mo',
    valueProp: 'Start sniping for free.',
    credits: '50 Credits/mo',
    trigger: 'Risk-free entry',
    features: [
      'Basic AI Lead Search',
      'Google Maps & LinkedIn indexing',
      'Personal dashboard',
      'Standard search speed',
    ],
    accent: 'slate',
  },
  {
    name: 'Basic',
    planId: 'hustler',
    subtitle: 'The Hustler',
    price: '$49.99',
    period: '/mo',
    valueProp: 'For solo entrepreneurs ready to scale.',
    credits: '2,000 Credits/mo',
    trigger: 'Best first upgrade',
    features: [
      'AI Email Personalization (Hyper-relevant)',
      'Verified Email Discovery (99% Accuracy)',
      'Export to CSV/Excel/JSON',
      'Access to 50M+ global B2B database',
      'Direct Email Support',
    ],
    accent: 'blue',
  },
  {
    name: 'Pro',
    planId: 'growth',
    subtitle: 'The Growth',
    price: '$79.99',
    period: '/mo',
    valueProp: 'One closed deal pays for the whole year.',
    credits: '7,000 Credits/mo',
    trigger: 'Most Popular',
    features: [
      'Deep Company Analysis (Employee count, Revenue, Tech stack)',
      'Priority AI Processing (3x faster results)',
      'Advanced Niche Filtering (Industry, Location, Seniority)',
      'Dedicated Proxy Rotation (Bypass all search limits)',
      'Bulk Export & CRM Integration',
    ],
    accent: 'neon',
    popular: true,
  },
  {
    name: 'Business',
    planId: 'scale',
    subtitle: 'The Scale',
    price: '$99.99',
    period: '/mo',
    valueProp: 'Built for high-performance sales teams.',
    credits: '20,000 Credits/mo',
    trigger: 'Team acceleration',
    features: [
      'Automated Outreach Drip Campaigns (Send from app)',
      'AI Lead Scoring (Identifies hot prospects)',
      'Multi-User Access (Team Collaboration)',
      'Custom Webhook Support',
      'Advanced Analytics & ROI Tracking',
    ],
    accent: 'indigo',
  },
  {
    name: 'Elite',
    planId: 'empire',
    subtitle: 'The Empire',
    price: '$149.99',
    period: '/mo',
    valueProp: 'Full market domination.',
    credits: '100,000 Credits/mo',
    trigger: 'Category domination',
    features: [
      'Unlimited Database Access (Up to 100k credits)',
      'Custom AI Model Training for your specific niche',
      '24/7 Priority Concierge & Success Manager',
      'Dedicated IP for Outreach',
      'Early access to all Beta features (Mobile App coming soon)',
    ],
    accent: 'cyan',
  },
]

async function fetchJson(path, options) {
  const token = getStoredValue('lf_token')
  const headers = {
    ...(options?.headers || {}),
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }

  const response = await fetch(`${API_BASE}${path}`, { ...(options || {}), headers })
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

  const startPlanCheckout = useCallback(async (planId) => {
    const normalizedPlanId = String(planId || '').trim().toLowerCase()
    const token = getStoredValue('lf_token')

    if (!normalizedPlanId || normalizedPlanId === 'free') {
      window.location.assign(token ? '/app' : '/get-started')
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

  return (
    <div className="min-h-screen text-white" style={{ background: '#020617' }}>
      <MarketingNavbar />

      <section className="pt-32 pb-20 px-6">
        <div className="max-w-5xl mx-auto text-center">
          <p className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full border border-yellow-500/30 bg-yellow-500/10 text-yellow-400 text-xs font-semibold mb-8">
            AI-Powered Outbound Platform
          </p>
          <h1 className="text-4xl sm:text-5xl lg:text-6xl font-extrabold leading-tight tracking-tight mb-6">
            Build a pipeline that closes like a premium agency
          </h1>
          <p className="text-lg text-slate-400 max-w-3xl mx-auto mb-10 leading-relaxed">
            Search, enrich, score, and automate outreach from a single command surface built for serious growth teams.
          </p>
          <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
            <a href="/get-started" className="w-full sm:w-auto px-8 py-4 rounded-2xl bg-yellow-500 text-slate-900 font-bold hover:bg-yellow-400 transition-colors text-center">
              Start Free Trial
            </a>
            <a href="/features" className="w-full sm:w-auto px-6 py-4 rounded-2xl border border-slate-700 text-slate-300 hover:border-slate-500 hover:text-white transition-colors text-center">
              Explore Features
            </a>
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

      <section id="features" className="py-20 px-6">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">Core Product Areas</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              Every feature has its own dedicated page with full details.
            </p>
          </div>

          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-5">
            {FEATURE_SUMMARY.map((item) => (
              <div key={item.title} className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
                <h3 className="font-bold text-white mb-2">{item.title}</h3>
                <p className="text-sm text-slate-400 leading-relaxed">{item.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="py-20 px-6 bg-slate-900/40 border-y border-white/5">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">Built as a Full Outbound System</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              Sniped is not a single tool. It is a complete operating layer for prospecting, personalization, and campaign execution.
            </p>
          </div>

          <div className="grid lg:grid-cols-3 gap-5">
            {PLATFORM_PILLARS.map((pillar) => (
              <div key={pillar.title} className="rounded-2xl border border-white/10 bg-slate-900/70 p-6">
                <p className="text-xs uppercase tracking-widest text-yellow-400">Platform Pillar</p>
                <h3 className="mt-2 text-xl font-bold text-white">{pillar.title}</h3>
                <p className="mt-3 text-sm text-slate-400 leading-relaxed">{pillar.text}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="py-20 px-6">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">How Teams Execute in Sniped</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              A practical workflow designed for agencies, SDR teams, and founders who need speed with quality control.
            </p>
          </div>

          <div className="grid md:grid-cols-2 gap-5">
            {PROCESS_STEPS.map((step, index) => (
              <div key={step.title} className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
                <p className="text-xs uppercase tracking-wider text-yellow-300">Step {index + 1}</p>
                <h3 className="mt-2 text-xl font-bold text-white">{step.title}</h3>
                <p className="mt-3 text-sm text-slate-400 leading-relaxed">{step.text}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="py-20 px-6 bg-slate-950/70 border-y border-yellow-500/20">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <h2 className="text-3xl sm:text-4xl font-extrabold tracking-tight">A Quick Look Inside The App</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              Sniped gives your team one command center for acquisition, AI enrichment, and outreach performance.
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
            Choose the credit engine that matches your growth stage
          </h2>
          <p className="mx-auto mt-4 max-w-2xl text-slate-300">
            Modern outbound infrastructure with AI search, enrichment, and automation — wrapped in a premium dark SaaS experience.
          </p>

          <div className="mt-12 grid gap-6 text-left sm:grid-cols-2 xl:grid-cols-5">
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
                      {loadingPlanId === plan.planId ? 'Redirecting…' : (plan.planId === 'free' ? 'Start Free' : 'Buy Now')}
                    </button>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </section>

      <section className="py-20 px-6 border-y border-white/5 bg-slate-950/60">
        <div className="max-w-6xl mx-auto">
          <div className="text-center mb-12">
            <p className="inline-flex px-3 py-1 rounded-full border border-yellow-500/30 bg-yellow-500/10 text-yellow-300 text-xs font-semibold uppercase tracking-wider">
              Global Reach
            </p>
            <h2 className="mt-4 text-3xl sm:text-4xl font-extrabold tracking-tight">Helping teams in 195+ countries</h2>
            <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
              Sniped powers prospecting and outbound execution for founders, sales teams, and agencies around the world.
            </p>
          </div>

          <div className="grid md:grid-cols-3 gap-5">
            {GLOBAL_REACH_STATS.map((item) => (
              <div key={item.label} className="rounded-2xl border border-white/10 bg-slate-900/70 p-6">
                <p className="text-4xl font-extrabold text-yellow-300">{item.value}</p>
                <p className="mt-2 text-sm font-semibold uppercase tracking-wider text-slate-300">{item.label}</p>
                <p className="mt-3 text-sm text-slate-400 leading-relaxed">{item.detail}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <Footer />
    </div>
  )
}
