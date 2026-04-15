import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Search,
  Rocket,
  Sparkles,
  Ticket,
  Plug,
  CreditCard,
  Code2,
  ArrowUpRight,
  MessageSquare,
} from 'lucide-react'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'
import { getStoredValue } from './authStorage'

const categories = [
  {
    name: 'Getting Started',
    description: 'Set up your first workflow in 2 minutes.',
    icon: Rocket,
  },
  {
    name: 'Lead Enrichment',
    description: 'How does the AI audit work for your niche?',
    icon: Sparkles,
  },
  {
    name: 'AppSumo FAQ',
    description: 'Redemption codes, tiers, and limits.',
    icon: Ticket,
  },
  {
    name: 'Integrations',
    description: 'Connect HubSpot, Zapier, and your CRM.',
    icon: Plug,
  },
  {
    name: 'Billing & Subscription',
    description: 'Manage your Stripe portal and billing.',
    icon: CreditCard,
  },
  {
    name: 'API & Developers',
    description: 'Documentation for advanced usage.',
    icon: Code2,
  },
]

const helpArticles = [
  {
    id: 'export-csv',
    category: 'Getting Started',
    title: 'How to export leads to CSV?',
    summary: 'Export qualified leads in two clicks from the dashboard.',
    content: [
      'Open Dashboard and go to Lead Management.',
      'Filter by status (for example Enriched or Meeting Set).',
      'Use Export Targets or Export AI and download your CSV file.',
    ],
  },
  {
    id: 'smtp-custom',
    category: 'Integrations',
    title: 'How to connect a custom SMTP sender?',
    summary: 'Add your sender credentials once and reuse them in every campaign.',
    content: [
      'Go to Settings > SMTP configuration.',
      'Enter host, port, email and password for your sender account.',
      'Run Test SMTP and save when status is successful.',
    ],
  },
  {
    id: 'appsumo-tier',
    category: 'AppSumo FAQ',
    title: 'How AppSumo redemption works by tier?',
    summary: 'Redeem your code once, then your workspace unlocks matching limits.',
    content: [
      'Open Developers > AppSumo Redemption.',
      'Paste your code and click Redeem Now.',
      'If already redeemed, the page confirms activation for your account.',
    ],
  },
  {
    id: 'timezone-schedule',
    category: 'Lead Enrichment',
    title: 'How to schedule campaigns by local timezone?',
    summary: 'Choose a country and schedule in local time; Sniped converts it to ET automatically.',
    content: [
      'Select country in your workflow before launching mailer.',
      'In Confirm Launch Mailer choose local send hour (recommended 09:00-11:00).',
      'Sniped converts selected local time to backend ET schedule.',
    ],
  },
  {
    id: 'api-enrich',
    category: 'API & Developers',
    title: 'How to use POST /v1/enrich-lead?',
    summary: 'Send lead metadata and selected category to get actionable sales intelligence.',
    content: [
      'Use API key in Authorization Bearer header.',
      'POST lead_name, lead_url, lead_bio and selected_category JSON fields.',
      'Read score, reason and competitive_hook in response payload.',
    ],
  },
  {
    id: 'first-workflow',
    category: 'Getting Started',
    title: 'Set up your first workflow in 2 minutes',
    summary: 'From keyword to launched mailer, this is the fastest clean setup path.',
    content: [
      'Create scrape query with keyword + country and run Search & Scrape.',
      'Run Enrichment to score leads and generate personalized context.',
      'Launch Mailer with safe delay and local-time scheduling.',
    ],
  },
  {
    id: 'stripe-billing',
    category: 'Billing & Subscription',
    title: 'Manage invoices and plan from billing portal',
    summary: 'Upgrade, downgrade and review invoices without contacting support.',
    content: [
      'Open Billing & Subscription section in your account.',
      'Use Stripe portal to change plan or payment method.',
      'Download invoices directly for accounting.',
    ],
  },
]

const topArticleIds = ['export-csv', 'smtp-custom', 'appsumo-tier', 'timezone-schedule', 'api-enrich']

export default function HelpCenterPage() {
  const [query, setQuery] = useState('')
  const [activeCategory, setActiveCategory] = useState('All')
  const [activeArticleId, setActiveArticleId] = useState(helpArticles[0].id)
  const userName = getStoredValue('lf_display_name') || getStoredValue('lf_contact_name') || 'there'

  const filteredCategories = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return categories
    return categories.filter((item) => {
      const haystack = `${item.name} ${item.description}`.toLowerCase()
      return haystack.includes(q)
    })
  }, [query])

  const filteredArticles = useMemo(() => {
    const q = query.trim().toLowerCase()
    const source = topArticleIds
      .map((id) => helpArticles.find((article) => article.id === id))
      .filter(Boolean)

    if (!q) return source
    return source.filter((article) => {
      const haystack = `${article.title} ${article.summary}`.toLowerCase()
      return haystack.includes(q)
    })
  }, [query])

  const visibleKnowledgeBase = useMemo(() => {
    const q = query.trim().toLowerCase()
    return helpArticles.filter((article) => {
      const categoryPass = activeCategory === 'All' || article.category === activeCategory
      if (!categoryPass) return false
      if (!q) return true
      const text = `${article.title} ${article.summary} ${article.content.join(' ')}`.toLowerCase()
      return text.includes(q)
    })
  }, [query, activeCategory])

  const selectedArticle = useMemo(() => {
    const explicit = helpArticles.find((article) => article.id === activeArticleId)
    if (explicit && visibleKnowledgeBase.some((article) => article.id === explicit.id)) return explicit
    if (visibleKnowledgeBase.length > 0) return visibleKnowledgeBase[0]
    return helpArticles[0]
  }, [activeArticleId, visibleKnowledgeBase])

  return (
    <div className="min-h-screen flex flex-col bg-slate-950">
      <MarketingNavbar />
      <main className="flex-1 max-w-6xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="mb-10 flex items-center justify-between gap-4 flex-wrap">
          <div>
            <p className="text-xs uppercase tracking-[0.25em] text-cyan-300">Developers</p>
            <h1 className="mt-2 text-4xl font-black tracking-tight text-white">Help Center</h1>
          </div>
          <Link
            to="/app"
            className="inline-flex items-center rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm font-semibold text-cyan-200 hover:bg-cyan-400/20 transition-colors"
          >
            Back to Dashboard
          </Link>
        </div>

        <section className="relative overflow-hidden rounded-3xl border border-cyan-400/20 bg-gradient-to-b from-[#071634] via-slate-900/95 to-slate-950 p-6 md:p-10 shadow-[0_0_0_1px_rgba(56,189,248,0.16),0_25px_70px_rgba(0,8,20,0.65)]">
          <div className="pointer-events-none absolute inset-0 opacity-60" style={{ background: 'radial-gradient(circle at 20% 10%, rgba(56,189,248,0.24), transparent 45%)' }} />
          <div className="relative">
            <p className="text-xs uppercase tracking-[0.24em] text-cyan-300">Self-Service Hub</p>
            <h2 className="mt-2 text-3xl md:text-4xl font-black tracking-tight text-white">
              How can we help you today, {userName}?
            </h2>
            <div className="mt-6 mx-auto max-w-4xl rounded-2xl border border-cyan-300/25 bg-slate-950/75 p-3">
              <div className="flex items-center gap-3 rounded-xl border border-white/10 bg-[#030d22] px-4 py-3">
                <Search className="h-5 w-5 text-cyan-300" />
                <input
                  type="text"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search guides, API docs, billing answers, AppSumo help..."
                  className="w-full bg-transparent text-sm md:text-base text-slate-100 placeholder:text-slate-500 outline-none"
                />
              </div>
            </div>
          </div>
        </section>

        <section className="mt-8 grid gap-6 lg:grid-cols-[2fr_1fr]">
          <div>
            <div className="mb-4 flex items-center justify-between">
              <h3 className="text-lg font-bold text-white">Help Categories</h3>
              <span className="text-xs text-slate-400">{filteredCategories.length} sections</span>
            </div>
            <div className="mb-4">
              <button
                type="button"
                onClick={() => setActiveCategory('All')}
                className={`rounded-xl px-3 py-1.5 text-xs font-semibold transition-colors ${
                  activeCategory === 'All' ? 'bg-cyan-400 text-slate-950' : 'bg-white/5 text-slate-300 hover:bg-cyan-500/15 hover:text-cyan-200'
                }`}
              >
                All topics
              </button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
              {filteredCategories.map((cat) => {
                const Icon = cat.icon
                return (
                  <article
                    key={cat.name}
                    className="group rounded-2xl border border-white/10 bg-slate-900/60 p-5 transition-all duration-200 hover:border-cyan-300/60 hover:shadow-[0_0_0_1px_rgba(34,211,238,0.4),0_0_28px_rgba(34,211,238,0.18)]"
                  >
                    <div className="mb-4 inline-flex h-10 w-10 items-center justify-center rounded-lg border border-cyan-300/35 bg-cyan-500/10 text-cyan-300">
                      <Icon className="h-5 w-5" />
                    </div>
                    <h4 className="text-sm font-bold text-white leading-tight">{cat.name}</h4>
                    <p className="mt-2 text-sm text-slate-300 leading-relaxed">{cat.description}</p>
                    <button
                      type="button"
                      onClick={() => {
                        setActiveCategory(cat.name)
                        const firstInCategory = helpArticles.find((article) => article.category === cat.name)
                        if (firstInCategory) setActiveArticleId(firstInCategory.id)
                      }}
                      className="mt-4 inline-flex items-center gap-1 text-xs font-semibold text-cyan-300 group-hover:text-cyan-200"
                    >
                      Open articles <ArrowUpRight className="h-3.5 w-3.5" />
                    </button>
                  </article>
                )
              })}
            </div>
          </div>

          <aside className="rounded-2xl border border-white/10 bg-slate-900/65 p-5 h-fit">
            <h3 className="text-lg font-bold text-white">Top Articles</h3>
            <p className="mt-1 text-sm text-slate-400">Most viewed support answers this week.</p>
            <ul className="mt-4 space-y-2">
              {filteredArticles.map((article) => (
                <li key={article.id}>
                  <button
                    type="button"
                    onClick={() => {
                      setActiveCategory('All')
                      setActiveArticleId(article.id)
                    }}
                    className={`w-full rounded-xl border px-3 py-3 text-left text-sm transition-colors ${
                      selectedArticle?.id === article.id
                        ? 'border-cyan-300/45 bg-cyan-500/10 text-cyan-200'
                        : 'border-white/8 bg-slate-950/70 text-slate-200 hover:border-cyan-300/35 hover:text-cyan-200'
                    }`}
                  >
                    {article.title}
                  </button>
                </li>
              ))}
              {filteredArticles.length === 0 ? (
                <li className="rounded-xl border border-white/8 bg-slate-950/70 px-3 py-3 text-sm text-slate-400">
                  No article found for that search.
                </li>
              ) : null}
            </ul>
          </aside>
        </section>

        <section className="mt-8 rounded-2xl border border-white/10 bg-slate-900/65 p-6">
          <div className="flex items-center justify-between gap-3 flex-wrap">
            <div>
              <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">Article Viewer</p>
              <h3 className="mt-1 text-2xl font-bold text-white">{selectedArticle.title}</h3>
              <p className="mt-2 text-sm text-slate-300">{selectedArticle.summary}</p>
            </div>
            <span className="rounded-lg border border-cyan-300/25 bg-cyan-500/10 px-2.5 py-1 text-xs font-semibold text-cyan-200">
              {selectedArticle.category}
            </span>
          </div>

          <ol className="mt-5 space-y-2">
            {selectedArticle.content.map((step, idx) => (
              <li key={step} className="rounded-xl border border-white/8 bg-slate-950/70 px-4 py-3 text-sm text-slate-200 leading-relaxed">
                <span className="mr-2 inline-flex h-5 w-5 items-center justify-center rounded-full bg-cyan-500/20 text-[11px] font-bold text-cyan-200">
                  {idx + 1}
                </span>
                {step}
              </li>
            ))}
          </ol>

          <div className="mt-5 flex flex-wrap gap-2">
            {visibleKnowledgeBase.map((article) => (
              <button
                key={article.id}
                type="button"
                onClick={() => setActiveArticleId(article.id)}
                className={`rounded-lg px-3 py-1.5 text-xs font-semibold transition-colors ${
                  selectedArticle.id === article.id
                    ? 'bg-cyan-400 text-slate-950'
                    : 'bg-white/5 text-slate-300 hover:bg-cyan-500/15 hover:text-cyan-200'
                }`}
              >
                {article.title}
              </button>
            ))}
          </div>
        </section>

        <section className="mt-8 rounded-2xl border border-cyan-300/25 bg-[#061126] p-6 flex items-center justify-between gap-4 flex-wrap">
          <div className="flex items-start gap-3">
            <span className="mt-0.5 inline-flex h-9 w-9 items-center justify-center rounded-lg border border-cyan-300/40 bg-cyan-500/15 text-cyan-200">
              <MessageSquare className="h-4 w-4" />
            </span>
            <div>
              <h3 className="text-white font-bold">Still stuck?</h3>
              <p className="mt-1 text-sm text-slate-300">Chat with our AI support for instant guidance and step-by-step fixes.</p>
            </div>
          </div>
          <button
            type="button"
            className="inline-flex items-center rounded-xl bg-cyan-400 px-5 py-2.5 text-sm font-bold text-slate-950 hover:bg-cyan-300 transition-colors"
          >
            Still stuck? Chat with our AI support
          </button>
        </section>
      </main>
      <Footer />
    </div>
  )
}
