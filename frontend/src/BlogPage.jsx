import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

const COMMUNITY_WINS_KEY = 'lf_community_wins'

const posts = [
  {
    title: 'Lead Gen Tips: The 3-Signal Rule for Better Replies',
    tag: 'Lead Gen Tips',
    excerpt: 'Use urgency, relevance, and proof in your first two lines to raise reply rate without sounding pushy.',
    readTime: '6 min read',
  },
  {
    title: 'AI Updates: Smarter Niche Detection in Enrichment',
    tag: 'AI Updates',
    excerpt: 'Sniped now prioritizes niche-specific gaps so outreach angles are tighter and easier to personalize.',
    readTime: '4 min read',
  },
  {
    title: 'Lead Gen Tips: Timing Windows That Protect Deliverability',
    tag: 'Lead Gen Tips',
    excerpt: 'How to schedule campaigns in local business hours and avoid spam-like velocity spikes.',
    readTime: '5 min read',
  },
  {
    title: 'AI Updates: Competitive Hook 2.0',
    tag: 'AI Updates',
    excerpt: 'New competitive hook logic now surfaces one concrete weakness your prospect can act on fast.',
    readTime: '3 min read',
  },
]

const initialCommunityWins = [
  {
    id: 'seed-1',
    author: 'Maja K.',
    title: 'From 0 to 7 meetings in 14 days',
    revenue: '€3,200',
    story: 'Used the niche recommendation + local-time mailer schedule. Reply rate jumped to 11.8% and we closed two retainers.',
    createdAt: 'Just now',
  },
]

export default function BlogPage() {
  const [wins, setWins] = useState(() => {
    if (typeof window === 'undefined') return initialCommunityWins
    try {
      const raw = window.localStorage.getItem(COMMUNITY_WINS_KEY)
      if (!raw) return initialCommunityWins
      const parsed = JSON.parse(raw)
      return Array.isArray(parsed) && parsed.length > 0 ? parsed : initialCommunityWins
    } catch {
      return initialCommunityWins
    }
  })
  const [form, setForm] = useState({ author: '', title: '', revenue: '', story: '' })
  const [error, setError] = useState('')

  useEffect(() => {
    try {
      window.localStorage.setItem(COMMUNITY_WINS_KEY, JSON.stringify(wins))
    } catch {
      // Ignore storage quota errors.
    }
  }, [wins])

  function onSubmitWin(e) {
    e.preventDefault()
    const author = form.author.trim()
    const title = form.title.trim()
    const revenue = form.revenue.trim()
    const story = form.story.trim()

    if (!author || !title || !story) {
      setError('Please fill author, title and story.')
      return
    }

    setError('')
    const nextWin = {
      id: String(Date.now()),
      author,
      title,
      revenue: revenue || 'Not shared',
      story,
      createdAt: new Date().toLocaleDateString('en-GB'),
    }
    setWins((prev) => [nextWin, ...prev])
    setForm({ author: '', title: '', revenue: '', story: '' })
  }

  return (
    <div className="min-h-screen flex flex-col bg-slate-950">
      <MarketingNavbar />
      <main className="flex-1 max-w-6xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="mb-10 flex items-center justify-between gap-4 flex-wrap">
          <div>
            <p className="text-xs uppercase tracking-[0.25em] text-cyan-300">Developers</p>
            <h1 className="mt-2 text-4xl font-black tracking-tight text-white">Blog</h1>
            <p className="mt-3 text-slate-400">Insights for outbound teams: lead gen tactics and fresh AI capabilities.</p>
          </div>
          <Link
            to="/app"
            className="inline-flex items-center rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm font-semibold text-cyan-200 hover:bg-cyan-400/20 transition-colors"
          >
            Back to Dashboard
          </Link>
        </div>

        <div className="grid gap-5 md:grid-cols-2">
          {posts.map((post) => (
            <article
              key={post.title}
              className="rounded-2xl border border-white/10 bg-slate-900/70 p-6 shadow-[0_0_0_1px_rgba(56,189,248,0.08),0_18px_60px_rgba(0,8,20,0.55)]"
            >
              <span className="inline-flex rounded-full border border-cyan-400/40 bg-cyan-500/10 px-3 py-1 text-xs font-semibold text-cyan-200">
                {post.tag}
              </span>
              <h2 className="mt-4 text-xl font-bold text-white leading-snug">{post.title}</h2>
              <p className="mt-3 text-sm text-slate-300 leading-relaxed">{post.excerpt}</p>
              <div className="mt-5 flex items-center justify-between">
                <span className="text-xs text-slate-400">{post.readTime}</span>
                <button className="text-sm font-semibold text-cyan-300 hover:text-cyan-200 transition-colors" type="button">
                  Read article
                </button>
              </div>
            </article>
          ))}
        </div>

        <section className="mt-10 grid gap-6 lg:grid-cols-[1.15fr_1fr]">
          <div className="rounded-2xl border border-cyan-400/25 bg-slate-900/70 p-6 shadow-[0_0_0_1px_rgba(34,211,238,0.14),0_20px_60px_rgba(0,8,20,0.6)]">
            <p className="text-xs uppercase tracking-[0.22em] text-cyan-300">Community Wins</p>
            <h2 className="mt-2 text-2xl font-black text-white">Share what you achieved with Sniped</h2>
            <p className="mt-2 text-sm text-slate-400">
              Add your own mini-blog: what you executed, what changed, and how much revenue you generated.
            </p>

            <form className="mt-5 space-y-3" onSubmit={onSubmitWin}>
              <input
                type="text"
                value={form.author}
                onChange={(e) => setForm((prev) => ({ ...prev, author: e.target.value }))}
                placeholder="Your name or team"
                className="w-full rounded-xl border border-white/10 bg-slate-950/80 px-4 py-3 text-sm text-slate-100 placeholder:text-slate-500 outline-none focus:border-cyan-400/60"
              />
              <input
                type="text"
                value={form.title}
                onChange={(e) => setForm((prev) => ({ ...prev, title: e.target.value }))}
                placeholder="Win title (e.g. 5 clients in 30 days)"
                className="w-full rounded-xl border border-white/10 bg-slate-950/80 px-4 py-3 text-sm text-slate-100 placeholder:text-slate-500 outline-none focus:border-cyan-400/60"
              />
              <input
                type="text"
                value={form.revenue}
                onChange={(e) => setForm((prev) => ({ ...prev, revenue: e.target.value }))}
                placeholder="Revenue (e.g. €4,500 MRR)"
                className="w-full rounded-xl border border-white/10 bg-slate-950/80 px-4 py-3 text-sm text-slate-100 placeholder:text-slate-500 outline-none focus:border-cyan-400/60"
              />
              <textarea
                value={form.story}
                onChange={(e) => setForm((prev) => ({ ...prev, story: e.target.value }))}
                rows={4}
                placeholder="Tell people exactly what you did with the app and what results you got."
                className="w-full rounded-xl border border-white/10 bg-slate-950/80 px-4 py-3 text-sm text-slate-100 placeholder:text-slate-500 outline-none focus:border-cyan-400/60 resize-y"
              />
              {error ? <p className="text-sm text-red-300">{error}</p> : null}
              <button
                type="submit"
                className="inline-flex items-center rounded-xl bg-cyan-400 px-5 py-2.5 text-sm font-bold text-slate-950 hover:bg-cyan-300 transition-colors"
              >
                Publish your story
              </button>
            </form>
          </div>

          <div className="space-y-4">
            {wins.map((win) => (
              <article
                key={win.id}
                className="rounded-2xl border border-white/10 bg-slate-900/65 p-5"
              >
                <div className="flex items-center justify-between gap-3 flex-wrap">
                  <p className="font-semibold text-white">{win.author}</p>
                  <span className="text-xs text-slate-400">{win.createdAt}</span>
                </div>
                <h3 className="mt-2 text-lg font-bold text-cyan-200">{win.title}</h3>
                <p className="mt-2 text-sm text-slate-300 leading-relaxed">{win.story}</p>
                <p className="mt-3 inline-flex rounded-lg border border-emerald-400/30 bg-emerald-500/10 px-2.5 py-1 text-xs font-semibold text-emerald-300">
                  Revenue: {win.revenue}
                </p>
              </article>
            ))}
          </div>
        </section>
      </main>
      <Footer />
    </div>
  )
}
