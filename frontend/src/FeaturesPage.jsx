import { Link } from 'react-router-dom'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'
import { FEATURE_PAGES } from './featurePagesData'

export default function FeaturesPage() {
  return (
    <div className="min-h-screen flex flex-col relative overflow-hidden" style={{ background: '#020617' }}>
      <MarketingNavbar />
      <div className="pointer-events-none absolute inset-0 opacity-80" aria-hidden>
        <div className="absolute -top-24 left-1/2 -translate-x-1/2 w-[44rem] h-[44rem] rounded-full bg-yellow-500/10 blur-3xl" />
        <div className="absolute top-64 -right-24 w-80 h-80 rounded-full bg-yellow-400/10 blur-3xl" />
      </div>

      <main className="relative flex-1 max-w-6xl mx-auto px-6 lg:px-8 pt-28 pb-24 w-full">
        <div className="mb-14 text-center">
          <p className="inline-flex px-3 py-1 rounded-full border border-yellow-500/30 bg-yellow-500/10 text-yellow-300 text-xs font-semibold uppercase tracking-wider">
            Features
          </p>
          <h1 className="mt-4 text-4xl sm:text-5xl font-extrabold text-white tracking-tight">Sniped Product Pages</h1>
          <p className="mt-4 max-w-3xl text-slate-300 text-lg mx-auto">
            Choose a product page below. Each feature opens on its own dedicated page.
          </p>
        </div>

        <div className="grid sm:grid-cols-2 gap-5">
          {FEATURE_PAGES.map((feature) => (
            <Link
              key={feature.slug}
              to={`/features/${feature.slug}`}
              className="group rounded-2xl border border-white/10 bg-slate-900/70 p-6 hover:border-yellow-500/35 hover:bg-slate-900 transition-colors"
            >
              <p className="text-xl font-bold text-white group-hover:text-yellow-300 transition-colors">{feature.label}</p>
              <p className="mt-3 text-sm text-slate-400 leading-relaxed">{feature.subtitle}</p>
              <div className="mt-5 flex flex-wrap gap-2">
                {feature.highlights.slice(0, 2).map((highlight) => (
                  <span key={highlight} className="text-[11px] uppercase tracking-wider px-2 py-1 rounded-md border border-white/10 text-slate-300">
                    {highlight}
                  </span>
                ))}
              </div>
              <p className="mt-5 text-xs font-semibold uppercase tracking-wider text-yellow-400">Open page</p>
            </Link>
          ))}
        </div>

        <div className="mt-12 rounded-2xl border border-white/10 bg-slate-900/60 p-7 text-center">
          <h2 className="text-2xl font-bold text-white">Need a custom workflow for your niche?</h2>
          <p className="mt-3 text-slate-400 max-w-2xl mx-auto">
            We can tune extraction, enrichment, and campaign logic for your vertical so your team reaches qualified buyers faster.
          </p>
          <div className="mt-6">
            <Link to="/get-started" className="inline-flex px-6 py-3 rounded-xl bg-yellow-500 text-slate-900 font-bold hover:bg-yellow-400 transition-colors">
              Talk to Sales
            </Link>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}
