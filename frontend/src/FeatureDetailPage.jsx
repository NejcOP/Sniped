import { Link, Navigate, useParams } from 'react-router-dom'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'
import { FEATURE_PAGES } from './featurePagesData'

export default function FeatureDetailPage() {
  const { slug } = useParams()
  const feature = FEATURE_PAGES.find((item) => item.slug === slug)

  if (!feature) {
    return <Navigate to="/features" replace />
  }

  return (
    <div className="min-h-screen flex flex-col relative overflow-hidden" style={{ background: '#020617' }}>
      <MarketingNavbar />
      <div className="pointer-events-none absolute inset-0" aria-hidden>
        <div className="absolute -top-32 left-1/2 -translate-x-1/2 w-[46rem] h-[46rem] rounded-full bg-yellow-500/10 blur-3xl" />
        <div className="absolute top-72 -right-20 w-80 h-80 rounded-full bg-yellow-400/10 blur-3xl" />
      </div>

      <main className="relative flex-1 max-w-6xl mx-auto px-6 lg:px-8 pt-24 pb-20 w-full">
        <Link to="/features" className="inline-flex items-center gap-2 text-sm text-yellow-400 hover:text-yellow-300 transition-colors">
          Back to Features
        </Link>

        <div className="mt-6 mb-10 rounded-3xl border border-white/10 bg-slate-900/60 p-8 sm:p-10">
          <p className="inline-flex px-3 py-1 rounded-full border border-yellow-500/35 bg-yellow-500/10 text-yellow-300 text-xs uppercase tracking-wider font-semibold">
            Product Deep Dive
          </p>
          <h1 className="text-4xl sm:text-5xl font-extrabold text-white tracking-tight">{feature.title}</h1>
          <p className="mt-4 text-lg text-slate-300 max-w-4xl leading-relaxed">{feature.subtitle}</p>
        </div>

        <div className="grid lg:grid-cols-2 gap-5 mb-6">
          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
            <p className="text-xs uppercase tracking-widest text-slate-500 mb-3">Problem</p>
            <p className="text-sm text-slate-300 leading-relaxed">{feature.problem}</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
            <p className="text-xs uppercase tracking-widest text-slate-500 mb-3">Sniped solution</p>
            <p className="text-sm text-slate-300 leading-relaxed">{feature.solution}</p>
          </div>
        </div>

        <div className="grid lg:grid-cols-2 gap-5">
          <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
            <p className="text-xs uppercase tracking-widest text-slate-500 mb-3">How it works</p>
            <div className="space-y-3">
              {feature.howItWorks.map((step, index) => (
                <div key={step} className="rounded-lg border border-white/8 bg-slate-950/60 p-4">
                  <p className="text-[11px] uppercase tracking-wider text-yellow-300 font-semibold mb-1">Step {index + 1}</p>
                  <p className="text-sm text-slate-300 leading-relaxed">{step}</p>
                </div>
              ))}
            </div>
          </div>

          <div className="space-y-5">
            <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
              <p className="text-xs uppercase tracking-widest text-slate-500 mb-3">Core highlights</p>
              <div className="space-y-2">
                {feature.highlights.map((point) => (
                  <p key={point} className="text-sm text-slate-300 leading-relaxed">• {point}</p>
                ))}
              </div>
            </div>

            <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-6">
              <p className="text-xs uppercase tracking-widest text-slate-500 mb-3">Visual direction</p>
              <p className="text-sm text-slate-300 leading-relaxed">{feature.visual}</p>
            </div>

            <div className="rounded-2xl border border-yellow-500/30 bg-yellow-500/10 p-6">
              <p className="text-xs uppercase tracking-widest text-yellow-300 mb-3">Business outcome</p>
              <p className="text-sm text-slate-200 leading-relaxed">
                Teams using {feature.label} reduce manual prospecting workload, improve message relevance, and convert more first touches into booked conversations.
              </p>
              <Link
                to="/get-started"
                className="mt-5 inline-flex px-5 py-2.5 rounded-xl bg-yellow-500 text-slate-900 font-semibold hover:bg-yellow-400 transition-colors"
              >
                Activate This Feature
              </Link>
            </div>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}
