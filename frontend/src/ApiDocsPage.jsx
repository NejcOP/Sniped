import { Link } from 'react-router-dom'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

const requestCode = `curl -X POST https://api.sniped.ai/v1/enrich-lead \\
  -H "Authorization: Bearer YOUR_API_KEY" \\
  -H "Content-Type: application/json" \\
  -d '{
    "lead_name": "Acme Dental",
    "lead_url": "https://acmedental.example",
    "lead_bio": "Local clinic with outdated website and low review velocity.",
    "selected_category": "Web Design & Dev"
  }'`

const responseCode = `{
  "score": 8,
  "reason": "Site speed and outdated UX are reducing conversion from paid and organic traffic.",
  "competitive_hook": "A nearby competitor uses fast mobile pages with clear CTAs while this site loads slowly and hides contact actions."
}`

export default function ApiDocsPage() {
  return (
    <div className="min-h-screen flex flex-col bg-slate-950">
      <MarketingNavbar />
      <main className="flex-1 max-w-7xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="mb-8 flex items-center justify-between gap-4 flex-wrap">
          <div>
            <p className="text-xs uppercase tracking-[0.25em] text-cyan-300">Developers</p>
            <h1 className="mt-2 text-4xl font-black tracking-tight text-white">API Docs</h1>
          </div>
          <Link
            to="/app"
            className="inline-flex items-center rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm font-semibold text-cyan-200 hover:bg-cyan-400/20 transition-colors"
          >
            Back to Dashboard
          </Link>
        </div>

        <div className="grid gap-6 lg:grid-cols-2">
          <section className="rounded-2xl border border-white/10 bg-slate-900/65 p-7">
            <h2 className="text-2xl font-bold text-white">POST /v1/enrich-lead</h2>
            <p className="mt-3 text-slate-300 leading-relaxed">
              Generate personalized sales intelligence from lead metadata and selected niche category.
              Use this endpoint to enrich a lead before writing outreach.
            </p>

            <div className="mt-6 space-y-4 text-sm">
              <div>
                <h3 className="font-semibold text-cyan-300">Headers</h3>
                <p className="mt-1 text-slate-300">Authorization: Bearer token, Content-Type: application/json</p>
              </div>
              <div>
                <h3 className="font-semibold text-cyan-300">Required fields</h3>
                <ul className="mt-1 space-y-1 text-slate-300 list-disc list-inside">
                  <li>lead_name</li>
                  <li>lead_url</li>
                  <li>lead_bio</li>
                  <li>selected_category</li>
                </ul>
              </div>
              <div>
                <h3 className="font-semibold text-cyan-300">Returns</h3>
                <p className="mt-1 text-slate-300">JSON with score, reason, and competitive_hook ready for sales workflows.</p>
              </div>
            </div>
          </section>

          <section className="rounded-2xl border border-cyan-400/20 bg-[#061126] p-6 shadow-[0_0_34px_rgba(34,211,238,0.12)]">
            <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">Request</p>
            <pre className="mt-3 overflow-x-auto rounded-xl border border-white/10 bg-slate-950/80 p-4 text-xs leading-relaxed text-cyan-100">{requestCode}</pre>

            <p className="mt-5 text-xs uppercase tracking-[0.2em] text-cyan-300">Response</p>
            <pre className="mt-3 overflow-x-auto rounded-xl border border-white/10 bg-slate-950/80 p-4 text-xs leading-relaxed text-cyan-100">{responseCode}</pre>
          </section>
        </div>
      </main>
      <Footer />
    </div>
  )
}
