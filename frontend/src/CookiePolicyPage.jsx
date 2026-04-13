import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

export default function CookiePolicyPage() {
  return (
    <div className="min-h-screen flex flex-col" style={{ background: '#020617' }}>
      <MarketingNavbar />
      <main className="flex-1 max-w-4xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-8">
          <p className="text-xs uppercase tracking-widest text-yellow-300">Legal</p>
          <h1 className="mt-2 text-4xl font-extrabold text-white tracking-tight">Cookie Policy</h1>
          <p className="mt-3 text-sm text-slate-400">Last updated: March 30, 2026</p>

          <div className="mt-8 space-y-6 text-slate-300 text-sm leading-relaxed">
            <section>
              <h2 className="text-white font-semibold mb-2">1. What Cookies Are Used For</h2>
              <p>
                Cookies and similar technologies help keep sessions active, remember preferences, and understand product usage for performance improvements.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">2. Essential Cookies</h2>
              <p>
                These are required for core product functionality such as authentication, security, and session continuity.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">3. Analytics Cookies</h2>
              <p>
                We may use analytics technologies to measure product performance and improve user experience. These can be limited via your browser settings.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">4. Managing Cookies</h2>
              <p>
                You can control or delete cookies in your browser. Disabling essential cookies may impact some parts of the service.
              </p>
            </section>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}
