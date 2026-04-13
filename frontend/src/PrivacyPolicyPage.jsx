import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

export default function PrivacyPolicyPage() {
  return (
    <div className="min-h-screen flex flex-col" style={{ background: '#020617' }}>
      <MarketingNavbar />
      <main className="flex-1 max-w-4xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-8">
          <p className="text-xs uppercase tracking-widest text-yellow-300">Legal</p>
          <h1 className="mt-2 text-4xl font-extrabold text-white tracking-tight">Privacy Policy</h1>
          <p className="mt-3 text-sm text-slate-400">Last updated: March 30, 2026</p>

          <div className="mt-8 space-y-6 text-slate-300 text-sm leading-relaxed">
            <section>
              <h2 className="text-white font-semibold mb-2">1. Data We Process</h2>
              <p>
                We process account information, workspace configuration, campaign metadata, and usage logs required to operate and secure the service.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">2. Why We Process Data</h2>
              <p>
                Data is processed to deliver core features, improve performance, provide support, prevent abuse, and comply with legal requirements.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">3. Data Sharing</h2>
              <p>
                We only share data with trusted infrastructure and service providers required to run Sniped. We do not sell personal data.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">4. Retention and Security</h2>
              <p>
                We retain data only as needed for business and legal obligations, and apply technical and organizational safeguards to protect it.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">5. Your Rights</h2>
              <p>
                Depending on your jurisdiction, you may request access, correction, export, or deletion. Contact
                {' '}<a href="mailto:hello@sniped.ai" className="text-yellow-400 hover:text-yellow-300">hello@sniped.ai</a>.
              </p>
            </section>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}
