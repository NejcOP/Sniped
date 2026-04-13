import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

export default function GDPRCompliancePage() {
  return (
    <div className="min-h-screen flex flex-col" style={{ background: '#020617' }}>
      <MarketingNavbar />
      <main className="flex-1 max-w-4xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-8">
          <p className="text-xs uppercase tracking-widest text-yellow-300">Legal</p>
          <h1 className="mt-2 text-4xl font-extrabold text-white tracking-tight">GDPR Compliance</h1>
          <p className="mt-3 text-sm text-slate-400">Last updated: March 30, 2026</p>

          <div className="mt-8 space-y-6 text-slate-300 text-sm leading-relaxed">
            <section>
              <h2 className="text-white font-semibold mb-2">1. Data Protection Commitment</h2>
              <p>
                Sniped is designed to support responsible processing and secure handling of business data used for outreach operations.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">2. Lawful Basis and Purpose</h2>
              <p>
                Data is processed for legitimate business purposes such as prospecting, campaign execution, service improvement, and support.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">3. Data Subject Rights</h2>
              <p>
                We support requests for access, correction, deletion, portability, and objection where applicable by law.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">4. Processor and Security Controls</h2>
              <p>
                Appropriate contractual, organizational, and technical measures are applied to protect data and manage processor relationships.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">5. Contact for GDPR Requests</h2>
              <p>
                Send GDPR-related requests to <a href="mailto:hello@sniped.ai" className="text-yellow-400 hover:text-yellow-300">hello@sniped.ai</a>.
              </p>
            </section>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}
