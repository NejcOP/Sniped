import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

export default function TermsPage() {
  return (
    <div className="min-h-screen flex flex-col" style={{ background: '#020617' }}>
      <MarketingNavbar />
      <main className="flex-1 max-w-4xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="rounded-2xl border border-white/10 bg-slate-900/60 p-8">
          <p className="text-xs uppercase tracking-widest text-yellow-300">Legal</p>
          <h1 className="mt-2 text-4xl font-extrabold text-white tracking-tight">Terms of Service</h1>
          <p className="mt-3 text-sm text-slate-400">Last updated: March 30, 2026</p>

          <div className="mt-8 space-y-6 text-slate-300 text-sm leading-relaxed">
            <section>
              <h2 className="text-white font-semibold mb-2">1. Service Scope</h2>
              <p>
                Sniped provides lead discovery, enrichment, and outreach tooling for business use. You are responsible for how your team configures
                and executes campaigns.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">2. Account Responsibility</h2>
              <p>
                You are responsible for account security, user access, and all activity executed through your workspace, including third-party
                integrations.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">3. Acceptable Use</h2>
              <p>
                You agree not to use the platform for unlawful, deceptive, or abusive outreach. Campaigns must follow applicable anti-spam and data
                protection regulations.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">4. Billing and Plan Changes</h2>
              <p>
                Paid plans renew according to your billing cycle. You can upgrade, downgrade, or cancel according to plan terms shown in your billing
                settings.
              </p>
            </section>
            <section>
              <h2 className="text-white font-semibold mb-2">5. Contact</h2>
              <p>
                For legal questions, contact <a href="mailto:hello@sniped.ai" className="text-yellow-400 hover:text-yellow-300">hello@sniped.ai</a>.
              </p>
            </section>
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}