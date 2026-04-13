import { useState } from 'react'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

const FAQS = [
  {
    q: 'Where does lead data come from?',
    a: 'Sniped collects public business data from live web sources and structures it for outbound workflows.',
  },
  {
    q: 'Can I run campaigns globally?',
    a: 'Yes. Sniped supports multi-country targeting with localized search and enrichment behavior.',
  },
  {
    q: 'How fast is enrichment?',
    a: 'Most enrichment jobs complete in seconds per lead and can be processed in parallel for scale.',
  },
  {
    q: 'How quickly can I get started?',
    a: 'Most users launch their first campaign in under 30 minutes. You can sign up, define your target, and start lead generation the same day.',
  },
  {
    q: 'Will this hurt my email deliverability?',
    a: 'Sniped is built with deliverability-first logic: paced sending, sequence controls, and follow-up timing to reduce spam risk and protect sender reputation.',
  },
  {
    q: 'Can I connect my own email account?',
    a: 'Yes. You can connect your sending setup and run campaigns with your own infrastructure for better control and brand consistency.',
  },
  {
    q: 'Do you support agencies with multiple clients?',
    a: 'Yes. Agency teams can organize campaigns by client, keep targeting separated, and scale outreach operations across multiple niches.',
  },
  {
    q: 'Can I import my existing lead list?',
    a: 'Yes. You can upload existing lead data and enrich it with AI context, quality scoring, and personalization before sending.',
  },
  {
    q: 'Is Sniped GDPR-friendly?',
    a: 'Sniped focuses on public business data workflows and supports compliance-oriented outreach practices. You should still configure campaigns to match your legal obligations.',
  },
  {
    q: 'Do you offer API access?',
    a: 'Yes. API access is available on higher plans so you can sync leads, automate workflows, and connect Sniped to your internal stack.',
  },
  {
    q: 'What is included in AI credits?',
    a: 'AI credits are used for enrichment tasks such as contextual analysis, personalized opener generation, and advanced lead intelligence actions.',
  },
  {
    q: 'Can I cancel or change my plan anytime?',
    a: 'Yes. You can upgrade, downgrade, or cancel your plan directly from billing settings without long-term lock-in.',
  },
  {
    q: 'Is onboarding support included?',
    a: 'Yes. We provide setup guidance and best-practice recommendations so your team can reach first results quickly.',
  },
  {
    q: 'What support do you provide?',
    a: 'All plans include support, while higher tiers include priority response and deeper strategy assistance for scaling campaigns.',
  },
]

export default function FAQPage() {
  const [openFaqIdx, setOpenFaqIdx] = useState(0)

  return (
    <div className="min-h-screen flex flex-col relative overflow-hidden" style={{ background: '#020617' }}>
      <MarketingNavbar />
      <div className="pointer-events-none absolute inset-0" aria-hidden>
        <div className="absolute -top-24 left-1/2 -translate-x-1/2 w-[44rem] h-[44rem] rounded-full bg-yellow-500/10 blur-3xl" />
      </div>

      <main className="relative flex-1 max-w-5xl mx-auto px-6 lg:px-8 pt-24 pb-20 w-full">
        <div className="mb-10 text-center">
          <p className="inline-flex px-3 py-1 rounded-full border border-yellow-500/30 bg-yellow-500/10 text-yellow-300 text-xs font-semibold uppercase tracking-wider">
            FAQ
          </p>
          <h1 className="mt-4 text-4xl sm:text-5xl font-extrabold text-white tracking-tight">Frequently Asked Questions</h1>
          <p className="mt-4 text-slate-400 max-w-2xl mx-auto">
            Answers to the most common questions about setup, campaigns, pricing, and compliance.
          </p>
        </div>

        <div className="space-y-3">
          {FAQS.map((faq, index) => {
            const isOpen = openFaqIdx === index

            return (
              <div key={faq.q} className="rounded-xl border border-white/10 bg-slate-900/50 overflow-hidden">
                <button
                  onClick={() => setOpenFaqIdx(isOpen ? null : index)}
                  className="w-full px-5 py-4 text-left flex items-center justify-between"
                >
                  <span className="font-semibold text-white pr-3">{faq.q}</span>
                  <span className="text-yellow-400 text-sm">{isOpen ? 'Hide' : 'Show'}</span>
                </button>
                {isOpen && <p className="px-5 pb-4 text-sm text-slate-400 leading-relaxed">{faq.a}</p>}
              </div>
            )
          })}
        </div>
      </main>

      <Footer />
    </div>
  )
}