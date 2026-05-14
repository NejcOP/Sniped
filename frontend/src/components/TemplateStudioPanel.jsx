import { useState } from 'react'
import { snipedEmailTemplates } from '../sniped-email-templates'

const templates = snipedEmailTemplates

const accentMap = {
  rose: { ring: 'ring-rose-500', badge: 'bg-rose-500/10 text-rose-400 border-rose-500/20', tab: 'bg-rose-500 text-white', tabOff: 'text-rose-400 hover:bg-rose-500/10' },
  cyan: { ring: 'ring-cyan-500', badge: 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20', tab: 'bg-cyan-500 text-white', tabOff: 'text-cyan-400 hover:bg-cyan-500/10' },
  emerald: { ring: 'ring-emerald-500', badge: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20', tab: 'bg-emerald-500 text-white', tabOff: 'text-emerald-400 hover:bg-emerald-500/10' },
  violet: { ring: 'ring-violet-500', badge: 'bg-violet-500/10 text-violet-400 border-violet-500/20', tab: 'bg-violet-500 text-white', tabOff: 'text-violet-400 hover:bg-violet-500/10' },
  amber: { ring: 'ring-amber-500', badge: 'bg-amber-500/10 text-amber-400 border-amber-500/20', tab: 'bg-amber-500 text-white', tabOff: 'text-amber-400 hover:bg-amber-500/10' },
}

const gapIcons = { 'No Website': '🌐', 'Traffic Opportunity': '📈', 'Competitor Gap': '⚔️', 'Site Speed': '⚡' }

export default function TemplateStudioPanel() {
  const niches = Object.keys(templates)
  const [niche, setNiche] = useState(niches[0])
  const [gapIdx, setGapIdx] = useState(0)
  const [tab, setTab] = useState('live')
  const [copied, setCopied] = useState(null)

  const data = templates[niche]
  const accent = accentMap[data.accent]
  const gaps = data.templates.map((t) => t.gap)
  const current = data.templates[gapIdx]

  const copy = (text, key) => {
    navigator.clipboard.writeText(text)
    setCopied(key)
    setTimeout(() => setCopied(null), 2000)
  }

  return (
    <div className="min-h-screen bg-[#020617] text-slate-200" style={{ fontFamily: "'IBM Plex Mono', monospace" }}>
      <div className="border-b border-slate-800 px-5 py-4">
        <div className="mx-auto flex max-w-5xl flex-wrap items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-gradient-to-br from-cyan-400 to-blue-500 text-sm">✉</div>
            <div>
              <p className="text-xs uppercase tracking-widest text-slate-500">Sniped.io</p>
              <h1 className="text-base font-black tracking-tight text-white">Template Studio</h1>
            </div>
          </div>
          <div className="flex items-center gap-2 text-xs">
            <span className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-slate-400">5 niches</span>
            <span className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-slate-400">4 gaps each</span>
            <span className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-slate-400">Live + Follow-up</span>
          </div>
        </div>
      </div>

      <div className="mx-auto max-w-5xl space-y-5 px-5 py-6">
        <div>
          <p className="mb-2 px-1 text-xs font-bold uppercase tracking-widest text-slate-500">Active Category</p>
          <div className="flex flex-wrap gap-2">
            {niches.map((n) => {
              const t = templates[n]
              const active = n === niche
              return (
                <button
                  key={n}
                  onClick={() => {
                    setNiche(n)
                    setGapIdx(0)
                    setTab('live')
                  }}
                  className={`flex items-center gap-2 rounded-lg border px-3 py-2 text-xs font-bold transition-all ${active ? `bg-gradient-to-r ${t.color} border-transparent text-white shadow-lg` : 'border-slate-700 bg-slate-900 text-slate-400 hover:border-slate-500'}`}
                >
                  <span>{t.icon}</span>
                  {n}
                </button>
              )
            })}
          </div>
        </div>

        <div>
          <p className="mb-2 px-1 text-xs font-bold uppercase tracking-widest text-slate-500">Gap Type</p>
          <div className="flex flex-wrap gap-2">
            {gaps.map((g, i) => (
              <button
                key={g}
                onClick={() => setGapIdx(i)}
                className={`flex items-center gap-1.5 rounded-lg border px-3 py-2 text-xs font-bold transition-all ${gapIdx === i ? `${accent.tab} border-transparent shadow` : 'border-slate-700 bg-slate-900 text-slate-400 hover:border-slate-600'}`}
              >
                <span>{gapIcons[g]}</span>
                {g}
              </button>
            ))}
          </div>
        </div>

        <div className={`overflow-hidden rounded-xl border border-slate-800 bg-slate-900 ring-1 ${accent.ring} ring-opacity-30`}>
          <div className={`flex items-center justify-between border-b border-slate-800 bg-gradient-to-r ${data.color} bg-opacity-10 px-5 py-4`}>
            <div>
              <div className="mb-1 flex items-center gap-2">
                <span className={`rounded-full border px-2 py-0.5 text-xs font-bold ${accent.badge}`}>{niche}</span>
                <span className="text-xs text-slate-500">·</span>
                <span className="text-xs text-slate-400">{gapIcons[current.gap]} {current.gap}</span>
              </div>
              <p className="text-sm font-bold text-white">Edit both live and follow-up templates here.</p>
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => setTab('live')}
                className={`rounded-lg px-3 py-1.5 text-xs font-bold transition-all ${tab === 'live' ? accent.tab : 'bg-slate-800 text-slate-400 hover:bg-slate-700'}`}
              >
                Live Template
              </button>
              <button
                onClick={() => setTab('followup')}
                className={`rounded-lg px-3 py-1.5 text-xs font-bold transition-all ${tab === 'followup' ? accent.tab : 'bg-slate-800 text-slate-400 hover:bg-slate-700'}`}
              >
                Follow-up
              </button>
            </div>
          </div>

          <div className="space-y-4 p-5">
            {tab === 'live' && (
              <>
                <div>
                  <div className="mb-2 flex items-center justify-between">
                    <label className="text-xs font-bold uppercase tracking-widest text-slate-500">Subject</label>
                    <button onClick={() => copy(current.subject, 'subj')} className="text-xs text-slate-500 transition-colors hover:text-slate-300">
                      {copied === 'subj' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                  <div className="rounded-lg border border-slate-700 bg-slate-800 px-4 py-3 text-sm leading-relaxed text-slate-200">
                    {current.subject}
                  </div>
                </div>

                <div>
                  <div className="mb-2 flex items-center justify-between">
                    <label className="text-xs font-bold uppercase tracking-widest text-slate-500">Body</label>
                    <button onClick={() => copy(current.body, 'body')} className="text-xs text-slate-500 transition-colors hover:text-slate-300">
                      {copied === 'body' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                  <div className="rounded-lg border border-slate-700 bg-slate-800 px-4 py-4">
                    <pre className="whitespace-pre-wrap text-sm leading-relaxed text-slate-200" style={{ fontFamily: 'inherit' }}>{current.body}</pre>
                  </div>
                </div>

                <div className="flex flex-wrap gap-2">
                  {['{BusinessName}', '{City}', '{Niche}', '{YourName}'].map((p) => (
                    <span key={p} className="rounded-full border border-slate-700 bg-slate-800 px-2 py-1 font-mono text-xs text-slate-400">{p}</span>
                  ))}
                </div>
              </>
            )}

            {tab === 'followup' && (
              <>
                <div className="rounded-lg border border-amber-800 bg-amber-950 px-4 py-3">
                  <p className="text-xs font-bold text-amber-400">⏱ Send this 3–4 days after the first email with no reply.</p>
                </div>
                <div>
                  <div className="mb-2 flex items-center justify-between">
                    <label className="text-xs font-bold uppercase tracking-widest text-slate-500">Follow-up Body</label>
                    <button onClick={() => copy(current.followup, 'fu')} className="text-xs text-slate-500 transition-colors hover:text-slate-300">
                      {copied === 'fu' ? '✓ Copied' : 'Copy'}
                    </button>
                  </div>
                  <div className="rounded-lg border border-slate-700 bg-slate-800 px-4 py-4">
                    <pre className="whitespace-pre-wrap text-sm leading-relaxed text-slate-200" style={{ fontFamily: 'inherit' }}>{current.followup}</pre>
                  </div>
                </div>
                <div className="flex flex-wrap gap-2">
                  {['{BusinessName}', '{City}', '{Niche}', '{YourName}'].map((p) => (
                    <span key={p} className="rounded-full border border-slate-700 bg-slate-800 px-2 py-1 font-mono text-xs text-slate-400">{p}</span>
                  ))}
                </div>
              </>
            )}
          </div>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-900 p-5">
          <p className="mb-3 text-xs font-bold uppercase tracking-widest text-slate-500">⚡ Why These Templates Get High Reply Rates</p>
          <div className="grid grid-cols-1 gap-3 text-xs leading-relaxed text-slate-400 md:grid-cols-2">
            {[
              ['Specific not generic', "Every email references something real about the business — no 'I noticed your company could benefit...' language."],
              ['Problem-first, pitch-last', 'The email names the pain before mentioning any solution. Readers feel understood, not sold to.'],
              ['One CTA only', 'Each email asks for exactly one thing: a 15-min call, a sent report, or a yes/no question. Never multiple asks.'],
              ['Follow-up adds value', "The follow-up isn't 'just checking in' — it adds a new data point or concrete offer to re-open the conversation."],
              ['Short subject lines', "Under 8 words. Curiosity or specificity. Never clickbait. Passes the 'is this spam?' test."],
              ['Plain text only', 'No HTML, no images, no fancy formatting. Looks like a real person wrote it. Deliverability and reply rates are both higher.'],
            ].map(([title, desc]) => (
              <div key={title} className="flex gap-2">
                <span className="flex-shrink-0 text-slate-600">→</span>
                <div><span className="font-bold text-slate-200">{title}:</span> {desc}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-3 gap-3">
          {[
            { n: '5', label: 'Niches covered' },
            { n: '40', label: 'Total templates' },
            { n: '34%', label: 'Avg reply rate benchmark' },
          ].map((s) => (
            <div key={s.label} className="rounded-xl border border-slate-800 bg-slate-900 p-4 text-center">
              <div className={`bg-gradient-to-r ${data.color} bg-clip-text text-2xl font-black text-transparent`}>{s.n}</div>
              <div className="mt-1 text-xs text-slate-400">{s.label}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
