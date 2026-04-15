import { useState, useEffect } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import toast, { Toaster } from 'react-hot-toast'
import { Zap, Copy, RefreshCw, LogOut, Sparkles, Info, Check } from 'lucide-react'
import { clearAuthSession, getStoredValue, setAuthSession } from './authStorage'
import { NICHE_HINTS } from './constants'

const API_BASE = String(import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_URL || '').trim().replace(/\/$/, '')

function getFriendlyAiError(status, detail) {
  const normalized = String(detail || '').toLowerCase()
  if (status === 503 || normalized.includes('backend is not configured') || normalized.includes('backend request failed')) {
    return detail || 'Backend is not configured for production yet. Please contact support.'
  }
  if (normalized.includes('insufficient_quota') || normalized.includes('quota') || normalized.includes('credit') || normalized.includes('billing')) {
    return 'Please check your API credits.'
  }
  if (status === 429 || normalized.includes('rate limit') || normalized.includes('too many requests')) {
    return 'Our AI is a bit busy, retrying in 5 seconds...'
  }
  if (status >= 500) {
    return 'Our AI is a bit busy, retrying in 5 seconds...'
  }
  return detail || 'Generation failed.'
}

function splitIntoSentences(text) {
  const normalized = String(text || '').replace(/\s+/g, ' ').trim()
  if (!normalized) return []
  return normalized
    .split(/(?<=[.!?])\s+/)
    .map((part) => part.trim())
    .filter(Boolean)
}

export default function ColdEmailOpenerPage() {
  const navigate = useNavigate()
  const token = getStoredValue('lf_token')
  const pendingRaw = localStorage.getItem('lf_pending_signup')
  let pendingSignup = null
  try {
    pendingSignup = pendingRaw ? JSON.parse(pendingRaw) : null
  } catch {
    pendingSignup = null
  }

  const niche = getStoredValue('lf_niche') || pendingSignup?.niche || ''
  const email = getStoredValue('lf_email') || pendingSignup?.email || ''

  const [prospectData, setProspectData] = useState('')
  const [opener, setOpener] = useState('')
  const [loading, setLoading] = useState(false)
  const [packMode, setPackMode] = useState(null) // null | 'local_first' | 'aggressive'
  const [copiedSentenceIndex, setCopiedSentenceIndex] = useState(null)
  const [autoCopyEnabled, setAutoCopyEnabled] = useState(() => {
    const saved = localStorage.getItem('lf_auto_copy_hook')
    return saved == null ? true : saved === '1'
  })

  useEffect(() => {
    if (!token && !pendingSignup) {
      navigate('/login')
    }
  }, [token, pendingSignup, navigate])

  function handleLogout() {
    clearAuthSession()
    localStorage.removeItem('lf_pending_signup')
    navigate('/login')
  }

  useEffect(() => {
    localStorage.setItem('lf_auto_copy_hook', autoCopyEnabled ? '1' : '0')
  }, [autoCopyEnabled])

  async function tryCopyText(text) {
    if (!text || !String(text).trim()) {
      return false
    }
    if (!navigator?.clipboard?.writeText) {
      return false
    }
    try {
      await navigator.clipboard.writeText(String(text).trim())
      return true
    } catch {
      return false
    }
  }

  async function generateOpener({ retried = false } = {}) {
    if (!prospectData.trim()) {
      toast.error('Describe your prospect first.')
      return
    }
    setLoading(true)
    setOpener('')
    setCopiedSentenceIndex(null)
    try {
      const isOnboarding = !token && !!pendingSignup

      if (isOnboarding) {
        // Step 1: register account
        const regRes = await fetch(`${API_BASE}/api/auth/register`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            email: pendingSignup.email,
            password: pendingSignup.password,
            niche: pendingSignup.niche,
            account_type: pendingSignup.account_type,
            display_name: pendingSignup.display_name,
            contact_name: pendingSignup.contact_name,
          }),
        })
        const regRaw = await regRes.text()
        let regData = {}
        try { regData = regRaw ? JSON.parse(regRaw) : {} } catch { regData = { detail: regRaw || 'Unknown error.' } }

        if (!regRes.ok) {
          if (regRes.status === 409) {
            toast.error('This email already exists. Please sign in.')
            navigate('/login')
            return
          }
          toast.error(regData.detail || 'Registration failed.')
          return
        }

        // Step 2: store session
        setAuthSession({
          lf_token: regData.token,
          lf_niche: regData.niche,
          lf_email: regData.email,
          lf_display_name: regData.display_name || '',
        })
        localStorage.removeItem('lf_pending_signup')

        // Step 3: generate opener with new token
        const openerRes = await fetch(`${API_BASE}/api/cold-email-opener`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            token: regData.token,
            prospect_data: prospectData.trim(),
            pack_mode: packMode,
          }),
        })
        await openerRes.text()

        // Redirect to /app regardless of opener result
        toast.success('Account created! Redirecting to app...')
        window.location.assign('/app')
        return
      }

      // Logged-in flow
      const res = await fetch(`${API_BASE}/api/cold-email-opener`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token,
          prospect_data: prospectData.trim(),
          pack_mode: packMode,
        }),
      })
      const raw = await res.text()
      let data = {}
      try {
        data = raw ? JSON.parse(raw) : {}
      } catch {
        data = { detail: raw || 'Unknown server response.' }
      }
      if (!res.ok) {
        const detail = data.detail || 'Generation failed.'
        if (res.status === 401) {
          toast.error('Session expired. Please log in again.')
          handleLogout()
          return
        }
        const friendly = getFriendlyAiError(res.status, detail)
        toast.error(friendly)
        if (!retried && (res.status === 429 || res.status >= 500)) {
          window.setTimeout(() => {
            void generateOpener({ retried: true })
          }, 5000)
        }
        return
      }

      const generatedOpener = String(data.opener || '').trim()
      setOpener(generatedOpener)

      if (autoCopyEnabled && generatedOpener) {
        const firstSentence = splitIntoSentences(generatedOpener)[0] || generatedOpener
        const copied = await tryCopyText(firstSentence)
        if (copied) {
          setCopiedSentenceIndex(0)
          toast.success('Success! Hook generated and copied to clipboard.')
          window.setTimeout(() => setCopiedSentenceIndex(null), 2000)
        } else {
          toast('Hook generated. Auto-copy was blocked by your browser.', { icon: 'ℹ️' })
        }
      }
    } catch {
      toast.error('Could not reach the server. Is the backend running?')
    } finally {
      setLoading(false)
    }
  }

  async function copySentence(sentence, index) {
    const copied = await tryCopyText(sentence)
    if (copied) {
      setCopiedSentenceIndex(index)
      toast.success('Copied to clipboard!')
      window.setTimeout(() => setCopiedSentenceIndex(null), 2000)
    } else {
      toast.error('Copy failed.')
    }
  }

  const openerSentences = splitIntoSentences(opener)

  return (
    <div className="min-h-screen px-4 py-10" style={{ background: '#060d1c' }}>
      <Toaster position="top-right" />

      {/* Nav */}
      <header className="max-w-2xl mx-auto flex items-center justify-between mb-10">
        <Link to="/?stay=1" className="flex items-center gap-2 transition-opacity hover:opacity-90" aria-label="Go to Sniped landing page">
          <Zap size={22} className="text-yellow-400" />
          <span className="text-lg font-bold text-white tracking-tight">Sniped</span>
        </Link>
        <div className="flex items-center gap-4">
          <span className="text-xs text-slate-500 hidden sm:block">{email}</span>
          <button
            onClick={handleLogout}
            className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-white transition-colors"
          >
            <LogOut size={13} />
            Sign out
          </button>
        </div>
      </header>

      <main className="max-w-2xl mx-auto">
        {/* Hero */}
        <div className="mb-8">
          <div className="flex items-center gap-2 mb-1">
            <Sparkles size={16} className="text-yellow-400" />
            <span className="text-xs font-semibold text-yellow-400 uppercase tracking-widest">AI Cold Email Opener</span>
          </div>
          <h1 className="text-3xl sm:text-4xl font-extrabold text-white leading-tight">
            Generate a&nbsp;
            <span style={{ background: 'linear-gradient(90deg,#f59e0b,#fbbf24)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
              No-BS
            </span>
            &nbsp;first line
          </h1>
          <p className="mt-2 text-slate-400 text-sm">
            Dial in on your prospect's biggest digital gap — tailored for your niche.
          </p>
        </div>

        {/* Niche badge */}
        <div
          className="inline-flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-semibold mb-6"
          style={{ background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.3)', color: '#f59e0b' }}
        >
          Your niche: {niche}
          <Link to="/get-started" className="text-slate-400 hover:text-yellow-400 transition-colors text-xs font-normal underline">
            change
          </Link>
        </div>

        {/* Pack selector */}
        <div className="mb-4">
          <p className="text-xs font-semibold text-slate-400 uppercase tracking-widest mb-2">Tone Pack</p>
          <div className="grid grid-cols-2 gap-3">
            {[
              { id: 'local_first', label: 'Local First', desc: 'Heavier local context and map visibility angle.' },
              { id: 'aggressive', label: 'Aggressive', desc: 'Sharper pain framing and stronger commercial angle.' },
            ].map((pack) => {
              const active = packMode === pack.id
              return (
                <button
                  key={pack.id}
                  onClick={() => setPackMode(active ? null : pack.id)}
                  className="text-left rounded-xl px-4 py-3 transition-all"
                  style={{
                    background: active ? 'rgba(245,158,11,0.10)' : 'rgba(255,255,255,0.04)',
                    border: active ? '1px solid rgba(245,158,11,0.5)' : '1px solid rgba(255,255,255,0.08)',
                  }}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className={`text-sm font-bold ${active ? 'text-yellow-400' : 'text-white'}`}>{pack.label}</span>
                    <span
                      className="text-[10px] font-semibold px-2 py-0.5 rounded-full tracking-widest"
                      style={{
                        background: active ? 'rgba(245,158,11,0.2)' : 'rgba(255,255,255,0.07)',
                        color: active ? '#f59e0b' : '#64748b',
                      }}
                    >
                      PACK
                    </span>
                  </div>
                  <p className={`text-xs ${active ? 'text-yellow-200/70' : 'text-slate-500'}`}>{pack.desc}</p>
                </button>
              )
            })}
          </div>
          {packMode && (
            <p className="mt-2 text-xs text-yellow-500/70">
              <span className="font-semibold">{packMode === 'local_first' ? 'Local First' : 'Aggressive'}</span> pack active — click again to deactivate.
            </p>
          )}
        </div>

        {/* Input card */}
        <div
          className="rounded-2xl p-6 mb-4"
          style={{ background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(255,255,255,0.08)' }}
        >
          <label className="block text-sm font-medium text-white mb-1">Describe your prospect</label>
          <p className="text-xs text-slate-500 mb-3 flex items-start gap-1">
            <Info size={12} className="mt-0.5 flex-shrink-0" />
            {NICHE_HINTS[niche] || 'Describe the prospect in your own words — what they do, where they are, what stands out.'}
          </p>
          <textarea
            rows={5}
            value={prospectData}
            onChange={e => setProspectData(e.target.value)}
            placeholder={`Describe the prospect in your own words, e.g.:\n"Dental clinic in Manchester, running Facebook ads but no Pixel. Their site is slow and breaks on mobile."`}
            className="w-full rounded-lg px-4 py-3 text-sm text-white placeholder-slate-600 outline-none focus:ring-2 focus:ring-yellow-400/40 resize-none"
            style={{ background: 'rgba(255,255,255,0.05)', border: '1px solid rgba(255,255,255,0.09)' }}
          />

          <button
            onClick={generateOpener}
            disabled={loading}
            className="mt-4 w-full flex items-center justify-center gap-2 py-2.5 rounded-lg font-semibold text-sm transition-opacity disabled:opacity-50"
            style={{ background: 'linear-gradient(135deg,#f59e0b,#d97706)', color: '#000' }}
          >
            {loading ? (
              <>
                <RefreshCw size={14} className="animate-spin" />
                AI is analyzing...
              </>
            ) : (
              <>
                <Sparkles size={14} />
                {pendingSignup && !token ? 'Generate Opener and Enter App' : 'Generate Opener'}
              </>
            )}
          </button>
        </div>

        {/* Result card / skeleton */}
        {(loading || opener) && (
          <div
            className="rounded-2xl p-6 animate-fade-in"
            style={{
              background: 'rgba(245,158,11,0.06)',
              border: '1px solid rgba(245,158,11,0.25)',
              minHeight: '136px',
            }}
          >
            <div className="flex items-start justify-between gap-4 mb-3">
              <span className="text-xs font-semibold text-yellow-400 uppercase tracking-widest">Generated Opener</span>
              <div className="flex items-center gap-3">
                <label className="text-[11px] text-slate-400 flex items-center gap-1.5 select-none">
                  <input
                    type="checkbox"
                    checked={autoCopyEnabled}
                    onChange={(event) => setAutoCopyEnabled(event.target.checked)}
                    className="accent-yellow-400"
                  />
                  Auto-copy first hook
                </label>
                <span className="text-[11px] text-slate-500">Click any sentence to copy</span>
              </div>
            </div>

            <div className="relative" style={{ minHeight: '48px' }}>
              <div
                className={`transition-opacity duration-300 ${loading ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
                aria-hidden={!loading}
              >
                <div className="space-y-2.5">
                  <div
                    className="rounded-lg px-3 py-2"
                    style={{ background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.08)' }}
                  >
                    <div className="h-6 w-full rounded-md bg-slate-500/30 animate-pulse" />
                  </div>
                </div>
              </div>

              <div
                className={`transition-opacity duration-300 ${loading ? 'opacity-0 pointer-events-none absolute inset-0' : 'opacity-100'}`}
                aria-hidden={loading}
              >
                <div className="space-y-2.5">
                  {(openerSentences.length ? openerSentences : [opener]).map((sentence, index) => (
                    <div
                      key={`${sentence}-${index}`}
                      className="flex items-start justify-between gap-3 rounded-lg px-3 py-2 cursor-pointer"
                      style={{ background: 'rgba(255,255,255,0.03)', border: '1px solid rgba(255,255,255,0.08)' }}
                      onClick={() => copySentence(sentence, index)}
                      role="button"
                      tabIndex={0}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault()
                          void copySentence(sentence, index)
                        }
                      }}
                    >
                      <p className="text-white text-base leading-relaxed font-medium">{sentence}</p>
                      <button
                        onClick={(event) => {
                          event.stopPropagation()
                          void copySentence(sentence, index)
                        }}
                        className="flex items-center gap-1.5 text-xs font-medium text-slate-400 hover:text-yellow-400 transition-colors flex-shrink-0"
                        aria-label={`Copy sentence ${index + 1}`}
                      >
                        {copiedSentenceIndex === index ? <Check size={13} /> : <Copy size={13} />}
                        {copiedSentenceIndex === index ? 'Copied!' : 'Copy'}
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="mt-4 flex gap-2">
              <button
                onClick={generateOpener}
                disabled={loading}
                className="flex items-center gap-1.5 text-xs text-slate-400 hover:text-white transition-colors disabled:opacity-40"
              >
                <RefreshCw size={12} className={loading ? 'animate-spin' : ''} />
                {loading ? 'AI is analyzing...' : 'Regenerate'}
              </button>
            </div>
          </div>
        )}

        {/* Tips */}
        <div className="mt-8 text-xs text-slate-600 text-center">
          Tip: the more specific your description, the sharper the opening line.
        </div>
      </main>
    </div>
  )
}
