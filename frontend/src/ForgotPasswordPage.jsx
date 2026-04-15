import { useEffect, useState } from 'react'
import { ArrowLeft, LifeBuoy, Mail, Send, ShieldCheck, Zap } from 'lucide-react'
import { Link } from 'react-router-dom'
import toast, { Toaster } from 'react-hot-toast'
import { getRememberedEmail, getStoredValue } from './authStorage'

const API_BASE = String(import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_URL || '').trim().replace(/\/$/, '')

export default function ForgotPasswordPage() {
  const [email, setEmail] = useState(() => getRememberedEmail())
  const [loading, setLoading] = useState(false)
  const [submitted, setSubmitted] = useState(false)

  useEffect(() => {
    if (getStoredValue('lf_token')) {
      window.location.assign('/app')
    }
  }, [])

  async function handleSubmit(event) {
    event.preventDefault()
    setLoading(true)
    try {
      const response = await fetch(`${API_BASE}/api/auth/request-password-reset`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: email.trim().toLowerCase(),
          reset_base_url: `${window.location.origin}/reset-password`,
        }),
      })
      const raw = await response.text()
      let data = {}
      try {
        data = raw ? JSON.parse(raw) : {}
      } catch {
        data = { detail: raw || 'Unknown server response.' }
      }
      if (!response.ok) {
        throw new Error(data.detail || 'Password reset request failed.')
      }
      setSubmitted(true)
      toast.success('If the account exists, a reset link has been sent.')
    } catch (error) {
      toast.error(error.message || 'Password reset request failed.')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4 py-12" style={{ background: '#060d1c' }}>
      <Toaster position="top-right" />
      <div className="w-full max-w-xl">
        <Link to="/?stay=1" className="flex items-center justify-center gap-2 mb-8 transition-opacity hover:opacity-90" aria-label="Go to Sniped landing page">
          <Zap size={28} className="text-yellow-400" />
          <span className="text-2xl font-bold text-white tracking-tight">Sniped</span>
        </Link>

        <div
          className="rounded-2xl p-8"
          style={{ background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(255,255,255,0.08)' }}
        >
          <Link to="/login" className="inline-flex items-center gap-2 text-sm text-slate-400 hover:text-white transition-colors">
            <ArrowLeft size={14} /> Back to login
          </Link>

          <h1 className="mt-6 text-2xl font-bold text-white">Forgot your password?</h1>
          <p className="mt-2 text-sm text-slate-400">
            Enter your account email and we will send you a reset link if the account exists.
          </p>

          <form className="mt-6 space-y-4" onSubmit={handleSubmit}>
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">Account email</label>
              <input
                type="email"
                required
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                placeholder="you@agency.com"
                className="w-full px-4 py-2.5 rounded-lg text-sm text-white placeholder-slate-600 outline-none focus:ring-2 focus:ring-yellow-400/40"
                style={{ background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.1)' }}
              />
            </div>

            <button
              type="submit"
              disabled={loading}
              className="w-full flex items-center justify-center gap-2 py-2.5 rounded-lg font-semibold text-sm transition-opacity disabled:opacity-50"
              style={{ background: 'linear-gradient(135deg,#f59e0b,#d97706)', color: '#000' }}
            >
              <Send size={15} />
              {loading ? 'Sending reset link…' : 'Send reset link'}
            </button>
          </form>

          {submitted ? (
            <div className="mt-4 rounded-xl border border-emerald-500/20 bg-emerald-500/5 p-4 text-sm text-emerald-200">
              If the account exists, a password reset link has been sent.
            </div>
          ) : null}

          <div className="mt-6 grid gap-4 sm:grid-cols-2">
            <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4">
              <div className="flex items-center gap-2 text-yellow-400">
                <Mail size={16} />
                <span className="text-sm font-semibold">Inbox tip</span>
              </div>
              <p className="mt-2 text-sm text-slate-400">Check spam/promotions if the email does not show up in your main inbox.</p>
            </div>

            <div className="rounded-xl border border-white/10 bg-white/[0.03] p-4">
              <div className="flex items-center gap-2 text-yellow-400">
                <ShieldCheck size={16} />
                <span className="text-sm font-semibold">Link expiry</span>
              </div>
              <p className="mt-2 text-sm text-slate-400">Reset links expire after 1 hour for security reasons.</p>
            </div>
          </div>

          <div className="mt-6 rounded-xl border border-yellow-500/20 bg-yellow-500/5 p-4">
            <div className="flex items-center gap-2 text-yellow-400">
              <LifeBuoy size={16} />
              <span className="text-sm font-semibold">Need more help?</span>
            </div>
            <p className="mt-2 text-sm text-slate-400">
              If email delivery fails, contact support directly and include the email tied to your Sniped account.
            </p>
            <div className="mt-3 flex flex-wrap gap-4">
              <a href="mailto:hello@sniped.ai?subject=Sniped%20Password%20Reset" className="text-sm font-semibold text-yellow-400 hover:text-yellow-300">
                hello@sniped.ai
              </a>
              <Link to="/faq" className="text-sm font-semibold text-yellow-400 hover:text-yellow-300">
                Open Help Center
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
