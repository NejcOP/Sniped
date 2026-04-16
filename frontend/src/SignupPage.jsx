import { createElement, useEffect, useState } from 'react'
import { Link, useNavigate, useSearchParams } from 'react-router-dom'
import { Zap, Mail, Lock, UserPlus, Briefcase, User, Building2, ArrowLeft, Eye, EyeOff } from 'lucide-react'
import { clearAuthSession, getStoredValue } from './authStorage'
import { ALLOWED_NICHES, NICHE_DESCRIPTIONS, ACCOUNT_TYPE_LABELS } from './constants'

const NICHES = ALLOWED_NICHES
const COMPANY_TYPES = ['agency', 'company']
const API_BASE = String(import.meta.env.VITE_API_BASE_URL || import.meta.env.VITE_API_URL || '').trim().replace(/\/$/, '')

function InputField({ icon, ...props }) {
  return (
    <div className="relative">
      {icon ? createElement(icon, { size: 15, className: 'absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none' }) : null}
      <input
        {...props}
        className="w-full pl-9 pr-4 py-2.5 rounded-lg text-sm text-white placeholder-slate-600 outline-none focus:ring-2 focus:ring-yellow-400/40"
        style={{ background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.1)' }}
      />
    </div>
  )
}

function PasswordField({ value, onChange, placeholder }) {
  const [visible, setVisible] = useState(false)

  return (
    <div className="relative">
      {createElement(Lock, { size: 15, className: 'absolute left-3 top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none' })}
      <input
        type={visible ? 'text' : 'password'}
        required
        value={value}
        onChange={onChange}
        placeholder={placeholder}
        className="w-full pl-9 pr-10 py-2.5 rounded-lg text-sm text-white placeholder-slate-600 outline-none focus:ring-2 focus:ring-yellow-400/40"
        style={{ background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.1)' }}
      />
      <button
        type="button"
        onClick={() => setVisible((current) => !current)}
        className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-white transition-colors"
        aria-label={visible ? 'Hide password' : 'Show password'}
      >
        {visible ? <EyeOff size={15} /> : <Eye size={15} />}
      </button>
    </div>
  )
}

export default function SignupPage() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()
  const accountType = searchParams.get('accountType') || 'entrepreneur'
  const isCompany = COMPANY_TYPES.includes(accountType)

  const [firstName, setFirstName] = useState('')
  const [lastName, setLastName] = useState('')
  const [companyName, setCompanyName] = useState('')
  const [yourName, setYourName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [niche, setNiche] = useState('')
  const [loading, setLoading] = useState(false)
  const [signupError, setSignupError] = useState('')

  useEffect(() => {
    if (getStoredValue('lf_token')) {
      navigate('/app', { replace: true })
      return
    }
    try {
      const raw = localStorage.getItem('lf_pending_signup')
      if (!raw) return
      const pending = JSON.parse(raw)
      if (pending && typeof pending === 'object') {
        setEmail((current) => current || String(pending.email || ''))
        setNiche(String(pending.niche || ''))
        setCompanyName(String(pending.display_name || ''))
      }
    } catch {
      // Ignore malformed localStorage
    }
  }, [navigate])

  useEffect(() => {
    const error = String(searchParams.get('error') || '').trim().toLowerCase()
    const nextEmail = String(searchParams.get('email') || '').trim().toLowerCase()
    if (nextEmail) {
      setEmail(nextEmail)
    }
    if (error === 'email_exists') {
      setSignupError('An account with this email already exists. Please sign in.')
    }
  }, [searchParams])

  async function handleSubmit(e) {
    e.preventDefault()
    setSignupError('')
    if (!niche) {
      return
    }
    if (password.length < 8) {
      return
    }
    const display_name = isCompany
      ? companyName.trim()
      : `${firstName.trim()} ${lastName.trim()}`.trim()
    if (!display_name) {
      return
    }
    setLoading(true)
    try {
      const normalizedEmail = email.trim().toLowerCase()
      const response = await fetch(`${API_BASE}/api/auth/check-email?email=${encodeURIComponent(normalizedEmail)}`)
      const raw = await response.text()
      let payload = {}
      try {
        payload = raw ? JSON.parse(raw) : {}
      } catch {
        payload = { detail: raw || 'Could not validate email.' }
      }
      if (!response.ok) {
        setSignupError(String(payload?.detail || 'Could not validate email.'))
        return
      }
      if (!payload?.available) {
        setSignupError(String(payload?.detail || 'An account with this email already exists. Please sign in.'))
        return
      }

      clearAuthSession()
      localStorage.setItem(
        'lf_pending_signup',
        JSON.stringify({
          email: normalizedEmail,
          password,
          niche,
          account_type: accountType,
          display_name,
          contact_name: isCompany ? yourName.trim() : '',
        })
      )
      navigate('/cold-email-opener')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4 py-12" style={{ background: '#060d1c' }}>
      <div className="w-full max-w-md">
        {/* Logo */}
        <Link to="/?stay=1" className="flex items-center justify-center gap-2 mb-8 transition-opacity hover:opacity-90" aria-label="Go to Sniped landing page">
          <Zap size={28} className="text-yellow-400" />
          <span className="text-2xl font-bold text-white tracking-tight">Sniped</span>
        </Link>

        <div
          className="rounded-2xl p-8"
          style={{ background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(255,255,255,0.08)' }}
        >
          {/* Back + type badge */}
          <div className="flex items-center justify-between mb-5">
            <button
              onClick={() => navigate('/get-started')}
              className="flex items-center gap-1 text-xs text-slate-500 hover:text-white transition-colors"
            >
              <ArrowLeft size={13} />
              Change type
            </button>
            <span
              className="flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold"
              style={{ background: 'rgba(245,158,11,0.12)', border: '1px solid rgba(245,158,11,0.3)', color: '#f59e0b' }}
            >
              {isCompany ? <Building2 size={11} /> : <User size={11} />}
              {ACCOUNT_TYPE_LABELS[accountType] || 'Individual'}
            </span>
          </div>

          <h1 className="text-2xl font-bold text-white mb-1">Create account</h1>
          <p className="text-sm text-slate-400 mb-6">Start generating cold email openers in seconds</p>

          <form onSubmit={handleSubmit} className="space-y-4">

            {/* Individual name fields */}
            {!isCompany && (
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">First name</label>
                  <InputField icon={User} type="text" required value={firstName} onChange={e => setFirstName(e.target.value)} placeholder="Jane" />
                </div>
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">Last name</label>
                  <InputField icon={User} type="text" required value={lastName} onChange={e => setLastName(e.target.value)} placeholder="Smith" />
                </div>
              </div>
            )}

            {/* Company/Agency name fields */}
            {isCompany && (
              <>
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">
                    {accountType === 'agency' ? 'Agency name' : 'Company name'}
                  </label>
                  <InputField
                    icon={Building2} type="text" required
                    value={companyName} onChange={e => setCompanyName(e.target.value)}
                    placeholder={accountType === 'agency' ? 'Spark Media Agency' : 'TechNova Inc.'}
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-slate-400 mb-1">Your name</label>
                  <InputField icon={User} type="text" value={yourName} onChange={e => setYourName(e.target.value)} placeholder="John Smith" />
                </div>
              </>
            )}

            {/* Email */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">Email</label>
              <InputField icon={Mail} type="email" required value={email} onChange={e => setEmail(e.target.value)} placeholder="you@agency.com" />
            </div>

            {/* Password */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">Password</label>
              <PasswordField value={password} onChange={e => setPassword(e.target.value)} placeholder="Min. 8 characters" />
            </div>

            {/* Niche selector */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-2">
                <Briefcase size={12} className="inline mr-1" />
                What type of agency / service are you?
              </label>
              <div className="grid grid-cols-1 gap-2">
                {NICHES.map(n => (
                  <button
                    key={n}
                    type="button"
                    onClick={() => setNiche(n)}
                    className="flex items-start gap-3 px-3 py-2.5 rounded-lg text-left transition-all"
                    style={{
                      background: niche === n ? 'rgba(245,158,11,0.12)' : 'rgba(255,255,255,0.04)',
                      border: niche === n ? '1px solid rgba(245,158,11,0.5)' : '1px solid rgba(255,255,255,0.07)',
                    }}
                  >
                    <span
                      className="mt-0.5 w-4 h-4 rounded-full flex-shrink-0 flex items-center justify-center border-2 transition-colors"
                      style={{
                        borderColor: niche === n ? '#f59e0b' : 'rgba(255,255,255,0.2)',
                        background: niche === n ? '#f59e0b' : 'transparent',
                      }}
                    >
                      {niche === n && (
                        <span className="w-1.5 h-1.5 rounded-full bg-black block" />
                      )}
                    </span>
                    <span>
                      <span className={`block text-sm font-medium ${niche === n ? 'text-yellow-400' : 'text-white'}`}>
                        {n}
                      </span>
                      <span className="block text-xs text-slate-500 mt-0.5">{NICHE_DESCRIPTIONS[n]}</span>
                    </span>
                  </button>
                ))}
              </div>
            </div>

            {signupError ? (
              <p className="rounded-lg border border-red-500/40 bg-red-500/10 px-3 py-2 text-sm text-red-300">
                {signupError}
              </p>
            ) : null}

            <button
              type="submit"
              disabled={loading}
              className="w-full flex items-center justify-center gap-2 py-2.5 rounded-lg font-semibold text-sm transition-opacity disabled:opacity-50"
              style={{ background: 'linear-gradient(135deg,#f59e0b,#d97706)', color: '#000' }}
            >
              <UserPlus size={15} />
              {loading ? 'Saving details…' : 'Continue'}
            </button>
          </form>

          <p className="mt-6 text-center text-sm text-slate-500">
            Already have an account?{' '}
            <Link to="/login" className="text-yellow-400 hover:underline font-medium">
              Sign in
            </Link>
          </p>
        </div>
      </div>
    </div>
  )
}
