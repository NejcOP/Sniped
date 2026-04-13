import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { Zap, Mail, Lock, LogIn, Eye, EyeOff } from 'lucide-react'
import { getRememberPreference, getRememberedEmail, getStoredValue, setAuthSession, setRememberedEmail } from './authStorage'

const API_BASE = import.meta.env.VITE_API_URL ?? ''

export default function LoginPage() {
  const navigate = useNavigate()
  const [email, setEmail] = useState(() => getRememberedEmail())
  const [password, setPassword] = useState('')
  const [showPassword, setShowPassword] = useState(false)
  const [rememberMe, setRememberMe] = useState(() => getRememberPreference())
  const [loading, setLoading] = useState(false)
  const [loginError, setLoginError] = useState('')

  useEffect(() => {
    const existingToken = getStoredValue('lf_token')
    if (existingToken) {
      navigate('/app', { replace: true })
    }
  }, [navigate])

  async function handleSubmit(e) {
    e.preventDefault()
    setLoginError('')
    setLoading(true)
    try {
      const res = await fetch(`${API_BASE}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: email.trim().toLowerCase(), password }),
      })
      const raw = await res.text()
      let data = {}
      try {
        data = raw ? JSON.parse(raw) : {}
      } catch {
        data = { detail: raw || 'Unknown server response.' }
      }
      if (!res.ok) {
        const detail = typeof data?.detail === 'string' ? data.detail : ''
        if (res.status === 401) {
          setLoginError(detail || 'Invalid email or password.')
        } else {
          setLoginError(detail || 'Login failed. Please try again.')
        }
        return
      }
      setAuthSession(
        {
          lf_token: data.token,
          lf_niche: data.niche,
          lf_email: data.email,
          lf_display_name: data.display_name || '',
          lf_contact_name: data.contact_name || '',
          lf_account_type: data.account_type || '',
        },
        rememberMe,
      )
      setRememberedEmail(data.email, rememberMe)
      // Clear any pending signup data to avoid auth race conditions
      localStorage.removeItem('lf_pending_signup')
      navigate('/app')
    } catch {
      setLoginError('Could not connect to the server. Please try again.')
      return
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4" style={{ background: '#060d1c' }}>
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
          <h1 className="text-2xl font-bold text-white mb-1">Login</h1>
          <p className="text-sm text-slate-400 mb-6">Access your Sniped account</p>

          {loginError ? (
            <div
              className="mb-4 rounded-lg px-3 py-2 text-sm"
              role="alert"
              style={{ background: 'rgba(239,68,68,0.15)', border: '1px solid rgba(239,68,68,0.35)', color: '#fecaca' }}
            >
              {loginError}
            </div>
          ) : null}

          <form onSubmit={handleSubmit} className="space-y-4">
            {/* Email */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">Email</label>
              <div className="relative">
                <Mail size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
                <input
                  type="email"
                  required
                  value={email}
                  onChange={e => {
                    setEmail(e.target.value)
                    if (loginError) setLoginError('')
                  }}
                  placeholder="you@agency.com"
                  className="w-full pl-9 pr-4 py-2.5 rounded-lg text-sm text-white placeholder-slate-600 outline-none focus:ring-2 focus:ring-yellow-400/40"
                  style={{ background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.1)' }}
                />
              </div>
            </div>

            {/* Password */}
            <div>
              <label className="block text-xs font-medium text-slate-400 mb-1">Password</label>
              <div className="relative">
                <Lock size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500" />
                <input
                  type={showPassword ? 'text' : 'password'}
                  required
                  value={password}
                  onChange={e => {
                    setPassword(e.target.value)
                    if (loginError) setLoginError('')
                  }}
                  placeholder="••••••••"
                  className="w-full pl-9 pr-10 py-2.5 rounded-lg text-sm text-white placeholder-slate-600 outline-none focus:ring-2 focus:ring-yellow-400/40"
                  style={{ background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.1)' }}
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((current) => !current)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-500 hover:text-white transition-colors"
                  aria-label={showPassword ? 'Hide password' : 'Show password'}
                >
                  {showPassword ? <EyeOff size={15} /> : <Eye size={15} />}
                </button>
              </div>
            </div>

            <label className="flex items-center gap-2 text-sm text-slate-400 select-none">
              <input
                type="checkbox"
                checked={rememberMe}
                onChange={e => setRememberMe(e.target.checked)}
                className="h-4 w-4 rounded border-white/20 bg-white/5 text-yellow-400 focus:ring-yellow-400/40"
              />
              Remember me
            </label>

            <div className="flex justify-end">
              <Link to="/forgot-password" className="text-xs font-medium text-slate-400 hover:text-yellow-400 transition-colors">
                Forgot password?
              </Link>
            </div>

            <button
              type="submit"
              disabled={loading}
              className="w-full flex items-center justify-center gap-2 py-2.5 rounded-lg font-semibold text-sm transition-opacity disabled:opacity-50"
              style={{ background: 'linear-gradient(135deg,#f59e0b,#d97706)', color: '#000' }}
            >
              <LogIn size={15} />
              {loading ? 'Logging in…' : 'Login'}
            </button>
          </form>

          <p className="mt-6 text-center text-sm text-slate-500">
            Don't have an account?{' '}
            <Link to="/get-started" className="text-yellow-400 hover:underline font-medium">
              Sign up free
            </Link>
          </p>
        </div>
      </div>
    </div>
  )
}
