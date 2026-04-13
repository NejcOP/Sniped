import { useState } from 'react'
import { Link } from 'react-router-dom'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'
import { getStoredValue } from './authStorage'

export default function AppSumoRedemptionPage() {
  const [coupon, setCoupon] = useState('')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [result, setResult] = useState(null)
  const [error, setError] = useState('')

  async function onRedeem() {
    const token = getStoredValue('lf_token')
    const couponCode = String(coupon || '').trim()

    setError('')
    setResult(null)

    if (!token) {
      setError('Please log in first to redeem your AppSumo code.')
      return
    }
    if (!couponCode) {
      setError('Please enter a coupon code.')
      return
    }

    setIsSubmitting(true)
    try {
      const response = await fetch('/api/redeem/appsumo', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ coupon_code: couponCode }),
      })
      const data = await response.json().catch(() => ({}))
      if (!response.ok) {
        setError(typeof data?.detail === 'string' ? data.detail : 'Redemption failed.')
        return
      }
      setResult(data)
    } catch {
      setError('Network error. Please try again.')
    } finally {
      setIsSubmitting(false)
    }
  }

  return (
    <div className="min-h-screen flex flex-col bg-slate-950">
      <MarketingNavbar />
      <main className="flex-1 max-w-3xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="mb-8 flex items-center justify-between gap-4 flex-wrap">
          <div>
            <p className="text-xs uppercase tracking-[0.25em] text-cyan-300">Developers</p>
            <h1 className="mt-2 text-4xl font-black tracking-tight text-white">AppSumo Redemption</h1>
            <p className="mt-3 text-slate-400">Activate your deal code and continue onboarding in seconds.</p>
          </div>
          <Link
            to="/app"
            className="inline-flex items-center rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm font-semibold text-cyan-200 hover:bg-cyan-400/20 transition-colors"
          >
            Back to Dashboard
          </Link>
        </div>

        <section className="rounded-2xl border border-white/10 bg-slate-900/70 p-7 shadow-[0_0_0_1px_rgba(56,189,248,0.08),0_18px_60px_rgba(0,8,20,0.55)]">
          <label className="block text-sm font-semibold text-slate-200 mb-2" htmlFor="coupon-input">
            Vnos kupona
          </label>
          <input
            id="coupon-input"
            type="text"
            value={coupon}
            onChange={(e) => setCoupon(e.target.value)}
            placeholder="APPSUMO-XXXX-XXXX"
            className="w-full rounded-xl border border-cyan-400/25 bg-slate-950/70 px-4 py-3 text-slate-100 placeholder:text-slate-500 outline-none focus:border-cyan-300"
          />
          <button
            type="button"
            disabled={isSubmitting}
            onClick={onRedeem}
            className="mt-4 inline-flex items-center rounded-xl bg-cyan-400 px-5 py-3 text-sm font-bold text-slate-950 hover:bg-cyan-300 transition-colors"
          >
            {isSubmitting ? 'Redeeming...' : 'Redeem Now'}
          </button>
          {error ? (
            <p className="mt-3 text-sm text-red-300">{error}</p>
          ) : null}
          {result ? (
            <div className="mt-3 rounded-xl border border-emerald-400/30 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-200">
              <p className="font-semibold">{result.message || 'Coupon redeemed successfully.'}</p>
              {result.coupon_code ? <p className="mt-1 text-emerald-100/90">Code: {result.coupon_code}</p> : null}
            </div>
          ) : null}
        </section>
      </main>
      <Footer />
    </div>
  )
}
