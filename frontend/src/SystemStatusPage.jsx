import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import Footer from './Footer'
import MarketingNavbar from './MarketingNavbar'

const fallbackServices = [
  { key: 'lead_search_api', label: 'Lead Search API', status: 'Operational', operational: true },
  { key: 'enrichment_engine', label: 'Enrichment Engine', status: 'Operational', operational: true },
  { key: 'database', label: 'Database', status: 'Operational', operational: true },
]

export default function SystemStatusPage() {
  const [services, setServices] = useState(fallbackServices)
  const [lastUpdated, setLastUpdated] = useState('')
  const [isRefreshing, setIsRefreshing] = useState(false)

  useEffect(() => {
    let isMounted = true

    async function loadStatus() {
      setIsRefreshing(true)
      try {
        const response = await fetch('/api/system-status')
        const data = await response.json().catch(() => ({}))
        if (!response.ok) throw new Error('Status API failed')
        if (!isMounted) return

        const apiServices = Array.isArray(data?.services) && data.services.length > 0
          ? data.services
          : fallbackServices

        setServices(apiServices)
        setLastUpdated(String(data?.updated_at || ''))
      } catch {
        if (!isMounted) return
        setServices(
          fallbackServices.map((service) => ({
            ...service,
            status: 'Degraded',
            operational: false,
          })),
        )
      } finally {
        if (isMounted) setIsRefreshing(false)
      }
    }

    loadStatus()
    const intervalId = window.setInterval(loadStatus, 15000)

    return () => {
      isMounted = false
      window.clearInterval(intervalId)
    }
  }, [])

  const updatedLabel = lastUpdated
    ? new Date(lastUpdated).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' })
    : null

  return (
    <div className="min-h-screen flex flex-col bg-slate-950">
      <MarketingNavbar />
      <main className="flex-1 max-w-4xl mx-auto w-full px-6 lg:px-8 pt-24 pb-20">
        <div className="mb-8 flex items-center justify-between gap-4 flex-wrap">
          <div>
            <p className="text-xs uppercase tracking-[0.25em] text-cyan-300">Developers</p>
            <h1 className="mt-2 text-4xl font-black tracking-tight text-white">System Status</h1>
            <p className="mt-3 text-slate-400">Live service health across core Sniped infrastructure.</p>
          </div>
          <Link
            to="/app"
            className="inline-flex items-center rounded-xl border border-cyan-400/30 bg-cyan-400/10 px-4 py-2 text-sm font-semibold text-cyan-200 hover:bg-cyan-400/20 transition-colors"
          >
            Back to Dashboard
          </Link>
        </div>

        <div className="rounded-2xl border border-white/10 bg-slate-900/65 p-6 space-y-4">
          <div className="flex items-center justify-between gap-3 text-xs text-slate-400">
            <span>{isRefreshing ? 'Refreshing status...' : 'Live polling every 15s'}</span>
            <span>{updatedLabel ? `Updated: ${updatedLabel}` : 'Updated: --:--'}</span>
          </div>
          {services.map((service) => (
            <div
              key={service.key || service.label}
              className="rounded-xl border border-white/10 bg-slate-950/70 p-4"
            >
              <div className="flex items-center justify-between gap-4">
                <p className="font-semibold text-white">{service.label}</p>
                <span className={`text-sm font-semibold ${service.operational ? 'text-emerald-300' : 'text-amber-300'}`}>
                  {service.status || (service.operational ? 'Operational' : 'Degraded')}
                </span>
              </div>
              <div className={`mt-3 h-2 rounded-full overflow-hidden ${service.operational ? 'bg-emerald-900/40' : 'bg-amber-900/40'}`}>
                <div
                  className={`h-full w-full ${service.operational ? 'bg-emerald-400 shadow-[0_0_18px_rgba(16,185,129,0.65)]' : 'bg-amber-400 shadow-[0_0_18px_rgba(245,158,11,0.6)]'}`}
                />
              </div>
            </div>
          ))}
        </div>
      </main>
      <Footer />
    </div>
  )
}
