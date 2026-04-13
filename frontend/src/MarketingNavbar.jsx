import { useState } from 'react'
import { getStoredValue } from './authStorage.js'
import { FEATURE_PAGES } from './featurePagesData'

export default function MarketingNavbar() {
  const [menuOpen, setMenuOpen] = useState(false)
  const [featuresMenuOpen, setFeaturesMenuOpen] = useState(false)
  const isLoggedIn = Boolean(getStoredValue('lf_token'))

  return (
    <nav className="fixed top-0 inset-x-0 z-50 bg-slate-950/90 backdrop-blur-xl border-b border-white/10">
      <div className="max-w-7xl mx-auto px-6 lg:px-8 h-16 flex items-center justify-between">
        <a href="/?stay=1" className="flex items-center gap-2 group" aria-label="Go to Sniped landing page">
          <span className="w-8 h-8 rounded-lg bg-yellow-500 flex items-center justify-center text-slate-900 font-black text-sm">S</span>
          <span className="font-extrabold text-xl tracking-tight text-white">
            Sni<span className="text-yellow-400">ped</span>
          </span>
        </a>

        <div className="hidden md:flex items-center gap-8">
          <div
            className="relative"
            onMouseEnter={() => setFeaturesMenuOpen(true)}
            onMouseLeave={() => setFeaturesMenuOpen(false)}
          >
            <a
              href="/?stay=1"
              onClick={(e) => {
                if (!featuresMenuOpen) {
                  e.preventDefault()
                  setFeaturesMenuOpen(true)
                }
              }}
              className="text-sm text-slate-300 hover:text-white transition-colors"
            >
              Features
            </a>
            {featuresMenuOpen && (
              <div className="absolute left-0 top-full w-56 pt-2">
                <div className="rounded-xl border border-white/10 bg-slate-950 p-2 shadow-[0_20px_60px_rgba(0,0,0,0.45)]">
                  {FEATURE_PAGES.map((feature) => (
                    <a
                      key={feature.slug}
                      href={`/features/${feature.slug}`}
                      className="block rounded-lg px-3 py-2 text-sm text-slate-300 hover:bg-white/5 hover:text-yellow-300 transition-colors"
                    >
                      {feature.label}
                    </a>
                  ))}
                </div>
              </div>
            )}
          </div>
          <a href="/?stay=1#pricing" className="text-sm text-slate-300 hover:text-white transition-colors">Pricing</a>
          <a href="/faq" className="text-sm text-slate-300 hover:text-white transition-colors">FAQ</a>
        </div>

        <div className="hidden md:flex items-center gap-3">
          {isLoggedIn ? (
            <a href="/app" className="text-sm font-semibold px-5 py-2.5 rounded-xl bg-yellow-500 text-slate-900 hover:bg-yellow-400 transition-colors">
              Start Now
            </a>
          ) : (
            <>
              <a href="/login" className="text-sm text-slate-300 hover:text-white transition-colors px-3 py-2">Login</a>
              <a href="/get-started" className="text-sm font-semibold px-5 py-2.5 rounded-xl bg-yellow-500 text-slate-900 hover:bg-yellow-400 transition-colors">
                Get Started
              </a>
            </>
          )}
        </div>

        <button className="md:hidden p-2 text-slate-400" onClick={() => setMenuOpen((v) => !v)}>
          <span className="block w-5 h-px bg-current mb-1.5" />
          <span className="block w-5 h-px bg-current mb-1.5" />
          <span className="block w-5 h-px bg-current" />
        </button>
      </div>

      {menuOpen && (
        <div className="md:hidden bg-slate-950 border-t border-white/10 px-6 py-4 flex flex-col gap-3">
          <a href="/?stay=1" onClick={() => setMenuOpen(false)} className="text-sm text-slate-300 hover:text-white">Features</a>
          <div className="ml-3 flex flex-col gap-2 border-l border-white/10 pl-3">
            {FEATURE_PAGES.map((feature) => (
              <a
                key={feature.slug}
                href={`/features/${feature.slug}`}
                onClick={() => setMenuOpen(false)}
                className="text-sm text-slate-400 hover:text-yellow-300"
              >
                {feature.label}
              </a>
            ))}
          </div>
          <a href="/?stay=1#pricing" onClick={() => setMenuOpen(false)} className="text-sm text-slate-300 hover:text-white">Pricing</a>
          <a href="/faq" onClick={() => setMenuOpen(false)} className="text-sm text-slate-300 hover:text-white">FAQ</a>
          {isLoggedIn ? (
            <a href="/app" onClick={() => setMenuOpen(false)} className="mt-2 block w-full py-3 rounded-xl bg-yellow-500 text-slate-900 font-bold text-sm text-center">
              Start Now
            </a>
          ) : (
            <a href="/get-started" onClick={() => setMenuOpen(false)} className="mt-2 block w-full py-3 rounded-xl bg-yellow-500 text-slate-900 font-bold text-sm text-center">
              Get Started
            </a>
          )}
        </div>
      )}
    </nav>
  )
}
