import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Linkedin, Github, Twitter, ArrowUp } from 'lucide-react'

export default function Footer() {
  const [showBackToTop, setShowBackToTop] = useState(false)

  useEffect(() => {
    const handleScroll = () => {
      setShowBackToTop(window.scrollY > 300)
    }
    window.addEventListener('scroll', handleScroll, { passive: true })
    return () => window.removeEventListener('scroll', handleScroll)
  }, [])

  const scrollToTop = () => {
    window.scrollTo({ top: 0, behavior: 'smooth' })
  }

  return (
    <footer className="bg-slate-950 border-t border-slate-800 relative">
      {/* Main footer content */}
      <div className="max-w-7xl mx-auto px-6 lg:px-8 py-16">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-12 mb-12">
          {/* Brand & Mission */}
          <div className="space-y-6">
            <div>
              <Link to="/" className="flex items-center gap-2 group mb-4">
                <span className="w-8 h-8 rounded-lg bg-yellow-500 flex items-center justify-center text-slate-900 font-black text-sm shadow-[0_0_16px_rgba(234,179,8,0.4)]">
                  L
                </span>
                <span className="font-extrabold text-lg tracking-tight text-white">
                  Lead<span className="text-yellow-400">Flow</span>
                </span>
              </Link>
              <p className="text-sm text-slate-400 leading-relaxed">
                The world's most powerful AI-driven outbound engine. Built for agencies and winners. From Kranj to the World.
              </p>
            </div>

            {/* Social icons */}
            <div className="flex items-center gap-4 pt-2">
              <a
                href="https://linkedin.com"
                target="_blank"
                rel="noopener noreferrer"
                className="w-10 h-10 rounded-lg border border-slate-700 flex items-center justify-center text-slate-400 hover:text-yellow-400 hover:border-yellow-500/50 transition-colors"
                aria-label="LinkedIn"
              >
                <Linkedin className="w-5 h-5" />
              </a>
              <a
                href="https://twitter.com"
                target="_blank"
                rel="noopener noreferrer"
                className="w-10 h-10 rounded-lg border border-slate-700 flex items-center justify-center text-slate-400 hover:text-yellow-400 hover:border-yellow-500/50 transition-colors"
                aria-label="X (Twitter)"
              >
                <Twitter className="w-5 h-5" />
              </a>
              <a
                href="https://github.com"
                target="_blank"
                rel="noopener noreferrer"
                className="w-10 h-10 rounded-lg border border-slate-700 flex items-center justify-center text-slate-400 hover:text-yellow-400 hover:border-yellow-500/50 transition-colors"
                aria-label="GitHub"
              >
                <Github className="w-5 h-5" />
              </a>
            </div>
          </div>

          {/* Product & Features */}
          <div>
            <h3 className="font-bold text-white mb-6 text-sm uppercase tracking-wider">Product</h3>
            <ul className="space-y-3 text-sm">
              <li>
                <Link to="/features/search-scrape" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  Search & Scrape
                </Link>
              </li>
              <li>
                <Link to="/features/ai-enrichment" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  AI Enrichment
                </Link>
              </li>
              <li>
                <Link to="/features/email-automation" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  Email Automation
                </Link>
              </li>
              <li>
                <span className="inline-flex items-center gap-2">
                  <span className="text-slate-400">
                    Chrome Extension
                  </span>
                  <span className="px-2 py-0.5 rounded-full bg-yellow-500/20 text-yellow-400 text-xs font-semibold">
                    Coming Soon
                  </span>
                </span>
              </li>
            </ul>
          </div>

          {/* Resources & Support */}
          <div>
            <h3 className="font-bold text-white mb-6 text-sm uppercase tracking-wider">Developers</h3>
            <ul className="space-y-3 text-sm">
              <li>
                <Link to="/blog" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  Blog
                </Link>
              </li>
              <li>
                <Link to="/help" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  Help Center
                </Link>
              </li>
              <li>
                <Link to="/docs" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  API Docs
                </Link>
              </li>
              <li>
                <Link to="/redeem" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  AppSumo Redemption
                </Link>
              </li>
              <li>
                <Link to="/status" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  System Status
                </Link>
              </li>
            </ul>
          </div>

          {/* Legal & Company */}
          <div>
            <h3 className="font-bold text-white mb-6 text-sm uppercase tracking-wider">Legal</h3>
            <ul className="space-y-3 text-sm">
              <li>
                <Link to="/legal/terms" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  Terms of Service
                </Link>
              </li>
              <li>
                <Link to="/legal/privacy" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  Privacy Policy
                </Link>
              </li>
              <li>
                <Link to="/legal/cookies" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  Cookie Policy
                </Link>
              </li>
              <li>
                <Link to="/legal/gdpr" className="text-slate-400 hover:text-yellow-400 transition-colors">
                  GDPR Compliance
                </Link>
              </li>
            </ul>
          </div>
        </div>

        {/* Divider */}
        <div className="border-t border-slate-800" />

        {/* Bottom bar */}
        <div className="pt-8 flex flex-col sm:flex-row items-center justify-between gap-4">
          <p className="text-xs text-slate-500">
            © 2026 Sniped Inc. All rights reserved.
          </p>
          <p className="text-xs text-slate-500 font-medium">
            Global Coverage: 195+ Countries Supported 🌍
          </p>
        </div>
      </div>

      {/* Back to top button */}
      {showBackToTop && (
        <button
          onClick={scrollToTop}
          className="fixed bottom-8 right-8 w-12 h-12 rounded-lg bg-yellow-500 text-slate-900 flex items-center justify-center hover:bg-yellow-400 transition-all shadow-[0_0_24px_rgba(234,179,8,0.4)] hover:shadow-[0_0_32px_rgba(234,179,8,0.6)] z-40"
          aria-label="Back to top"
        >
          <ArrowUp className="w-5 h-5" />
        </button>
      )}
    </footer>
  )
}
