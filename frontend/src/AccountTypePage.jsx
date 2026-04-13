import { useEffect, useState } from 'react'
import { useNavigate, Link } from 'react-router-dom'
import { Zap, User, Building2, Briefcase, Rocket, ArrowRight } from 'lucide-react'
import { getStoredValue } from './authStorage'

const ACCOUNT_TYPES = [
  {
    id: 'entrepreneur',
    icon: Rocket,
    label: 'Entrepreneur',
    description: 'Solo founder building something new',
    nameLabel: 'your first + last name',
  },
  {
    id: 'freelancer',
    icon: User,
    label: 'Freelancer',
    description: 'Independent contractor or consultant',
    nameLabel: 'your first + last name',
  },
  {
    id: 'agency',
    icon: Briefcase,
    label: 'Agency',
    description: 'Marketing, design or service agency',
    nameLabel: 'your agency name',
  },
  {
    id: 'company',
    icon: Building2,
    label: 'Company',
    description: 'Established business or startup',
    nameLabel: 'your company name',
  },
]

export default function AccountTypePage() {
  const navigate = useNavigate()
  const [selected, setSelected] = useState('')

  useEffect(() => {
    if (getStoredValue('lf_token')) {
      navigate('/app', { replace: true })
    }
  }, [navigate])

  function handleContinue() {
    if (!selected) return
    navigate(`/signup?accountType=${selected}`)
  }

  return (
    <div className="min-h-screen flex items-center justify-center px-4 py-12" style={{ background: '#060d1c' }}>
      <div className="w-full max-w-lg">
        {/* Logo */}
        <Link to="/?stay=1" className="flex items-center justify-center gap-2 mb-10 transition-opacity hover:opacity-90" aria-label="Go to Sniped landing page">
          <Zap size={28} className="text-yellow-400" />
          <span className="text-2xl font-bold text-white tracking-tight">Sniped</span>
        </Link>

        <div
          className="rounded-2xl p-8"
          style={{ background: 'rgba(255,255,255,0.04)', border: '1px solid rgba(255,255,255,0.08)' }}
        >
          <h1 className="text-2xl font-bold text-white mb-1">Who are you?</h1>
          <p className="text-sm text-slate-400 mb-7">
            We'll personalise your experience based on your account type.
          </p>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 mb-8">
            {ACCOUNT_TYPES.map(({ id, icon, label, description }) => {
              const active = selected === id
              const IconComponent = icon
              return (
                <button
                  key={id}
                  type="button"
                  onClick={() => setSelected(id)}
                  className="flex items-start gap-3 px-4 py-4 rounded-xl text-left transition-all"
                  style={{
                    background: active ? 'rgba(245,158,11,0.1)' : 'rgba(255,255,255,0.04)',
                    border: active
                      ? '1.5px solid rgba(245,158,11,0.55)'
                      : '1.5px solid rgba(255,255,255,0.07)',
                  }}
                >
                  <span
                    className="mt-0.5 w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 transition-colors"
                    style={{
                      background: active ? 'rgba(245,158,11,0.18)' : 'rgba(255,255,255,0.06)',
                    }}
                  >
                    <IconComponent size={16} className={active ? 'text-yellow-400' : 'text-slate-400'} />
                  </span>
                  <span>
                    <span className={`block text-sm font-semibold ${active ? 'text-yellow-400' : 'text-white'}`}>
                      {label}
                    </span>
                    <span className="block text-xs text-slate-500 mt-0.5 leading-snug">{description}</span>
                  </span>
                </button>
              )
            })}
          </div>

          <button
            onClick={handleContinue}
            disabled={!selected}
            className="w-full flex items-center justify-center gap-2 py-3 rounded-xl font-semibold text-sm transition-all disabled:opacity-30 disabled:cursor-not-allowed"
            style={{
              background: selected ? 'linear-gradient(135deg,#f59e0b,#d97706)' : 'rgba(255,255,255,0.08)',
              color: selected ? '#000' : '#fff',
            }}
          >
            Continue
            <ArrowRight size={15} />
          </button>

          <p className="mt-5 text-center text-sm text-slate-500">
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
