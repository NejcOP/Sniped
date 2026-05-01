import { Sparkles } from 'lucide-react'
import { AnimatePresence, motion as Motion } from 'framer-motion'
import { useEffect, useState } from 'react'

export default function OnboardingWizard({
  open,
  onClose,
  onComplete,
  submitting = false,
  title = 'Let\'s launch your first lead stream',
  subtitle = 'Welcome to Sniped',
  completeCta = 'Run Magic Search',
}) {
  const [step, setStep] = useState(1)
  const [draft, setDraft] = useState({ niche: '', location: '' })

  useEffect(() => {
    if (!open) return
    setStep(1)
    setDraft({ niche: '', location: '' })
  }, [open])

  const niche = String(draft.niche || '').trim()
  const location = String(draft.location || '').trim()

  const canContinue = (step === 1 && niche) || (step === 2 && location)

  const handleComplete = async () => {
    if (!niche || !location || submitting) return
    await onComplete?.({ niche, location })
  }

  return (
    <AnimatePresence>
      {open ? (
        <Motion.div
          className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/75 px-4 backdrop-blur-sm"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
        >
          <Motion.div
            className="w-full max-w-2xl overflow-hidden rounded-[28px] border border-cyan-500/30 bg-[linear-gradient(135deg,rgba(8,47,73,0.55),rgba(15,23,42,0.98))] p-6 shadow-[0_30px_90px_rgba(8,47,73,0.45)]"
            initial={{ y: 24, scale: 0.98, opacity: 0 }}
            animate={{ y: 0, scale: 1, opacity: 1 }}
            exit={{ y: 18, scale: 0.98, opacity: 0 }}
            transition={{ duration: 0.28, ease: 'easeOut' }}
          >
            <div className="mb-4 flex items-start justify-between gap-3">
              <div>
                <p className="label-overline text-cyan-300">{subtitle}</p>
                <h3 className="mt-1 text-2xl font-semibold text-white">{title}</h3>
                <p className="mt-1 text-sm text-slate-300">Step {step} of 3</p>
              </div>
              <button
                type="button"
                className="btn-ghost px-3 py-2 text-xs"
                onClick={() => onClose?.()}
                disabled={submitting}
              >
                Skip for now
              </button>
            </div>

            <div className="mb-5 h-1.5 w-full overflow-hidden rounded-full bg-slate-800/80">
              <Motion.div
                className="h-full rounded-full bg-gradient-to-r from-cyan-400 via-sky-500 to-emerald-400"
                animate={{ width: `${(step / 3) * 100}%` }}
                transition={{ duration: 0.35, ease: 'easeOut' }}
              />
            </div>

            <AnimatePresence mode="wait">
              {step === 1 ? (
                <Motion.div
                  key="onboarding-step-1"
                  initial={{ x: 20, opacity: 0 }}
                  animate={{ x: 0, opacity: 1 }}
                  exit={{ x: -20, opacity: 0 }}
                  transition={{ duration: 0.22 }}
                >
                  <p className="text-sm text-slate-300">Step 1: What is your niche?</p>
                  <input
                    className="glass-input mt-3 h-11 w-full"
                    type="text"
                    placeholder="e.g. plumbers, dentists, roofers"
                    value={draft.niche}
                    onChange={(e) => setDraft((prev) => ({ ...prev, niche: e.target.value }))}
                    autoFocus
                  />
                </Motion.div>
              ) : null}

              {step === 2 ? (
                <Motion.div
                  key="onboarding-step-2"
                  initial={{ x: 20, opacity: 0 }}
                  animate={{ x: 0, opacity: 1 }}
                  exit={{ x: -20, opacity: 0 }}
                  transition={{ duration: 0.22 }}
                >
                  <p className="text-sm text-slate-300">Step 2: Where are your clients?</p>
                  <input
                    className="glass-input mt-3 h-11 w-full"
                    type="text"
                    placeholder="e.g. London, UK"
                    value={draft.location}
                    onChange={(e) => setDraft((prev) => ({ ...prev, location: e.target.value }))}
                    autoFocus
                  />
                </Motion.div>
              ) : null}

              {step === 3 ? (
                <Motion.div
                  key="onboarding-step-3"
                  initial={{ x: 20, opacity: 0 }}
                  animate={{ x: 0, opacity: 1 }}
                  exit={{ x: -20, opacity: 0 }}
                  transition={{ duration: 0.22 }}
                >
                  <p className="text-sm text-slate-300">Step 3: Magic Search</p>
                  <div className="mt-3 rounded-2xl border border-cyan-500/25 bg-slate-900/70 p-4">
                    <p className="text-xs uppercase tracking-[0.14em] text-cyan-300">Your first search</p>
                    <p className="mt-2 text-lg font-semibold text-white">
                      {niche || 'Your niche'} in {location || 'your location'}
                    </p>
                    <p className="mt-2 text-xs text-slate-400">
                      We will run your guided setup flow and keep your existing data intact.
                    </p>
                  </div>
                </Motion.div>
              ) : null}
            </AnimatePresence>

            <div className="mt-6 flex flex-wrap items-center justify-between gap-3">
              <button
                type="button"
                className="btn-ghost px-3 py-2 text-sm"
                disabled={step <= 1 || submitting}
                onClick={() => setStep((prev) => Math.max(1, prev - 1))}
              >
                Back
              </button>

              {step < 3 ? (
                <button
                  type="button"
                  className="btn-primary px-4 py-2"
                  disabled={!canContinue || submitting}
                  onClick={() => setStep((prev) => Math.min(3, prev + 1))}
                >
                  Continue
                </button>
              ) : (
                <button
                  type="button"
                  className="btn-primary px-4 py-2"
                  disabled={submitting}
                  onClick={() => void handleComplete()}
                >
                  <Sparkles className={`h-4 w-4 ${submitting ? 'animate-spin' : ''}`} />
                  {submitting ? 'Finishing…' : completeCta}
                </button>
              )}
            </div>
          </Motion.div>
        </Motion.div>
      ) : null}
    </AnimatePresence>
  )
}
