/**
 * ScrapeProgressBar — Real-time scraping progress indicator
 *
 * PERFORMANCE RULES IMPLEMENTED:
 * - CSS transform for bar fill (no layout reflow, GPU composited).
 * - Fixed height prevents layout shift.
 * - Framer Motion AnimatePresence for zero-cost entrance/exit.
 * - Consumes useScrapeProgress hook — no prop-drilling required.
 */
import { AnimatePresence, motion as Motion } from 'framer-motion'
import { useScrapeProgress } from '../hooks/useScrapeProgress'

const TASK_LABELS = {
  scrape: 'Scraping leads',
  enrich: 'Enriching leads',
  mailer: 'Sending emails',
}

export function ScrapeProgressBar() {
  const { isRunning, taskType, percent, found, total, phase } = useScrapeProgress()

  return (
    <AnimatePresence>
      {isRunning && (
        <Motion.div
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.22 }}
          /* Fixed height prevents CLS */
          className="w-full overflow-hidden rounded-full bg-white/8"
          style={{ height: 6 }}
          role="progressbar"
          aria-valuenow={percent}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-label={`${TASK_LABELS[taskType] ?? 'Working'}: ${percent}%`}
        >
          {/* GPU-composited fill — no layout reflow */}
          <Motion.div
            className="h-full rounded-full bg-gradient-to-r from-cyan-400 to-violet-500"
            style={{ originX: 0 }}
            animate={{ scaleX: percent / 100 }}
            transition={{ type: 'spring', stiffness: 180, damping: 26 }}
          />
        </Motion.div>
      )}
    </AnimatePresence>
  )
}

/**
 * ScrapeProgressBadge — compact inline badge version for toolbar areas
 */
export function ScrapeProgressBadge() {
  const { isRunning, taskType, percent, found, total, phase } = useScrapeProgress()
  if (!isRunning) return null

  const label = TASK_LABELS[taskType] ?? 'Working'
  const countLabel = total > 0 ? `${found}/${total}` : `${found} found`

  return (
    <Motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
      transition={{ duration: 0.18 }}
      className="flex items-center gap-2 rounded-full border border-cyan-500/30 bg-cyan-500/10 px-3 py-1 text-xs text-cyan-300"
    >
      {/* Pulsing dot */}
      <span className="relative flex h-2 w-2">
        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-cyan-400 opacity-75" />
        <span className="relative inline-flex h-2 w-2 rounded-full bg-cyan-400" />
      </span>
      <span>{label} — {countLabel}</span>
      <span className="font-semibold">{percent}%</span>
    </Motion.div>
  )
}
