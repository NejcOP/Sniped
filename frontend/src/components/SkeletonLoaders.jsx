/**
 * SkeletonCard — Zero Layout Shift (CLS) skeleton loader
 *
 * PERFORMANCE RULES IMPLEMENTED:
 * - Fixed dimensions match real lead card exactly → CLS = 0.
 * - CSS-only shimmer animation (no JS animation frames wasted).
 * - Rendered as a static list of N items so the scroll height doesn't jump.
 * - Uses will-change: transform on the shimmer layer only.
 */

/* ── Shimmer strip (injected once via a <style> tag) ── */
const SHIMMER_STYLE = `
@keyframes sniped-shimmer {
  0%   { transform: translateX(-100%); }
  100% { transform: translateX(100%); }
}
.sniped-shimmer::after {
  content: '';
  position: absolute;
  inset: 0;
  background: linear-gradient(
    90deg,
    transparent 0%,
    rgba(255,255,255,0.06) 50%,
    transparent 100%
  );
  will-change: transform;
  animation: sniped-shimmer 1.4s ease-in-out infinite;
}
`

let shimmerInjected = false
function ensureShimmerStyle() {
  if (shimmerInjected) return
  const el = document.createElement('style')
  el.textContent = SHIMMER_STYLE
  document.head.appendChild(el)
  shimmerInjected = true
}

/** Single skeleton card row — dimensions must match the real lead card. */
function SkeletonRow() {
  return (
    /* Fixed height matches the real LeadCard — prevents layout shift */
    <div
      className="relative overflow-hidden rounded-xl border border-white/5 bg-white/3 p-4"
      style={{ minHeight: 88 }}
      aria-hidden="true"
    >
      <div className="sniped-shimmer relative flex items-start gap-3 overflow-hidden">
        {/* Avatar circle — fixed 36×36 */}
        <div
          className="shrink-0 rounded-full bg-white/8"
          style={{ width: 36, height: 36 }}
        />
        <div className="flex-1 space-y-2">
          {/* Business name bar */}
          <div className="h-4 w-1/2 rounded bg-white/8" />
          {/* Meta line */}
          <div className="h-3 w-3/4 rounded bg-white/5" />
          {/* Badge strip */}
          <div className="flex gap-2 pt-1">
            <div className="h-5 w-16 rounded-full bg-white/6" />
            <div className="h-5 w-20 rounded-full bg-white/6" />
          </div>
        </div>
        {/* Score circle — fixed 44×44 */}
        <div
          className="shrink-0 rounded-full bg-white/8"
          style={{ width: 44, height: 44 }}
        />
      </div>
    </div>
  )
}

/**
 * LeadCardSkeletonList
 * @param {{ count?: number }} props
 */
export function LeadCardSkeletonList({ count = 8 }) {
  ensureShimmerStyle()
  return (
    <div
      className="space-y-3"
      /* Reserve vertical space so the page doesn't jump when real cards arrive.
         88px card + 12px gap × count — prevents CLS */
      style={{ minHeight: (88 + 12) * count - 12 }}
    >
      {Array.from({ length: count }, (_, i) => (
        <SkeletonRow key={i} />
      ))}
    </div>
  )
}

/**
 * StatCardSkeleton — for dashboard KPI tiles
 * @param {{ count?: number }} props
 */
export function StatCardSkeletonList({ count = 4 }) {
  ensureShimmerStyle()
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      {Array.from({ length: count }, (_, i) => (
        <div
          key={i}
          className="sniped-shimmer relative overflow-hidden rounded-xl border border-white/5 bg-white/3 p-4"
          style={{ minHeight: 72 }}
          aria-hidden="true"
        >
          <div className="h-3 w-2/3 rounded bg-white/8 mb-2" />
          <div className="h-6 w-1/3 rounded bg-white/10" />
        </div>
      ))}
    </div>
  )
}
