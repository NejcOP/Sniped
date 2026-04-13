/**
 * useScrapeProgress — real-time scraping progress tracker
 *
 * PERFORMANCE RULES IMPLEMENTED:
 * - User never waits on a page reload — progress is polled via lightweight interval.
 * - Exponential backoff on failure to avoid hammering the server.
 * - AbortController cleans up fetch on unmount.
 * - Progress bar percentage is derived server-side from task.result.found / task.result.total.
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import { getStoredValue } from '../authStorage'

const POLL_INTERVAL_MS = 1_500   // poll every 1.5s while running
const BACKOFF_MAX_MS = 10_000    // max backoff after failures

async function fetchTaskState(signal) {
  const token = getStoredValue('lf_token')
  const res = await fetch('/api/task', {
    signal,
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  })
  if (!res.ok) throw new Error(`Task poll failed (${res.status})`)
  return res.json()
}

/**
 * @returns {{
 *   isRunning: boolean,
 *   taskType: string|null,
 *   percent: number,        // 0-100
 *   found: number,
 *   total: number,
 *   phase: string,
 *   statusText: string,
 *   lastUpdated: number,
 *   poll: () => void,
 * }}
 */
export function useScrapeProgress() {
  const [state, setState] = useState({
    isRunning: false,
    taskType: null,
    percent: 0,
    found: 0,
    total: 0,
    phase: '',
    statusText: '',
    lastUpdated: 0,
  })
  const timerRef = useRef(null)
  const backoffRef = useRef(0)
  const mountedRef = useRef(true)

  const poll = useCallback(async () => {
    const controller = new AbortController()
    try {
      const task = await fetchTaskState(controller.signal)
      if (!mountedRef.current) return

      backoffRef.current = 0   // reset backoff on success

      const running = Boolean(task?.running)
      const result = task?.result || {}
      const found = Number(result.found ?? result.scraped ?? 0)
      const total = Number(result.total ?? result.results ?? 0)
      const percent = total > 0 ? Math.min(100, Math.round((found / total) * 100)) : (running ? 10 : 0)

      setState({
        isRunning: running,
        taskType: task?.task_type ?? null,
        percent,
        found,
        total,
        phase: String(result.phase ?? result.status ?? (running ? 'Running…' : '')),
        statusText: String(result.status ?? task?.status ?? ''),
        lastUpdated: Date.now(),
      })
    } catch (err) {
      if (err.name === 'AbortError') return
      // Exponential backoff on network error
      backoffRef.current = Math.min(backoffRef.current * 2 || POLL_INTERVAL_MS, BACKOFF_MAX_MS)
    }
  }, [])

  useEffect(() => {
    mountedRef.current = true

    function schedule() {
      if (!mountedRef.current) return
      poll().then(() => {
        if (!mountedRef.current) return
        timerRef.current = window.setTimeout(
          schedule,
          POLL_INTERVAL_MS + backoffRef.current,
        )
      })
    }

    schedule()

    return () => {
      mountedRef.current = false
      if (timerRef.current !== null) window.clearTimeout(timerRef.current)
    }
  }, [poll])

  return { ...state, poll }
}
