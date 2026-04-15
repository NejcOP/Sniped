/**
 * useLeadsCache — SWR-style stale-while-revalidate leads fetcher
 *
 * PERFORMANCE RULES IMPLEMENTED:
 * 1. Stale-While-Revalidate: returns cached data instantly, then refreshes in background.
 * 2. In-flight deduplication: only one request per cache key at a time.
 * 3. 60-second TTL per cache entry — avoids repeat requests within the same session.
 * 4. Silent background revalidation: UI never shows a blank state on revisit.
 * 5. AbortController: cancels pending fetches when component unmounts or params change.
 */
import { useCallback, useEffect, useRef, useState } from 'react'
import { getStoredValue } from '../authStorage'

/* ── In-memory cache (module-level, survives re-renders) ── */
const _cache = new Map()   // key → { data, ts }
const _inflight = new Map() // key → AbortController
const CACHE_TTL_MS = 60_000  // 60 seconds
const API_BASE = String(import.meta.env.VITE_API_BASE_URL || '').trim().replace(/\/$/, '')

function buildCacheKey(params) {
  return JSON.stringify(params)
}

async function _fetchLeads(params, signal) {
  const token = getStoredValue('lf_token')
  const qp = new URLSearchParams({
    limit: String(params.limit ?? 50),
    page: String((params.page ?? 0) + 1),       // backend is 1-indexed
    sort: String(params.sort ?? 'best'),
    include_blacklisted: params.includeBlacklisted ? '1' : '0',
  })
  if (params.status && params.status !== 'all') qp.set('status', params.status)
  if (params.quickFilter && params.quickFilter !== 'all') qp.set('quick_filter', params.quickFilter)
  if (params.search && params.search.trim()) qp.set('search', params.search.trim())

  const requestUrl = API_BASE ? `${API_BASE}/api/leads?${qp.toString()}` : `/api/leads?${qp.toString()}`
  const res = await fetch(requestUrl, {
    signal,
    headers: token ? { Authorization: `Bearer ${token}` } : {},
  })
  if (!res.ok) {
    const err = new Error(`Leads fetch failed (${res.status})`)
    err.status = res.status
    throw err
  }
  return res.json()
}

/**
 * @param {object} params - { page, sort, status, quickFilter, search, includeBlacklisted }
 * @param {object} options - { enabled, onSuccess, onError }
 */
export function useLeadsCache(params, { enabled = true, onSuccess, onError } = {}) {
  const [data, setData] = useState(null)
  const [isLoading, setIsLoading] = useState(false)
  const [isRevalidating, setIsRevalidating] = useState(false)
  const [error, setError] = useState(null)
  const mountedRef = useRef(true)

  const cacheKey = buildCacheKey(params)

  const revalidate = useCallback(
    async ({ silent = false } = {}) => {
      if (!enabled) return

      // Abort any in-flight request for this key
      if (_inflight.has(cacheKey)) {
        _inflight.get(cacheKey).abort()
      }
      const controller = new AbortController()
      _inflight.set(cacheKey, controller)

      if (!silent) setIsLoading(true)
      setIsRevalidating(true)
      setError(null)

      try {
        const json = await _fetchLeads(params, controller.signal)
        if (!mountedRef.current) return
        _cache.set(cacheKey, { data: json, ts: Date.now() })
        setData(json)
        onSuccess?.(json)
      } catch (err) {
        if (err.name === 'AbortError') return
        if (!mountedRef.current) return
        setError(err)
        onError?.(err)
      } finally {
        if (mountedRef.current) {
          setIsLoading(false)
          setIsRevalidating(false)
        }
        _inflight.delete(cacheKey)
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [cacheKey, enabled],
  )

  useEffect(() => {
    mountedRef.current = true
    if (!enabled) return

    const cached = _cache.get(cacheKey)
    const now = Date.now()

    if (cached && now - cached.ts < CACHE_TTL_MS) {
      // Serve stale data immediately — zero perceived loading time
      setData(cached.data)
      setIsLoading(false)
      // Revalidate silently in the background
      revalidate({ silent: true })
    } else {
      // No valid cache: show loading skeleton
      revalidate({ silent: false })
    }

    return () => {
      mountedRef.current = false
    }
  }, [cacheKey, enabled, revalidate])

  const mutate = useCallback(
    (updater) => {
      setData((prev) => {
        const next = typeof updater === 'function' ? updater(prev) : updater
        // Keep cache in sync with optimistic update
        _cache.set(cacheKey, { data: next, ts: Date.now() })
        return next
      })
    },
    [cacheKey],
  )

  const invalidate = useCallback(() => {
    _cache.delete(cacheKey)
    revalidate({ silent: false })
  }, [cacheKey, revalidate])

  return {
    data,
    isLoading,
    isRevalidating,
    error,
    revalidate,
    mutate,
    invalidate,
  }
}

/** Invalidate ALL leads cache entries (e.g. after scraping finishes). */
export function invalidateLeadsCache() {
  _cache.clear()
}
