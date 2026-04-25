// Vercel serverless function — DB Audit
// Proxies to the Railway backend /api/db-audit endpoint so the user can call
// https://sniped-one.vercel.app/api/db-audit from the browser or Postman and
// get a live count of every row in the Supabase leads table.
// Also provides a direct Supabase count as a backup when Railway is unreachable.

const { handleCors } = require('./_cors')

function normalizeBaseUrl(value) {
  const raw = String(value || '').trim()
  if (!raw) return ''
  return /^[a-zA-Z][a-zA-Z\d+.-]*:\/\//.test(raw) ? raw.replace(/\/$/, '') : `https://${raw}`.replace(/\/$/, '')
}

module.exports = async (req, res) => {
  if (handleCors(req, res)) return
  res.setHeader('Content-Type', 'application/json')

  // ── 1. Try Railway backend first ──────────────────────────────────────────
  const railwayBase = normalizeBaseUrl(process.env.RAILWAY_BACKEND_URL || process.env.NEXT_PUBLIC_API_BASE_URL || '')
  if (railwayBase) {
    try {
      const authHeader = req.headers?.authorization || ''
      const upstream = await fetch(`${railwayBase}/api/db-audit`, {
        headers: { Authorization: authHeader, 'Content-Type': 'application/json' },
        signal: AbortSignal.timeout(10000),
      })
      const body = await upstream.json().catch(() => ({}))
      return res.status(upstream.status).json({ source: 'railway', ...body })
    } catch (railwayErr) {
      // Fall through to direct Supabase probe below.
    }
  }

  // ── 2. Direct Supabase count as fallback ──────────────────────────────────
  const supabaseUrl = process.env.SUPABASE_URL || ''
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || ''

  if (!supabaseUrl || !supabaseKey) {
    return res.status(200).json({
      source: 'vercel-direct',
      total_rows: null,
      rows_per_user: null,
      error: 'SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY not set in Vercel env vars.',
    })
  }

  const dbHeaders = {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
    Prefer: 'count=exact',
  }

  try {
    // Total row count
    const totalRes = await fetch(`${supabaseUrl}/rest/v1/leads?select=user_id`, { headers: dbHeaders })
    const rows = await totalRes.json().catch(() => [])
    const total = Array.isArray(rows) ? rows.length : 0

    // Per-user breakdown
    const userCounts = {}
    if (Array.isArray(rows)) {
      for (const r of rows) {
        const uid = String(r.user_id || '')
        userCounts[uid] = (userCounts[uid] || 0) + 1
      }
    }

    // Sort descending by count, take top 20
    const sorted = Object.entries(userCounts)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20)
    const rows_per_user = Object.fromEntries(sorted)

    return res.status(200).json({
      source: 'vercel-direct',
      total_rows: total,
      rows_per_user,
    })
  } catch (err) {
    return res.status(500).json({ source: 'vercel-direct', error: String(err?.message || err) })
  }
}
