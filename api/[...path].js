const HOP_BY_HOP_HEADERS = new Set([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'te',
  'trailers',
  'transfer-encoding',
  'upgrade',
  'host',
])

function normalizeBaseUrl(value) {
  const raw = String(value || '').trim()
  if (!raw) return ''
  const withScheme = /^[a-zA-Z][a-zA-Z\d+.-]*:\/\//.test(raw)
    ? raw
    : /^(localhost|127\.0\.0\.1|0\.0\.0\.0)(:\d+)?(\/|$)/.test(raw)
      ? `http://${raw}`
      : `https://${raw}`
  return withScheme.replace(/\/$/, '')
}

function buildQueryString(query) {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(query || {})) {
    if (key === 'path') continue
    if (Array.isArray(value)) {
      for (const item of value) params.append(key, String(item))
    } else if (value !== undefined && value !== null) {
      params.append(key, String(value))
    }
  }
  const qs = params.toString()
  return qs ? `?${qs}` : ''
}

function extractPathParts(pathValue) {
  if (Array.isArray(pathValue)) return pathValue.map((part) => String(part).replace(/^\/+|\/+$/g, '')).filter(Boolean)
  const single = String(pathValue || '').replace(/^\/+|\/+$/g, '')
  return single ? [single] : []
}

function resolveCatchAllPathParts(req) {
  const direct = extractPathParts(req.query?.path || req.query?._path)
  if (direct.length) return direct

  const rawUrl = String(req.url || '')
  const pathname = rawUrl.split('?')[0] || ''
  const match = pathname.match(/^\/api\/(.+)$/)
  return extractPathParts(match ? match[1] : '')
}

async function readRawBody(req) {
  if (req.body === undefined || req.body === null) return null
  if (Buffer.isBuffer(req.body)) return req.body
  if (typeof req.body === 'string') return req.body
  if (typeof req.body === 'object') return JSON.stringify(req.body)
  return null
}

// ── Supabase helpers ────────────────────────────────────────────────────────
function getTokenFromCatchAll(req) {
  const auth = String(req.headers?.authorization || '')
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim()
  return String(req.query?.token || '').trim()
}

async function resolveUserId(supabaseUrl, supabaseKey, token) {
  const r = await fetch(
    `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`,
    { headers: { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` } },
  )
  if (!r.ok) return null
  const rows = await r.json()
  return Array.isArray(rows) && rows.length ? rows[0].id : null
}

function dbHeaders(supabaseKey) {
  return { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}`, 'Content-Type': 'application/json', Prefer: 'return=representation' }
}

// ── Native route: /api/saved-segments ───────────────────────────────────────
async function handleSavedSegments(req, res, supabaseUrl, supabaseKey) {
  const token = getTokenFromCatchAll(req)
  if (!token) return res.status(401).json({ detail: 'Unauthorized' })
  const userId = await resolveUserId(supabaseUrl, supabaseKey, token)
  if (!userId) return res.status(401).json({ detail: 'Unauthorized' })

  const hdrs = dbHeaders(supabaseKey)

  if (req.method === 'GET') {
    const r = await fetch(
      `${supabaseUrl}/rest/v1/SavedSegments?user_id=eq.${encodeURIComponent(userId)}&select=id,user_id,name,filters_json,created_at,updated_at&order=updated_at.desc&limit=100`,
      { headers: hdrs },
    )
    const rows = r.ok ? await r.json() : []
    return res.status(200).json({ items: rows })
  }

  if (req.method === 'POST') {
    const body = typeof req.body === 'object' && req.body !== null ? req.body : {}
    const name = String(body.name || '').trim()
    if (!name) return res.status(400).json({ detail: 'name is required' })
    const filtersJson = JSON.stringify(body.filters || {})
    const now = new Date().toISOString()
    // Check for existing
    const chk = await fetch(
      `${supabaseUrl}/rest/v1/SavedSegments?user_id=eq.${encodeURIComponent(userId)}&name=eq.${encodeURIComponent(name)}&select=id&limit=1`,
      { headers: hdrs },
    )
    const existing = chk.ok ? await chk.json() : []
    let saved
    if (Array.isArray(existing) && existing.length) {
      const segId = existing[0].id
      const upd = await fetch(
        `${supabaseUrl}/rest/v1/SavedSegments?id=eq.${segId}&user_id=eq.${encodeURIComponent(userId)}`,
        { method: 'PATCH', headers: hdrs, body: JSON.stringify({ filters_json: filtersJson, updated_at: now }) },
      )
      const updRows = upd.ok ? await upd.json() : []
      saved = Array.isArray(updRows) && updRows.length ? updRows[0] : null
    } else {
      const ins = await fetch(
        `${supabaseUrl}/rest/v1/SavedSegments`,
        { method: 'POST', headers: hdrs, body: JSON.stringify({ user_id: userId, name, filters_json: filtersJson, created_at: now, updated_at: now }) },
      )
      const insRows = ins.ok ? await ins.json() : []
      saved = Array.isArray(insRows) && insRows.length ? insRows[0] : null
    }
    if (!saved) return res.status(500).json({ detail: 'Could not save segment' })
    return res.status(200).json(saved)
  }

  return res.status(405).json({ detail: 'Method not allowed' })
}

// ── Native route: /api/saved-segments/:id (DELETE) ──────────────────────────
async function handleSavedSegmentsDelete(req, res, supabaseUrl, supabaseKey, segmentId) {
  const token = getTokenFromCatchAll(req)
  if (!token) return res.status(401).json({ detail: 'Unauthorized' })
  const userId = await resolveUserId(supabaseUrl, supabaseKey, token)
  if (!userId) return res.status(401).json({ detail: 'Unauthorized' })

  const hdrs = dbHeaders(supabaseKey)
  const chk = await fetch(
    `${supabaseUrl}/rest/v1/SavedSegments?id=eq.${encodeURIComponent(segmentId)}&user_id=eq.${encodeURIComponent(userId)}&select=id&limit=1`,
    { headers: hdrs },
  )
  const rows = chk.ok ? await chk.json() : []
  if (!Array.isArray(rows) || !rows.length) return res.status(404).json({ detail: 'Not found' })
  await fetch(
    `${supabaseUrl}/rest/v1/SavedSegments?id=eq.${encodeURIComponent(segmentId)}&user_id=eq.${encodeURIComponent(userId)}`,
    { method: 'DELETE', headers: hdrs },
  )
  return res.status(200).json({ status: 'deleted', id: segmentId })
}

// ── Native route: /api/blacklist ─────────────────────────────────────────────
async function handleBlacklist(req, res, supabaseUrl, supabaseKey) {
  const token = getTokenFromCatchAll(req)
  if (!token) return res.status(401).json({ detail: 'Unauthorized' })
  const userId = await resolveUserId(supabaseUrl, supabaseKey, token)
  if (!userId) return res.status(401).json({ detail: 'Unauthorized' })

  const hdrs = dbHeaders(supabaseKey)

  if (req.method === 'GET') {
    const r = await fetch(
      `${supabaseUrl}/rest/v1/lead_blacklist?select=id,kind,value,reason,created_at&order=created_at.desc&limit=200`,
      { headers: hdrs },
    )
    const rows = r.ok ? await r.json() : []
    return res.status(200).json({ items: rows, count: rows.length })
  }

  if (req.method === 'POST') {
    const body = typeof req.body === 'object' && req.body !== null ? req.body : {}
    const kind = String(body.kind || '').trim()
    const value = String(body.value || '').trim()
    const reason = String(body.reason || 'Manual blacklist').trim()
    if (!kind || !value) return res.status(400).json({ detail: 'kind and value are required' })
    const now = new Date().toISOString()
    const ins = await fetch(
      `${supabaseUrl}/rest/v1/lead_blacklist`,
      { method: 'POST', headers: hdrs, body: JSON.stringify({ kind, value, reason, created_at: now }) },
    )
    const rows = ins.ok ? await ins.json() : []
    const created = Array.isArray(rows) && rows.length ? rows[0] : { kind, value, reason, created_at: now }
    return res.status(200).json({ status: 'added', item: created })
  }

  if (req.method === 'DELETE') {
    const kind = String(req.query?.kind || '').trim()
    const value = String(req.query?.value || '').trim()
    if (!kind || !value) return res.status(400).json({ detail: 'kind and value query params required' })
    await fetch(
      `${supabaseUrl}/rest/v1/lead_blacklist?kind=eq.${encodeURIComponent(kind)}&value=eq.${encodeURIComponent(value)}`,
      { method: 'DELETE', headers: hdrs },
    )
    return res.status(200).json({ status: 'removed' })
  }

  return res.status(405).json({ detail: 'Method not allowed' })
}

// ── Main handler ─────────────────────────────────────────────────────────────
module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json')

  const pathParts = resolveCatchAllPathParts(req)
  const nativePath = pathParts.join('/')

  const supabaseUrl = (process.env.SUPABASE_URL || '').replace(/\/$/, '')
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || ''

  // saved-segments
  if (nativePath === 'saved-segments') {
    if (!supabaseUrl || !supabaseKey) return res.status(503).json({ detail: 'Database not configured' })
    return handleSavedSegments(req, res, supabaseUrl, supabaseKey)
  }
  // saved-segments/:id DELETE
  if (pathParts[0] === 'saved-segments' && pathParts.length === 2 && req.method === 'DELETE') {
    if (!supabaseUrl || !supabaseKey) return res.status(503).json({ detail: 'Database not configured' })
    return handleSavedSegmentsDelete(req, res, supabaseUrl, supabaseKey, pathParts[1])
  }
  // blacklist
  if (nativePath === 'blacklist') {
    if (!supabaseUrl || !supabaseKey) return res.status(503).json({ detail: 'Database not configured' })
    return handleBlacklist(req, res, supabaseUrl, supabaseKey)
  }

  // In non-production environments fall back to local Python backend so dev works without env vars.
  const isDev = process.env.VERCEL_ENV !== 'production'
  const devFallback = isDev ? 'http://localhost:8000' : ''
  const backendBase = normalizeBaseUrl(process.env.BACKEND_URL || process.env.VITE_API_URL || devFallback)
  if (!backendBase) {
    return res.status(503).json({
      detail: 'Backend is not configured. Set BACKEND_URL in Vercel environment variables.',
    })
  }

  const pathSuffix = pathParts.length ? `/${pathParts.join('/')}` : ''
  const queryString = buildQueryString(req.query)
  const targetUrl = `${backendBase}/api${pathSuffix}${queryString}`

  const headers = {}
  for (const [key, value] of Object.entries(req.headers || {})) {
    const lower = String(key || '').toLowerCase()
    if (HOP_BY_HOP_HEADERS.has(lower)) continue
    if (value === undefined) continue
    headers[key] = Array.isArray(value) ? value.join(', ') : String(value)
  }

  try {
    const body = req.method === 'GET' || req.method === 'HEAD' ? undefined : await readRawBody(req)
    const upstream = await fetch(targetUrl, {
      method: req.method,
      headers,
      body,
      redirect: 'manual',
    })

    res.status(upstream.status)
    upstream.headers.forEach((value, key) => {
      if (!HOP_BY_HOP_HEADERS.has(String(key || '').toLowerCase())) {
        res.setHeader(key, value)
      }
    })

    const arrayBuffer = await upstream.arrayBuffer()
    return res.send(Buffer.from(arrayBuffer))
  } catch (error) {
    return res.status(502).json({
      detail: `Backend request failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
    })
  }
}
