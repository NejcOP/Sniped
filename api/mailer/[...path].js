// Unified mailer router — handles:
//   GET  /api/mailer/campaign-stats
//   GET  /api/mailer/sequences
//   POST /api/mailer/sequences
//   GET  /api/mailer/templates
//   POST /api/mailer/templates

function getTokenFromReq(req) {
  const auth = String(req.headers?.authorization || '')
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim()
  return String(req.query?.token || '').trim()
}

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
  const match = pathname.match(/^\/api\/mailer\/(.+)$/)
  return extractPathParts(match ? match[1] : '')
}

async function getUserId(supabaseUrl, supabaseKey, token) {
  const h = { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
  const res = await fetch(
    `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`,
    { headers: h }
  )
  if (!res.ok) return null
  const users = await res.json()
  return Array.isArray(users) && users.length ? users[0].id : null
}

async function getKvList(supabaseUrl, supabaseKey, key) {
  const h = { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
  const res = await fetch(
    `${supabaseUrl}/rest/v1/runtime_kv?key=eq.${encodeURIComponent(key)}&select=value&limit=1`,
    { headers: h }
  )
  if (!res.ok) return []
  const rows = await res.json()
  if (!Array.isArray(rows) || !rows.length) return []
  try { return JSON.parse(rows[0].value) } catch { return [] }
}

async function setKvList(supabaseUrl, supabaseKey, key, items) {
  const h = {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
    Prefer: 'resolution=merge-duplicates',
  }
  await fetch(`${supabaseUrl}/rest/v1/runtime_kv`, {
    method: 'POST',
    headers: h,
    body: JSON.stringify({ key, value: JSON.stringify(items), updated_at: new Date().toISOString() }),
  })
}

const EMPTY_STATS = {
  sent: 0, opened: 0, replied: 0, bounced: 0, opens_total: 0,
  open_rate: 0, reply_rate: 0, bounce_rate: 0,
  ab_breakdown: { A: 0, B: 0 },
  sequences: [], saved_templates: [], recent_events: [],
}

async function handleCampaignStats(supabaseUrl, supabaseKey, userId) {
  const h = { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
  try {
    const lRes = await fetch(
      `${supabaseUrl}/rest/v1/leads?user_id=eq.${encodeURIComponent(userId)}&select=sent_at,open_count,status,reply_detected_at,bounced_at&limit=5000`,
      { headers: h }
    )
    let leads = lRes.ok ? await lRes.json() : []
    if (!Array.isArray(leads)) leads = []

    const sent = leads.filter(l => l.sent_at).length
    const opened = leads.filter(l => Number(l.open_count || 0) > 0).length
    const opens_total = leads.reduce((s, l) => s + Number(l.open_count || 0), 0)
    const replied = leads.filter(l => {
      const st = String(l.status || '').toLowerCase()
      return l.reply_detected_at || ['replied', 'interested', 'meeting set'].includes(st)
    }).length
    const bounced = leads.filter(l => {
      const st = String(l.status || '').toLowerCase()
      return l.bounced_at || ['bounced', 'invalid_email'].includes(st)
    }).length

    const sequences = await getKvList(supabaseUrl, supabaseKey, `mailer_sequences:${userId}`)
    const saved_templates = await getKvList(supabaseUrl, supabaseKey, `mailer_templates:${userId}`)

    return {
      sent, opened, replied, bounced, opens_total,
      open_rate: sent ? Math.round((opened / sent) * 1000) / 10 : 0,
      reply_rate: sent ? Math.round((replied / sent) * 1000) / 10 : 0,
      bounce_rate: sent ? Math.round((bounced / sent) * 1000) / 10 : 0,
      ab_breakdown: { A: 0, B: 0 },
      sequences, saved_templates, recent_events: [],
    }
  } catch { return EMPTY_STATS }
}

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json')

  const supabaseUrl = process.env.SUPABASE_URL || ''
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || ''
  const token = getTokenFromReq(req)

  // Determine sub-path
  const pathParts = resolveCatchAllPathParts(req)
  const subPath = pathParts.join('/')

  if (!token || !supabaseUrl || !supabaseKey) {
    if (subPath === 'campaign-stats') return res.status(200).json(EMPTY_STATS)
    return res.status(401).json({ detail: 'Unauthorized' })
  }

  const userId = await getUserId(supabaseUrl, supabaseKey, token)
  if (!userId) {
    if (subPath === 'campaign-stats') return res.status(200).json(EMPTY_STATS)
    return res.status(401).json({ detail: 'Unauthorized' })
  }

  // Route
  if (subPath === 'campaign-stats' && req.method === 'GET') {
    return res.status(200).json(await handleCampaignStats(supabaseUrl, supabaseKey, userId))
  }

  if (subPath === 'sequences') {
    const kvKey = `mailer_sequences:${userId}`
    if (req.method === 'GET') {
      const items = await getKvList(supabaseUrl, supabaseKey, kvKey)
      return res.status(200).json({ items })
    }
    if (req.method === 'POST') {
      const items = await getKvList(supabaseUrl, supabaseKey, kvKey)
      const newItem = { id: `seq_${Date.now()}`, created_at: new Date().toISOString(), ...(req.body || {}) }
      items.push(newItem)
      await setKvList(supabaseUrl, supabaseKey, kvKey, items)
      return res.status(200).json({ status: 'created', item: newItem })
    }
  }

  if (subPath === 'templates') {
    const kvKey = `mailer_templates:${userId}`
    if (req.method === 'GET') {
      const items = await getKvList(supabaseUrl, supabaseKey, kvKey)
      return res.status(200).json({ items })
    }
    if (req.method === 'POST') {
      const items = await getKvList(supabaseUrl, supabaseKey, kvKey)
      const newItem = { id: `tpl_${Date.now()}`, created_at: new Date().toISOString(), ...(req.body || {}) }
      items.push(newItem)
      await setKvList(supabaseUrl, supabaseKey, kvKey, items)
      return res.status(200).json({ status: 'created', item: newItem })
    }
  }

  // Fallback: proxy remaining paths (send, stop, preview, cold-outreach, etc.) to Python backend
  const isDev = process.env.VERCEL_ENV !== 'production'
  const backendBase = normalizeBaseUrl(process.env.BACKEND_URL || process.env.VITE_API_URL || (isDev ? 'http://localhost:8000' : ''))
  if (!backendBase) {
    return res.status(503).json({ detail: 'Backend is not configured. Set BACKEND_URL in Vercel environment variables.' })
  }

  const HOP_BY_HOP = new Set(['connection','keep-alive','proxy-authenticate','proxy-authorization','te','trailers','transfer-encoding','upgrade','host'])
  const qs = new URLSearchParams()
  for (const [k, v] of Object.entries(req.query || {})) {
    if (k === 'path' || k === '_path') continue
    if (Array.isArray(v)) v.forEach(i => qs.append(k, i))
    else if (v != null) qs.append(k, String(v))
  }
  const qStr = qs.toString() ? `?${qs.toString()}` : ''
  const targetUrl = `${backendBase}/api/mailer/${subPath}${qStr}`

  const headers = {}
  for (const [k, v] of Object.entries(req.headers || {})) {
    if (!HOP_BY_HOP.has(String(k).toLowerCase()) && v != null) {
      headers[k] = Array.isArray(v) ? v.join(', ') : String(v)
    }
  }

  try {
    let body
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      body = req.body == null ? undefined : (Buffer.isBuffer(req.body) ? req.body : (typeof req.body === 'string' ? req.body : JSON.stringify(req.body)))
    }
    const upstream = await fetch(targetUrl, { method: req.method, headers, body, redirect: 'manual' })
    res.status(upstream.status)
    upstream.headers.forEach((v, k) => { if (!HOP_BY_HOP.has(k.toLowerCase())) res.setHeader(k, v) })
    return res.send(Buffer.from(await upstream.arrayBuffer()))
  } catch (err) {
    return res.status(502).json({ detail: `Backend request failed: ${err instanceof Error ? err.message : 'Unknown error'}` })
  }
}
