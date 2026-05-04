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

const STATIC_ALLOWED_ORIGINS = new Set([
  'https://www.sniped.io',
  'https://sniped.io',
  'https://sniped-one.vercel.app',
  'https://sniped-production.up.railway.app',
])

function isAllowedOrigin(origin) {
  const normalized = String(origin || '').trim().replace(/\/$/, '')
  if (!normalized) return false
  if (STATIC_ALLOWED_ORIGINS.has(normalized)) return true
  if (/^https:\/\/[a-zA-Z0-9-]+\.vercel\.app$/.test(normalized)) return true
  if (/^http:\/\/localhost(:\d+)?$/.test(normalized)) return true
  return false
}

function isCorsHeader(key) {
  return String(key || '').toLowerCase().startsWith('access-control-')
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

function setCorsHeaders(req, res) {
  const requestOrigin = String(req?.headers?.origin || '').trim().replace(/\/$/, '')
  const resolvedOrigin = isAllowedOrigin(requestOrigin)
    ? requestOrigin
    : 'https://www.sniped.io'
  res.setHeader('Access-Control-Allow-Origin', resolvedOrigin)
  res.setHeader('Vary', 'Origin')
  res.setHeader('Access-Control-Allow-Credentials', 'true')
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization')
}

function buildQueryString(query) {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(query || {})) {
    if (key === 'path' || key === '_path') continue
    if (Array.isArray(value)) {
      for (const item of value) params.append(key, String(item))
    } else if (value !== undefined && value !== null) {
      params.append(key, String(value))
    }
  }
  const qs = params.toString()
  return qs ? `?${qs}` : ''
}

function resolveApiPath(req) {
  const qp = req.query?.path
  if (Array.isArray(qp) && qp.length) {
    const joined = qp.map((part) => String(part || '').replace(/^\/+|\/+$/g, '')).filter(Boolean).join('/')
    return joined ? `/${joined}` : ''
  }
  if (typeof qp === 'string' && qp.trim()) {
    const cleanedQp = qp.replace(/^\/+|\/+$/g, '')
    return cleanedQp ? `/${cleanedQp}` : ''
  }

  const rawUrl = String(req.url || '')
  const pathname = rawUrl.split('?')[0] || ''
  const stripped = pathname.replace(/^\/api\/?/, '').replace(/^index\/?/, '')
  const cleaned = String(stripped || '').replace(/^\/+|\/+$/g, '')
  return cleaned ? `/${cleaned}` : ''
}

async function readRawBody(req) {
  if (req.body === undefined || req.body === null) return null
  if (Buffer.isBuffer(req.body)) return req.body
  if (typeof req.body === 'string') return req.body
  if (typeof req.body === 'object') return JSON.stringify(req.body)
  return null
}

module.exports = async (req, res) => {
  setCorsHeaders(req, res)
  if (req.method === 'OPTIONS') {
    return res.status(204).end()
  }

  // Emergency loop breaker: keep frontend stable even if upstream saved-segments
  // endpoint is intermittently unavailable.
  const apiPath = resolveApiPath(req)
  if (req.method === 'GET' && (apiPath === '/saved-segments' || apiPath.startsWith('/saved-segments/'))) {
    res.setHeader('Content-Type', 'application/json')
    return res.status(200).send('[]')
  }

  const isDev = process.env.VERCEL_ENV !== 'production'
  const devFallback = isDev ? 'http://localhost:8000' : ''
  const backendBase = normalizeBaseUrl(
    process.env.BACKEND_URL
      || process.env.RAILWAY_BACKEND_URL
      || process.env.RAILWAY_STATIC_URL
      || process.env.VITE_API_URL
      || devFallback,
  )
  if (!backendBase) {
    res.setHeader('Content-Type', 'application/json')
    return res.status(503).json({
      detail: 'Backend is not configured. Set BACKEND_URL (or RAILWAY_BACKEND_URL / RAILWAY_STATIC_URL) in Vercel environment variables.',
    })
  }

  const pathSuffix = apiPath
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
      const lower = String(key || '').toLowerCase()
      if (!HOP_BY_HOP_HEADERS.has(lower) && !isCorsHeader(lower)) {
        res.setHeader(key, value)
      }
    })
    setCorsHeaders(req, res)

    const arrayBuffer = await upstream.arrayBuffer()
    return res.send(Buffer.from(arrayBuffer))
  } catch (error) {
    res.setHeader('Content-Type', 'application/json')
    return res.status(502).json({
      target_url: targetUrl,
      detail: `Backend request failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
    })
  }
}
