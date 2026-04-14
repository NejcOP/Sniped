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
  return raw.replace(/\/$/, '')
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

async function readRawBody(req) {
  if (req.body === undefined || req.body === null) return null
  if (Buffer.isBuffer(req.body)) return req.body
  if (typeof req.body === 'string') return req.body
  if (typeof req.body === 'object') return JSON.stringify(req.body)
  return null
}

module.exports = async (req, res) => {
  // In non-production environments fall back to local Python backend so dev works without env vars.
  const isDev = process.env.VERCEL_ENV !== 'production'
  const devFallback = isDev ? 'http://localhost:8000' : ''
  const backendBase = normalizeBaseUrl(process.env.BACKEND_URL || process.env.VITE_API_URL || devFallback)
  if (!backendBase) {
    return res.status(503).json({
      detail: 'Backend is not configured. Set BACKEND_URL in Vercel environment variables.',
    })
  }

  const pathParts = extractPathParts(req.query?.path)
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
