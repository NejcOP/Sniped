const { handleCors } = require('./_cors')

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

function setNoStore(res) {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate')
  res.setHeader('Pragma', 'no-cache')
  res.setHeader('Expires', '0')
}

module.exports = async (req, res) => {
  if (handleCors(req, res)) return
  setNoStore(res)

  const isDev = process.env.VERCEL_ENV !== 'production'
  const devFallback = isDev ? 'http://localhost:8000' : ''
  const backendBase = normalizeBaseUrl(process.env.BACKEND_URL || process.env.VITE_API_URL || devFallback)
  if (!backendBase) {
    return res.status(503).json({ detail: 'Backend is not configured. Set BACKEND_URL in Vercel environment variables.' })
  }

  const headers = {}
  for (const [key, value] of Object.entries(req.headers || {})) {
    const lower = String(key || '').toLowerCase()
    if (lower === 'host' || lower === 'connection' || lower === 'if-none-match' || lower === 'if-modified-since') continue
    if (value === undefined) continue
    headers[key] = Array.isArray(value) ? value.join(', ') : String(value)
  }

  try {
    const upstream = await fetch(`${backendBase}/api/stats`, {
      method: req.method,
      headers,
      redirect: 'manual',
    })

    const text = await upstream.text()
    res.status(upstream.status)
    res.setHeader('Content-Type', upstream.headers.get('content-type') || 'application/json')
    return res.send(text)
  } catch (error) {
    return res.status(502).json({
      detail: `Backend request failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
    })
  }
}
