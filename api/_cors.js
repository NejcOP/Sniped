// Shared CORS helper — import in every Vercel API function.
// Ensures error responses (4xx/5xx) include CORS headers so the browser
// can read the response body instead of showing "Failed to fetch".

const ALLOWED_ORIGINS = new Set([
  'https://sniped-one.vercel.app',
])

function setCorsHeaders(req, res) {
  const origin = String(req.headers?.origin || '').trim()
  const allowed =
    ALLOWED_ORIGINS.has(origin) ||
    /^https?:\/\/[a-zA-Z0-9-]+(\.vercel\.app)$/.test(origin) ||
    /^http:\/\/localhost(:\d+)?$/.test(origin)

  res.setHeader('Access-Control-Allow-Origin', allowed ? origin : 'https://sniped-one.vercel.app')
  res.setHeader('Access-Control-Allow-Credentials', 'true')
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS')
  res.setHeader('Access-Control-Allow-Headers', 'Authorization,Content-Type')
  res.setHeader('Vary', 'Origin')
}

function handleCors(req, res) {
  setCorsHeaders(req, res)
  if (req.method === 'OPTIONS') {
    res.status(204).end()
    return true // caller should return immediately
  }
  return false
}

module.exports = { setCorsHeaders, handleCors }
