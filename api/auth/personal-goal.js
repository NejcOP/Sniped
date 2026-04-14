const DEFAULT_GOAL = { name: 'My Goal', amount: 5000, currency: 'EUR' }

function getSupabaseUrl() { return process.env.SUPABASE_URL || '' }
function getSupabaseKey() { return process.env.SUPABASE_SERVICE_ROLE_KEY || '' }
function getToken(req) {
  const auth = String(req.headers.authorization || '')
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim()
  return ''
}

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json')

  if (req.method === 'PUT') return handleUpdate(req, res)
  if (req.method !== 'GET') return res.status(405).json({ detail: 'Method not allowed' })

  const token = getToken(req)
  if (!token) return res.status(200).json({ ...DEFAULT_GOAL, source: 'default' })

  const supabaseUrl = getSupabaseUrl()
  const supabaseKey = getSupabaseKey()
  if (!supabaseUrl || !supabaseKey) return res.status(200).json({ ...DEFAULT_GOAL, source: 'default' })

  try {
    const dbHeaders = { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }

    // Get user id
    const userRes = await fetch(
      `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`,
      { headers: dbHeaders }
    )
    if (!userRes.ok) return res.status(200).json({ ...DEFAULT_GOAL, source: 'default' })
    const users = await userRes.json()
    if (!Array.isArray(users) || users.length === 0) return res.status(200).json({ ...DEFAULT_GOAL, source: 'default' })

    const userId = users[0].id
    const key = `personal_goal:${userId}`

    // Read from runtime_kv table
    const kvRes = await fetch(
      `${supabaseUrl}/rest/v1/runtime_kv?key=eq.${encodeURIComponent(key)}&select=value&limit=1`,
      { headers: dbHeaders }
    )
    if (kvRes.ok) {
      const rows = await kvRes.json()
      if (Array.isArray(rows) && rows.length > 0) {
        try {
          const parsed = JSON.parse(rows[0].value)
          return res.status(200).json({
            name: parsed.name || 'My Goal',
            amount: Number(parsed.amount || DEFAULT_GOAL.amount),
            currency: parsed.currency || 'EUR',
            source: 'runtime',
          })
        } catch { /* fall through to default */ }
      }
    }
  } catch { /* fall through */ }

  return res.status(200).json({ ...DEFAULT_GOAL, source: 'default' })
}

async function handleUpdate(req, res) {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {})
  const token = getToken(req) || body.token || ''
  if (!token) return res.status(401).json({ detail: 'Invalid or expired session token.' })

  const name = String(body.name || 'My Goal').trim()
  const amount = Number(body.amount || 0)
  const currency = String(body.currency || 'EUR').toUpperCase()

  if (amount <= 0) return res.status(400).json({ detail: 'Goal amount must be greater than 0.' })

  const supabaseUrl = getSupabaseUrl()
  const supabaseKey = getSupabaseKey()
  if (!supabaseUrl || !supabaseKey) return res.status(503).json({ detail: 'Database not configured.' })

  const dbHeaders = {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
  }

  const userRes = await fetch(
    `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`,
    { headers: dbHeaders }
  )
  if (!userRes.ok) return res.status(502).json({ detail: 'User lookup failed.' })
  const users = await userRes.json()
  if (!Array.isArray(users) || users.length === 0) return res.status(401).json({ detail: 'Invalid session.' })

  const userId = users[0].id
  const key = `personal_goal:${userId}`
  const value = JSON.stringify({ name, amount, currency, updated_at: new Date().toISOString() })

  // Upsert into runtime_kv
  await fetch(`${supabaseUrl}/rest/v1/runtime_kv`, {
    method: 'POST',
    headers: { ...dbHeaders, Prefer: 'resolution=merge-duplicates' },
    body: JSON.stringify({ key, value }),
  })

  return res.status(200).json({ name, amount, currency })
}
