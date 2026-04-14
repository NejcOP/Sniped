// Native Vercel function — GET+POST /api/mailer/templates
// Stores saved mail templates in Supabase runtime_kv.

function getTokenFromReq(req) {
  const auth = String(req.headers?.authorization || '')
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim()
  return String(req.query?.token || '').trim()
}

async function getUserId(supabaseUrl, supabaseKey, token) {
  const h = { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
  const res = await fetch(`${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`, { headers: h })
  if (!res.ok) return null
  const users = await res.json()
  return Array.isArray(users) && users.length ? users[0].id : null
}

async function getList(supabaseUrl, supabaseKey, key) {
  const h = { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }
  const res = await fetch(`${supabaseUrl}/rest/v1/runtime_kv?key=eq.${encodeURIComponent(key)}&select=value&limit=1`, { headers: h })
  if (!res.ok) return []
  const rows = await res.json()
  if (!Array.isArray(rows) || !rows.length) return []
  try { return JSON.parse(rows[0].value) } catch { return [] }
}

async function setList(supabaseUrl, supabaseKey, key, items) {
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

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json')
  const supabaseUrl = process.env.SUPABASE_URL || ''
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || ''
  const token = getTokenFromReq(req)
  if (!token || !supabaseUrl || !supabaseKey) return res.status(401).json({ detail: 'Unauthorized' })

  const userId = await getUserId(supabaseUrl, supabaseKey, token)
  if (!userId) return res.status(401).json({ detail: 'Unauthorized' })

  const kvKey = `mailer_templates:${userId}`

  if (req.method === 'GET') {
    const items = await getList(supabaseUrl, supabaseKey, kvKey)
    return res.status(200).json({ items })
  }

  if (req.method === 'POST') {
    const body = req.body || {}
    const items = await getList(supabaseUrl, supabaseKey, kvKey)
    const newItem = {
      id: `tpl_${Date.now()}`,
      created_at: new Date().toISOString(),
      ...body,
    }
    items.push(newItem)
    await setList(supabaseUrl, supabaseKey, kvKey, items)
    return res.status(200).json({ status: 'created', item: newItem })
  }

  return res.status(405).json({ detail: 'Method not allowed' })
}
