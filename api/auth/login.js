const crypto = require('crypto')

function getSupabaseUrl() {
  return process.env.SUPABASE_URL || ''
}

function getSupabaseKey() {
  return process.env.SUPABASE_SERVICE_ROLE_KEY || ''
}

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 260000, 32, 'sha256').toString('hex')
}

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json')

  if (req.method !== 'POST') {
    return res.status(405).json({ detail: 'Method not allowed' })
  }

  const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {})
  const { email: rawEmail, password } = body

  if (!rawEmail || !password) {
    return res.status(400).json({ detail: 'Email and password are required.' })
  }

  const email = String(rawEmail).trim().toLowerCase()

  const supabaseUrl = getSupabaseUrl()
  const supabaseKey = getSupabaseKey()

  if (!supabaseUrl || !supabaseKey) {
    return res.status(503).json({ detail: 'Database not configured.' })
  }

  const headers = {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
  }

  // Fetch user
  const userRes = await fetch(
    `${supabaseUrl}/rest/v1/users?email=eq.${encodeURIComponent(email)}&select=id,password_hash,salt,niche,token,display_name,contact_name,account_type&limit=1`,
    { headers }
  )

  if (!userRes.ok) {
    return res.status(502).json({ detail: 'Login query failed.' })
  }

  const rows = await userRes.json()
  if (!Array.isArray(rows) || rows.length === 0) {
    return res.status(401).json({ detail: 'Invalid email or password.' })
  }

  const row = rows[0]
  const expected = hashPassword(String(password), String(row.salt || ''))

  // Constant-time compare
  const expectedBuf = Buffer.from(expected, 'hex')
  const actualBuf = Buffer.from(String(row.password_hash || ''), 'hex')
  const match = expectedBuf.length === actualBuf.length &&
    crypto.timingSafeEqual(expectedBuf, actualBuf)

  if (!match) {
    return res.status(401).json({ detail: 'Invalid email or password.' })
  }

  const token = row.token || crypto.randomUUID()

  // Update token
  await fetch(
    `${supabaseUrl}/rest/v1/users?id=eq.${encodeURIComponent(row.id)}`,
    {
      method: 'PATCH',
      headers: { ...headers, Prefer: 'return=minimal' },
      body: JSON.stringify({ token }),
    }
  )

  return res.status(200).json({
    token,
    niche: row.niche || '',
    email,
    display_name: row.display_name || '',
    contact_name: row.contact_name || '',
    account_type: row.account_type || 'entrepreneur',
  })
}
