const crypto = require('crypto')
const fs = require('fs')
const path = require('path')

let _config = null
function getConfig() {
  if (_config) return _config
  try {
    _config = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'config.json'), 'utf8'))
  } catch {
    _config = {}
  }
  return _config
}

function getSupabaseUrl() {
  return process.env.SUPABASE_URL || getConfig()?.supabase?.url || ''
}

function getSupabaseKey() {
  return process.env.SUPABASE_SERVICE_ROLE_KEY || getConfig()?.supabase?.service_role_key || ''
}

function hashPassword(password, salt) {
  // Identical to Python: hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260_000).hex()
  return crypto.pbkdf2Sync(password, salt, 260000, 32, 'sha256').toString('hex')
}

function utcNowIso() {
  return new Date().toISOString()
}

const FREE_QUOTA = 50

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json')

  if (req.method !== 'POST') {
    return res.status(405).json({ detail: 'Method not allowed' })
  }

  const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {})
  const { email: rawEmail, password, niche, account_type, display_name, contact_name } = body

  if (!rawEmail || !password || !niche) {
    return res.status(400).json({ detail: 'email, password and niche are required.' })
  }

  const email = String(rawEmail).trim().toLowerCase()
  if (!email.includes('@')) {
    return res.status(400).json({ detail: 'Invalid email address.' })
  }
  if (String(password).length < 8) {
    return res.status(400).json({ detail: 'Password must be at least 8 characters.' })
  }

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

  // Check if user already exists
  const checkRes = await fetch(
    `${supabaseUrl}/rest/v1/users?email=eq.${encodeURIComponent(email)}&select=id&limit=1`,
    { headers }
  )
  const existing = await checkRes.json()
  if (Array.isArray(existing) && existing.length > 0) {
    return res.status(409).json({ detail: 'An account with this email already exists.' })
  }

  const salt = crypto.randomBytes(32).toString('hex')
  const password_hash = hashPassword(String(password), salt)
  const token = crypto.randomUUID()
  const now = utcNowIso()

  const insertRes = await fetch(`${supabaseUrl}/rest/v1/users`, {
    method: 'POST',
    headers: { ...headers, Prefer: 'return=representation' },
    body: JSON.stringify({
      email,
      password_hash,
      salt,
      niche,
      account_type: (account_type || 'entrepreneur').toLowerCase(),
      display_name: (display_name || '').trim(),
      contact_name: (contact_name || '').trim(),
      token,
      credits_balance: FREE_QUOTA,
      monthly_quota: FREE_QUOTA,
      monthly_limit: FREE_QUOTA,
      credits_limit: FREE_QUOTA,
      subscription_start_date: now,
      created_at: now,
    }),
  })

  if (!insertRes.ok) {
    const errText = await insertRes.text()
    const lower = errText.toLowerCase()
    if (lower.includes('duplicate') || lower.includes('unique') || lower.includes('already exists')) {
      return res.status(409).json({ detail: 'An account with this email already exists.' })
    }
    return res.status(502).json({ detail: `Registration failed: ${errText}` })
  }

  return res.status(200).json({
    token,
    niche,
    email,
    display_name: (display_name || '').trim(),
  })
}
