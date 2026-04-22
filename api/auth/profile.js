const crypto = require('crypto')
const { handleCors } = require('../_cors')

function getSupabaseUrl() {
  return process.env.SUPABASE_URL || ''
}

function getSupabaseKey() {
  return process.env.SUPABASE_SERVICE_ROLE_KEY || ''
}

function hashPassword(password, salt) {
  return crypto.pbkdf2Sync(password, salt, 260000, 32, 'sha256').toString('hex')
}

const FREE_QUOTA = 50

const PLAN_DISPLAY_NAMES = {
  free: 'Free Plan',
  hustler: 'Hustler',
  growth: 'Growth',
  scale: 'Scale',
  empire: 'Empire',
  pro: 'Pro Plan',
}

module.exports = async (req, res) => {
  if (handleCors(req, res)) return
  res.setHeader('Content-Type', 'application/json')

  if (req.method === 'PUT') {
    return handleProfileUpdate(req, res)
  }

  if (req.method !== 'POST') {
    return res.status(405).json({ detail: 'Method not allowed' })
  }

  const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {})

  // Support both Bearer token header and body token
  let token = body.token || ''
  const authHeader = String(req.headers.authorization || '')
  if (!token && authHeader.startsWith('Bearer ')) {
    token = authHeader.slice(7).trim()
  }

  if (!token) {
    return res.status(401).json({ detail: 'Invalid or expired session token.' })
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

  const userRes = await fetch(
    `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=email,niche,display_name,contact_name,account_type,credits_balance,monthly_quota,monthly_limit,credits_limit,topup_credits_balance,subscription_start_date,plan_key,subscription_active,subscription_status,subscription_cancel_at,subscription_cancel_at_period_end,average_deal_value&limit=1`,
    { headers }
  )

  if (!userRes.ok) {
    return res.status(502).json({ detail: 'Profile lookup failed.' })
  }

  const rows = await userRes.json()
  if (!Array.isArray(rows) || rows.length === 0) {
    return res.status(401).json({ detail: 'Invalid or expired session token.' })
  }

  const row = rows[0]
  const planKey = String(row.plan_key || 'free').toLowerCase()
  const isSubscribed = Boolean(row.subscription_active)
  const creditsBalance = Number(row.credits_balance || 0)
  const creditsLimit = Number(row.monthly_quota || row.monthly_limit || row.credits_limit || FREE_QUOTA)
  const topupCredits = Number(row.topup_credits_balance || 0)

  return res.status(200).json({
    email: row.email || '',
    niche: row.niche || '',
    display_name: row.display_name || '',
    contact_name: row.contact_name || '',
    account_type: row.account_type || 'entrepreneur',
    average_deal_value: Number(row.average_deal_value || 1000),
    credits_balance: creditsBalance,
    credits_limit: creditsLimit,
    monthly_limit: creditsLimit,
    monthly_quota: creditsLimit,
    topup_credits_balance: topupCredits,
    subscription_start_date: row.subscription_start_date || null,
    subscription_active: isSubscribed,
    isSubscribed,
    currentPlanName: PLAN_DISPLAY_NAMES[planKey] || (isSubscribed ? 'Pro Plan' : 'Free Plan'),
    subscription_status: String(row.subscription_status || '').toLowerCase(),
    subscription_cancel_at: row.subscription_cancel_at || null,
    subscription_cancel_at_period_end: Boolean(row.subscription_cancel_at_period_end),
    plan_key: planKey,
  })
}

async function handleProfileUpdate(req, res) {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {})

  let token = body.token || ''
  const authHeader = String(req.headers.authorization || '')
  if (!token && authHeader.startsWith('Bearer ')) token = authHeader.slice(7).trim()
  if (!token) return res.status(401).json({ detail: 'Invalid or expired session token.' })

  const supabaseUrl = getSupabaseUrl()
  const supabaseKey = getSupabaseKey()
  if (!supabaseUrl || !supabaseKey) return res.status(503).json({ detail: 'Database not configured.' })

  const dbHeaders = {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
  }

  // Password change flow
  if (body.current_password && body.new_password) {
    if (String(body.new_password).length < 8) {
      return res.status(400).json({ detail: 'New password must be at least 8 characters.' })
    }

    // Fetch current hash+salt
    const userRes = await fetch(
      `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id,password_hash,salt&limit=1`,
      { headers: dbHeaders }
    )
    if (!userRes.ok) return res.status(502).json({ detail: 'Password verification failed.' })
    const rows = await userRes.json()
    if (!Array.isArray(rows) || rows.length === 0) return res.status(401).json({ detail: 'Invalid or expired session token.' })

    const row = rows[0]
    const expected = hashPassword(String(body.current_password), String(row.salt || ''))
    const actual = String(row.password_hash || '')

    // Constant-time compare
    const expBuf = Buffer.from(expected, 'hex')
    const actBuf = Buffer.from(actual, 'hex')
    const match = expBuf.length === actBuf.length && crypto.timingSafeEqual(expBuf, actBuf)

    if (!match) {
      return res.status(401).json({ detail: 'Current password is incorrect.' })
    }

    const newSalt = crypto.randomBytes(32).toString('hex')
    const newHash = hashPassword(String(body.new_password), newSalt)

    const patchRes = await fetch(
      `${supabaseUrl}/rest/v1/users?id=eq.${encodeURIComponent(row.id)}`,
      {
        method: 'PATCH',
        headers: { ...dbHeaders, Prefer: 'return=minimal' },
        body: JSON.stringify({ password_hash: newHash, salt: newSalt }),
      }
    )
    if (!patchRes.ok) {
      const err = await patchRes.text()
      return res.status(502).json({ detail: `Password update failed: ${err}` })
    }

    return res.status(200).json({ ok: true })
  }

  // Profile fields update
  const updates = {}
  if (body.display_name !== undefined) updates.display_name = String(body.display_name || '').trim()
  if (body.niche !== undefined) updates.niche = String(body.niche || '').trim()
  if (body.account_type !== undefined) updates.account_type = String(body.account_type || 'entrepreneur').toLowerCase()
  if (body.average_deal_value !== undefined) updates.average_deal_value = Number(body.average_deal_value || 1000)

  if (Object.keys(updates).length === 0) {
    return res.status(400).json({ detail: 'No fields to update.' })
  }

  const patchRes = await fetch(
    `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}`,
    {
      method: 'PATCH',
      headers: { ...dbHeaders, Prefer: 'return=representation' },
      body: JSON.stringify(updates),
    }
  )

  if (!patchRes.ok) {
    const err = await patchRes.text()
    return res.status(502).json({ detail: `Profile update failed: ${err}` })
  }

  const updated = await patchRes.json()
  const r = Array.isArray(updated) && updated.length > 0 ? updated[0] : {}

  return res.status(200).json({
    email: r.email || '',
    niche: r.niche || updates.niche || '',
    display_name: r.display_name || updates.display_name || '',
    account_type: r.account_type || updates.account_type || 'entrepreneur',
    average_deal_value: Number(r.average_deal_value || updates.average_deal_value || 1000),
  })
}
