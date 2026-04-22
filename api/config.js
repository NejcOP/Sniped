const { handleCors } = require('./_cors')
function getSupabaseUrl() {
  return process.env.SUPABASE_URL || ''
}

function getSupabaseKey() {
  return process.env.SUPABASE_SERVICE_ROLE_KEY || ''
}

function getTokenFromRequest(req) {
  const authHeader = String(req.headers.authorization || '')
  if (authHeader.startsWith('Bearer ')) return authHeader.slice(7).trim()
  return ''
}

module.exports = async (req, res) => {
  if (handleCors(req, res)) return
  res.setHeader('Content-Type', 'application/json')

  if (req.method === 'PUT') {
    return handleConfigUpdate(req, res)
  }

  if (req.method !== 'GET') {
    return res.status(405).json({ detail: 'Method not allowed' })
  }

  const token = getTokenFromRequest(req)
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
    `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=smtp_accounts_json&limit=1`,
    { headers }
  )

  let smtpAccounts = []

  if (userRes.ok) {
    try {
      const rows = await userRes.json()
      if (Array.isArray(rows) && rows.length > 0) {
        const raw = rows[0].smtp_accounts_json
        const parsed = raw ? JSON.parse(raw) : []
        smtpAccounts = Array.isArray(parsed) ? parsed : []
      }
    } catch {
      smtpAccounts = []
    }
  }
  // If query fails (e.g. column missing), just return default empty config — don't 502

  const safeAccounts = smtpAccounts.map((a) => ({
    host: a.host || 'smtp.gmail.com',
    port: Number(a.port) || 587,
    email: a.email || '',
    from_name: a.from_name || '',
    password_set: Boolean(a.password && String(a.password).trim()),
  }))

  const first = smtpAccounts[0] || {}

  return res.status(200).json({
    openai_api_key: process.env.OPENAI_API_KEY ? '***' : '',
    smtp_host: first.host || '',
    // NOTE: actual email sending requires the Python backend deployed with BACKEND_URL
    smtp_port: Number(first.port) || 587,
    smtp_email: first.email || '',
    smtp_password_set: Boolean(first.password && String(first.password).trim()),
    smtp_accounts: safeAccounts,
    sending_strategy: 'round_robin',
    mail_signature: '',
    ghost_subject_template: '',
    ghost_body_template: '',
    golden_subject_template: '',
    golden_body_template: '',
    competitor_subject_template: '',
    competitor_body_template: '',
    speed_subject_template: '',
    speed_body_template: '',
    open_tracking_base_url: '',
    hubspot_webhook_url: '',
    google_sheets_webhook_url: '',
    auto_weekly_report_email: true,
    auto_monthly_report_email: true,
    proxy_url: '',
    proxy_urls: '',
    supabase_url: supabaseUrl,
    supabase_publishable_key: process.env.SUPABASE_PUBLISHABLE_KEY || '',
    supabase_service_role_key_set: true,
    supabase_primary_mode: true,
  })
}

async function handleConfigUpdate(req, res) {
  const body = typeof req.body === 'string' ? JSON.parse(req.body) : (req.body || {})
  const token = getTokenFromRequest(req)
  if (!token) return res.status(401).json({ detail: 'Invalid or expired session token.' })

  const supabaseUrl = getSupabaseUrl()
  const supabaseKey = getSupabaseKey()
  if (!supabaseUrl || !supabaseKey) return res.status(503).json({ detail: 'Database not configured.' })

  const dbHeaders = {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
  }

  if (body.smtp_accounts !== undefined) {
    // Fetch existing to preserve passwords that weren't sent
    const existingRes = await fetch(
      `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=smtp_accounts_json&limit=1`,
      { headers: dbHeaders }
    )
    let existing = []
    if (existingRes.ok) {
      try {
        const rows = await existingRes.json()
        const raw = rows?.[0]?.smtp_accounts_json
        existing = raw ? JSON.parse(raw) : []
      } catch { existing = [] }
    }

    // Merge: keep old password if new one is blank
    const merged = (body.smtp_accounts || []).map((acct, i) => {
      const old = existing[i] || {}
      return {
        host: acct.host || old.host || 'smtp.gmail.com',
        port: Number(acct.port) || Number(old.port) || 587,
        email: acct.email || old.email || '',
        from_name: acct.from_name || old.from_name || '',
        // Only update password if explicitly provided
        password: acct.password ? acct.password : (old.password || ''),
      }
    })

    const patchRes = await fetch(
      `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}`,
      {
        method: 'PATCH',
        headers: { ...dbHeaders, Prefer: 'return=minimal' },
        body: JSON.stringify({ smtp_accounts_json: JSON.stringify(merged) }),
      }
    )

    if (!patchRes.ok) {
      const err = await patchRes.text()
      return res.status(502).json({ detail: `Config save failed: ${err}` })
    }
  }

  return res.status(200).json({ ok: true })
}
