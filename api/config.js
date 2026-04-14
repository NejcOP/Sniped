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
  res.setHeader('Content-Type', 'application/json')

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

  if (!userRes.ok) {
    return res.status(502).json({ detail: 'Config lookup failed.' })
  }

  const rows = await userRes.json()
  if (!Array.isArray(rows) || rows.length === 0) {
    return res.status(401).json({ detail: 'Invalid or expired session token.' })
  }

  let smtpAccounts = []
  try {
    const raw = rows[0].smtp_accounts_json
    const parsed = raw ? JSON.parse(raw) : []
    smtpAccounts = Array.isArray(parsed) ? parsed : []
  } catch {
    smtpAccounts = []
  }

  const safeAccounts = smtpAccounts.map((a) => ({
    host: a.host || 'smtp.gmail.com',
    port: Number(a.port) || 587,
    email: a.email || '',
    from_name: a.from_name || '',
    password_set: Boolean(a.password && String(a.password).trim()),
  }))

  const first = smtpAccounts[0] || {}

  return res.status(200).json({
    openai_api_key: '',
    smtp_host: first.host || '',
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
