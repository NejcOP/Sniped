const { handleCors } = require('./_cors')
function getSupabaseUrl() { return process.env.SUPABASE_URL || '' }
function getSupabaseKey() { return process.env.SUPABASE_SERVICE_ROLE_KEY || '' }

function getToken(req) {
  const auth = String(req.headers.authorization || '')
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim()
  return ''
}

module.exports = async (req, res) => {
  if (handleCors(req, res)) return
  res.setHeader('Content-Type', 'application/json')

  const token = getToken(req)
  const supabaseUrl = getSupabaseUrl()
  const supabaseKey = getSupabaseKey()

  const empty = {
    total_leads: 0, emails_sent: 0, opened_count: 0, opens_total: 0,
    open_rate: 0, paid_count: 0, total_revenue: 0, setup_revenue: 0,
    setup_milestone: 10000, milestone_progress_pct: 0,
    monthly_recurring_revenue: 0, website_clients: 0, ads_clients: 0,
    ads_and_website_clients: 0, mrr_goal: 5000, queued_mail_count: 0,
    next_drip_at: null, reply_rate: 0, replies_count: 0,
    found_this_month: 0, contacted_this_month: 0, replied_this_month: 0, won_this_month: 0,
    found_this_week: 0, contacted_this_week: 0, replied_this_week: 0, won_this_week: 0,
    client_folder_count: 0,
    pipeline: { scraped: 0, contacted: 0, replied: 0, won_paid: 0 },
  }

  if (!token || !supabaseUrl || !supabaseKey) {
    return res.status(200).json(empty)
  }

  try {
    const dbHeaders = {
      apikey: supabaseKey,
      Authorization: `Bearer ${supabaseKey}`,
    }

    // Get user id from token
    const userRes = await fetch(
      `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`,
      { headers: dbHeaders }
    )
    if (!userRes.ok) return res.status(200).json(empty)
    const users = await userRes.json()
    if (!Array.isArray(users) || users.length === 0) return res.status(200).json(empty)
    const userId = users[0].id

    // Count leads
    const leadsRes = await fetch(
      `${supabaseUrl}/rest/v1/leads?user_id=eq.${encodeURIComponent(userId)}&select=id,status,email_sent,opened`,
      { headers: dbHeaders }
    )
    let leads = []
    if (leadsRes.ok) {
      try { leads = await leadsRes.json() } catch { leads = [] }
    }

    const total_leads = leads.length
    const emails_sent = leads.filter(l => l.email_sent).length
    const opened_count = leads.filter(l => l.opened).length
    const paid_count = leads.filter(l => String(l.status || '').toLowerCase() === 'won').length
    const open_rate = emails_sent > 0 ? Math.round((opened_count / emails_sent) * 100) : 0
    const contacted = leads.filter(l => ['contacted', 'replied', 'won'].includes(String(l.status || '').toLowerCase())).length
    const replied = leads.filter(l => ['replied', 'won'].includes(String(l.status || '').toLowerCase())).length
    const reply_rate = emails_sent > 0 ? Math.round((replied / emails_sent) * 100) : 0

    return res.status(200).json({
      ...empty,
      total_leads,
      emails_sent,
      opened_count,
      open_rate,
      paid_count,
      reply_rate,
      replies_count: replied,
      pipeline: { scraped: total_leads, contacted, replied, won_paid: paid_count },
    })
  } catch {
    return res.status(200).json(empty)
  }
}
