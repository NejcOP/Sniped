// Native Vercel function — GET /api/mailer/campaign-stats
// Reads leads + campaign data from Supabase to compute mailer stats.

function getTokenFromReq(req) {
  const auth = String(req.headers?.authorization || '')
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim()
  return String(req.query?.token || '').trim()
}

const EMPTY = {
  sent: 0, opened: 0, replied: 0, bounced: 0, opens_total: 0,
  open_rate: 0, reply_rate: 0, bounce_rate: 0,
  ab_breakdown: { A: 0, B: 0 },
  sequences: [], saved_templates: [], recent_events: [],
}

module.exports = async (req, res) => {
  res.setHeader('Content-Type', 'application/json')
  if (req.method !== 'GET') return res.status(405).json({ detail: 'Method not allowed' })

  const supabaseUrl = process.env.SUPABASE_URL || ''
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || ''
  const token = getTokenFromReq(req)

  if (!token || !supabaseUrl || !supabaseKey) return res.status(200).json(EMPTY)

  const h = { apikey: supabaseKey, Authorization: `Bearer ${supabaseKey}` }

  try {
    // Resolve user id
    const uRes = await fetch(`${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`, { headers: h })
    if (!uRes.ok) return res.status(200).json(EMPTY)
    const users = await uRes.json()
    if (!Array.isArray(users) || !users.length) return res.status(200).json(EMPTY)
    const userId = users[0].id

    // Fetch leads metrics
    const lRes = await fetch(
      `${supabaseUrl}/rest/v1/leads?user_id=eq.${encodeURIComponent(userId)}&select=sent_at,open_count,status,reply_detected_at,bounced_at&limit=5000`,
      { headers: h }
    )
    let leads = lRes.ok ? await lRes.json() : []
    if (!Array.isArray(leads)) leads = []

    const sent = leads.filter(l => l.sent_at).length
    const opened = leads.filter(l => Number(l.open_count || 0) > 0).length
    const opens_total = leads.reduce((s, l) => s + Number(l.open_count || 0), 0)
    const replied = leads.filter(l => {
      const st = String(l.status || '').toLowerCase()
      return l.reply_detected_at || ['replied', 'interested', 'meeting set'].includes(st)
    }).length
    const bounced = leads.filter(l => {
      const st = String(l.status || '').toLowerCase()
      return l.bounced_at || ['bounced', 'invalid_email'].includes(st)
    }).length

    // Fetch sequences from runtime_kv
    const seqKey = `mailer_sequences:${userId}`
    const seqRes = await fetch(
      `${supabaseUrl}/rest/v1/runtime_kv?key=eq.${encodeURIComponent(seqKey)}&select=value&limit=1`,
      { headers: h }
    )
    let sequences = []
    if (seqRes.ok) {
      const rows = await seqRes.json()
      if (Array.isArray(rows) && rows.length) {
        try { sequences = JSON.parse(rows[0].value) } catch { sequences = [] }
      }
    }
    if (!Array.isArray(sequences)) sequences = []

    // Fetch saved templates from runtime_kv
    const tplKey = `mailer_templates:${userId}`
    const tplRes = await fetch(
      `${supabaseUrl}/rest/v1/runtime_kv?key=eq.${encodeURIComponent(tplKey)}&select=value&limit=1`,
      { headers: h }
    )
    let saved_templates = []
    if (tplRes.ok) {
      const rows = await tplRes.json()
      if (Array.isArray(rows) && rows.length) {
        try { saved_templates = JSON.parse(rows[0].value) } catch { saved_templates = [] }
      }
    }
    if (!Array.isArray(saved_templates)) saved_templates = []

    return res.status(200).json({
      sent, opened, replied, bounced, opens_total,
      open_rate: sent ? Math.round((opened / sent) * 1000) / 10 : 0,
      reply_rate: sent ? Math.round((replied / sent) * 1000) / 10 : 0,
      bounce_rate: sent ? Math.round((bounced / sent) * 1000) / 10 : 0,
      ab_breakdown: { A: 0, B: 0 },
      sequences, saved_templates, recent_events: [],
    })
  } catch (err) {
    console.error('campaign-stats error', err)
    return res.status(200).json(EMPTY)
  }
}
