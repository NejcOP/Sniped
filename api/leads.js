// Native Vercel function — Lead Management
const { handleCors } = require('../_cors')
// Reads leads from Supabase with filtering, sorting, search, and pagination.

const SUPABASE_COLUMNS = [
  'id','business_name','contact_name','email','website_url','phone_number','phone_formatted','phone_type',
  'rating','review_count','address','city','search_keyword','insecure_site',
  'main_shortcoming','ai_description','ai_score','client_tier','status','enrichment_status',
  'scraped_at','enriched_at','sent_at','last_contacted_at','follow_up_count',
  'generated_email_body','crm_comment','status_updated_at','last_sender_email',
  'is_ads_client','is_website_client','worker_id','assigned_worker_at','paid_at',
  'pipeline_stage','client_folder_id','open_tracking_token','open_count',
  'first_opened_at','last_opened_at','created_at','enrichment_data',
].join(',')

const BLACKLISTED_STATUSES = new Set(['blacklisted','do_not_contact','unsubscribed','bounced','spam'])

function isBlacklisted(status) {
  return BLACKLISTED_STATUSES.has(String(status || '').toLowerCase().trim())
}

function getTokenFromReq(req) {
  const auth = String(req.headers?.authorization || '')
  if (auth.startsWith('Bearer ')) return auth.slice(7).trim()
  return String(req.query?.token || '').trim()
}

module.exports = async (req, res) => {
  if (handleCors(req, res)) return
  res.setHeader('Content-Type', 'application/json')

  if (req.method !== 'GET') {
    return res.status(405).json({ detail: 'Method not allowed' })
  }

  const supabaseUrl = process.env.SUPABASE_URL || ''
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || ''
  const token = getTokenFromReq(req)

  const empty = { items: [], total: 0, count: 0, page: 1, limit: 50 }

  if (!token || !supabaseUrl || !supabaseKey) {
    return res.status(200).json(empty)
  }

  const dbHeaders = {
    apikey: supabaseKey,
    Authorization: `Bearer ${supabaseKey}`,
    'Content-Type': 'application/json',
  }

  try {
    // Resolve user
    const userRes = await fetch(
      `${supabaseUrl}/rest/v1/users?token=eq.${encodeURIComponent(token)}&select=id&limit=1`,
      { headers: dbHeaders }
    )
    if (!userRes.ok) return res.status(200).json(empty)
    const users = await userRes.json()
    if (!Array.isArray(users) || users.length === 0) return res.status(200).json(empty)
    const userId = users[0].id

    // Query params
    const limit = Math.max(1, Math.min(Number(req.query?.limit) || 50, 500))
    const page = Math.max(1, Number(req.query?.page) || 1)
    const sort = String(req.query?.sort || 'recent').toLowerCase()
    const statusFilter = String(req.query?.status || '').toLowerCase().trim()
    const quickFilter = String(req.query?.quick_filter || 'all').toLowerCase().trim()
    const search = String(req.query?.search || '').toLowerCase().trim()
    const includeBlacklisted = ['1', 'true'].includes(String(req.query?.include_blacklisted || '').toLowerCase())

    // Determine sort column
    let orderCol = 'created_at'
    let orderAsc = false
    if (sort === 'best') { orderCol = 'ai_score'; orderAsc = false }
    else if (sort === 'name') { orderCol = 'business_name'; orderAsc = true }
    else if (sort === 'recent') { orderCol = 'created_at'; orderAsc = false }

    // Fetch all matching leads (we do server-side filtering in JS since Supabase REST has limited filter combinability)
    // Use count header to get total
    const leadsUrl = new URL(`${supabaseUrl}/rest/v1/leads`)
    leadsUrl.searchParams.set('user_id', `eq.${userId}`)
    leadsUrl.searchParams.set('select', SUPABASE_COLUMNS)
    leadsUrl.searchParams.set('order', `${orderCol}.${orderAsc ? 'asc' : 'desc'}`)
    leadsUrl.searchParams.set('limit', '5000') // fetch up to 5000 to allow client-side filter

    const leadsRes = await fetch(leadsUrl.toString(), {
      headers: { ...dbHeaders, Prefer: 'count=exact' }
    })
    if (!leadsRes.ok) return res.status(200).json(empty)

    let items = await leadsRes.json()
    if (!Array.isArray(items)) items = []

    // Filter: blacklist
    if (!includeBlacklisted) {
      items = items.filter(l => !isBlacklisted(l.status))
    }

    // Filter: status
    if (statusFilter && statusFilter !== 'all') {
      items = items.filter(l => String(l.status || '').toLowerCase() === statusFilter)
    }

    // Filter: quick_filter
    if (quickFilter && quickFilter !== 'all') {
      items = items.filter(l => matchesQuickFilter(l, quickFilter))
    }

    // Filter: search
    if (search) {
      items = items.filter(l => {
        const haystack = [
          l.business_name, l.contact_name, l.email, l.website_url, l.address, l.search_keyword
        ].map(v => String(v || '')).join(' ').toLowerCase()
        return haystack.includes(search)
      })
    }

    const total = items.length
    const offset = (page - 1) * limit
    const pageItems = items.slice(offset, offset + limit)

    return res.status(200).json({ items: pageItems, total, count: total, page, limit })
  } catch (err) {
    console.error('leads error', err)
    return res.status(200).json(empty)
  }
}

function matchesQuickFilter(lead, filter) {
  const status = String(lead.status || '').toLowerCase()
  switch (filter) {
    case 'no_email': return !lead.email
    case 'has_email': return Boolean(lead.email)
    case 'sent': return Boolean(lead.sent_at)
    case 'not_sent': return !lead.sent_at
    case 'enriched': return ['enriched', 'queued_mail', 'contacted', 'replied', 'won', 'paid', 'interested', 'meeting set'].includes(status)
    case 'not_enriched': return ['scraped', 'pending'].includes(status)
    case 'opened': return Number(lead.open_count) > 0
    case 'replied': return ['replied', 'interested', 'meeting set'].includes(status)
    case 'won': return ['won', 'paid'].includes(status)
    case 'low_score': return Number(lead.ai_score || 0) < 4
    case 'high_score': return Number(lead.ai_score || 0) >= 7
    default: return true
  }
}
