// Native Vercel function — Market Intelligence / Niche Recommendation
const { handleCors } = require('./_cors')
// Calls OpenAI directly and caches results in Supabase runtime_kv.
// Mirrors the Python backend /api/recommend-niche endpoint.

const { createClient } = require('@supabase/supabase-js')

function supabase() {
  const url = process.env.SUPABASE_URL
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY
  if (!url || !key) throw new Error('Supabase env vars not configured')
  return createClient(url, key)
}

async function getUserFromToken(token) {
  if (!token) return null
  try {
    const db = supabase()
    const { data, error } = await db
      .from('users')
      .select('id, email, plan_key, subscription_active, niche')
      .eq('token', token)
      .single()
    if (error || !data) return null
    return data
  } catch {
    return null
  }
}

function cacheKey(userId, countryCode) {
  return `niche_recommendation:${userId}:${String(countryCode || 'US').toUpperCase()}`
}

async function getCachedRecommendation(userId, countryCode, maxAgeSeconds) {
  try {
    const db = supabase()
    const { data, error } = await db
      .from('runtime_kv')
      .select('value, updated_at')
      .eq('key', cacheKey(userId, countryCode))
      .single()
    if (error || !data) return null
    const ageSeconds = (Date.now() - new Date(data.updated_at).getTime()) / 1000
    if (ageSeconds > maxAgeSeconds) return null
    return typeof data.value === 'string' ? JSON.parse(data.value) : data.value
  } catch {
    return null
  }
}

async function setCachedRecommendation(userId, countryCode, payload) {
  try {
    const db = supabase()
    const key = cacheKey(userId, countryCode)
    await db.from('runtime_kv').upsert(
      { key, value: JSON.stringify(payload), updated_at: new Date().toISOString() },
      { onConflict: 'key' }
    )
  } catch {
    // non-fatal — just skip caching
  }
}

const COUNTRY_LABELS = {
  US: 'United States',
  DE: 'Germany',
  AT: 'Austria',
  SI: 'Slovenia',
  GB: 'United Kingdom',
  FR: 'France',
  IT: 'Italy',
  ES: 'Spain',
  NL: 'Netherlands',
  PL: 'Poland',
  CA: 'Canada',
  AU: 'Australia',
}

const HEURISTIC_FALLBACKS = {
  US: [
    { keyword: 'Roofing contractors', location: 'Dallas, TX', reason: 'High storm season demand', expected_reply_rate: 6.2 },
    { keyword: 'HVAC companies', location: 'Phoenix, AZ', reason: 'Summer heat drives leads', expected_reply_rate: 5.8 },
    { keyword: 'Landscaping services', location: 'Atlanta, GA', reason: 'Spring growth season', expected_reply_rate: 5.1 },
  ],
  DE: [
    { keyword: 'Dachdeckerbetriebe', location: 'München', reason: 'Hohe Nachfrage nach Renovierungen', expected_reply_rate: 5.5 },
    { keyword: 'Sanitärtechnik', location: 'Berlin', reason: 'Steigender Wohnungsbau', expected_reply_rate: 4.9 },
    { keyword: 'Elektriker', location: 'Hamburg', reason: 'E-Mobilität treibt Nachfrage', expected_reply_rate: 5.2 },
  ],
  AT: [
    { keyword: 'Dachdecker', location: 'Wien', reason: 'Frühjahrssaison Renovierungen', expected_reply_rate: 5.3 },
    { keyword: 'Heizungstechnik', location: 'Graz', reason: 'Energiesanierung-Förderungen', expected_reply_rate: 5.7 },
    { keyword: 'Fenster & Türen', location: 'Linz', reason: 'Sanierungsbonus 2026', expected_reply_rate: 4.8 },
  ],
  SI: [
    { keyword: 'Strešni servisi', location: 'Ljubljana', reason: 'Pomladna sezona prenov', expected_reply_rate: 5.1 },
    { keyword: 'Kovinostrugarstvo', location: 'Maribor', reason: 'Industrijska posodobitev', expected_reply_rate: 4.7 },
    { keyword: 'Elektroinštalacije', location: 'Celje', reason: 'E-mobilnost povpraševanje', expected_reply_rate: 5.0 },
  ],
}

function heuristicForCountry(countryCode) {
  const cc = String(countryCode || 'US').toUpperCase()
  const recs = HEURISTIC_FALLBACKS[cc] || HEURISTIC_FALLBACKS.US
  return recs.map((r) => ({ ...r, country_code: cc }))
}

async function generateWithOpenAI(countryCode) {
  const apiKey = process.env.OPENAI_API_KEY
  if (!apiKey) return null

  const cc = String(countryCode || 'US').toUpperCase()
  const countryLabel = COUNTRY_LABELS[cc] || cc

  const systemPrompt = 'You are a revenue strategist for lead generation campaigns. Always return valid JSON only.'
  const userPrompt = `
Based on today's date (April 2026), current economic trends, and the fact that the user sells Google Ads / SEO services, suggest 3 most profitable niches ONLY for the selected country: ${countryLabel} (${cc}).

Return a JSON object with this exact structure:
{
  "recommendations": [
    {
      "keyword": "Roofers in Miami, FL",
      "location": "Miami, FL",
      "country_code": "${cc}",
      "reason": "Short reason (seasonality / margin / demand)",
      "expected_reply_rate": 6.2
    }
  ],
  "top_pick_index": 0
}

Rules:
- recommendations must have exactly 3 elements.
- All 3 locations must be in ${countryLabel}.
- country_code must always be '${cc}'.
- expected_reply_rate should be a realistic number between 1.0 and 15.0.
`.trim()

  try {
    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        temperature: 0.55,
        response_format: { type: 'json_object' },
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt },
        ],
      }),
    })
    if (!response.ok) return null
    const json = await response.json()
    const content = json?.choices?.[0]?.message?.content || '{}'
    return JSON.parse(content)
  } catch {
    return null
  }
}

module.exports = async (req, res) => {
  if (handleCors(req, res)) return
  // Only GET
  if (req.method !== 'GET') {
    return res.status(405).json({ detail: 'Method not allowed' })
  }

  try {
  // Auth
  const authHeader = String(req.headers?.authorization || '')
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7).trim() : req.query?.token
  const user = await getUserFromToken(token)
  const supabaseConfigured = Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  if (!user && supabaseConfigured) {
    return res.status(401).json({ detail: 'Unauthorized' })
  }

  const userId = user?.id || 'anonymous'
  const isFreePlan =
    !user?.subscription_active &&
    (String(user?.plan_key || 'free').toLowerCase() === 'free' || !user?.plan_key)
  const forceRefresh = ['1', 'true', 'yes'].includes(String(req.query?.refresh || '').toLowerCase())
  const countryCode = String(req.query?.country || req.query?.country_code || 'US').toUpperCase()

  // Cache TTL: free = 7 days, paid = 1 hour
  const cacheTtlSeconds = isFreePlan ? 7 * 24 * 3600 : 1 * 3600
  const refreshWindowDays = isFreePlan ? 7 : 0
  const refreshWindowHours = isFreePlan ? 168 : 1

  // Try cache unless force refresh
  if (!forceRefresh) {
    const cached = await getCachedRecommendation(userId, countryCode, cacheTtlSeconds)
    if (cached) {
      return res.status(200).json({
        ...cached,
        cached: true,
        credits_charged: 0,
        refresh_window_days: refreshWindowDays,
        refresh_window_hours: refreshWindowHours,
        selected_country_code: countryCode,
        ...(isFreePlan ? { monthly_limited: true } : {}),
      })
    }
  }

  // Generate
  const heuristic = heuristicForCountry(countryCode)
  let aiPayload = await generateWithOpenAI(countryCode)

  let recommendations = heuristic
  let source = 'heuristic'

  if (aiPayload && Array.isArray(aiPayload.recommendations) && aiPayload.recommendations.length >= 3) {
    const cc = countryCode
    recommendations = aiPayload.recommendations.slice(0, 3).map((r, idx) => ({
      keyword: String(r.keyword || heuristic[idx]?.keyword || '').trim(),
      location: String(r.location || heuristic[idx]?.location || '').trim(),
      country_code: cc,
      reason: String(r.reason || '').trim(),
      expected_reply_rate: Number(r.expected_reply_rate) || heuristic[idx]?.expected_reply_rate || 5.0,
    }))
    source = 'openai'
  }

  const topPickIndex = Number(aiPayload?.top_pick_index) || 0
  const generatedAt = new Date().toISOString()

  const result = {
    source,
    generated_at: generatedAt,
    recommendations,
    top_pick: recommendations[topPickIndex] || recommendations[0],
    performance_snapshot: [],
    selected_country_code: countryCode,
    selected_country_label: COUNTRY_LABELS[countryCode] || countryCode,
    cached: false,
    credits_charged: source === 'openai' ? 1 : 0,
    refresh_window_days: refreshWindowDays,
    refresh_window_hours: refreshWindowHours,
    ...(isFreePlan ? { monthly_limited: true } : {}),
  }

  await setCachedRecommendation(userId, countryCode, result)

  return res.status(200).json(result)
  } catch (err) {
    return res.status(500).json({ detail: err?.message || 'Internal server error' })
  }
}
