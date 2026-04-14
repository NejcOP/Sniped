// Handles both /api/health and /api/config-health
module.exports = (req, res) => {
  res.setHeader('Content-Type', 'application/json')
  const openaiSet = Boolean(process.env.OPENAI_API_KEY)
  const supabaseSet = Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY)
  res.status(200).json({
    ok: true,
    openai_ok: openaiSet,
    smtp_ok: false,
    db_ok: supabaseSet,
    supabase_ok: supabaseSet,
  })
}
