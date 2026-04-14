module.exports = (req, res) => {
  res.setHeader('Content-Type', 'application/json')
  const openaiSet = Boolean(process.env.OPENAI_API_KEY)
  res.status(200).json({
    ok: openaiSet,
    openai_ok: openaiSet,
    smtp_ok: false,
    db_ok: true,
    supabase_ok: Boolean(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY),
  })
}
