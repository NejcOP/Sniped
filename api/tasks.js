module.exports = (req, res) => {
  res.setHeader('Content-Type', 'application/json')
  res.status(200).json({
    tasks: {},
    history: [],
    autopilot: { next_drip_at: null, high_score_threshold: 70 },
  })
}
