const { handleCors } = require('./_cors')
module.exports = (req, res) => {
  if (handleCors(req, res)) return
  res.setHeader('Content-Type', 'application/json')
  res.status(200).json({
    tasks: {},
    history: [],
    autopilot: { next_drip_at: null, high_score_threshold: 70 },
  })
}
