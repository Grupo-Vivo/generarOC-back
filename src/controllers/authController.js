const { login } = require('../services/authService');
const logger    = require('../utils/logger');

async function handleLogin(req, res) {
  try {
    const { usuario, password } = req.body;
    const ip = req.headers['x-forwarded-for'] || req.ip || null;
    const resultado = await login(usuario, password, ip);
    return res.json({ ok: true, ...resultado });
  } catch (err) {
    logger.warn('Login fallido:', err.message);
    return res.status(401).json({ ok: false, error: err.message });
  }
}

function handleMe(req, res) {
  return res.json({ ok: true, usuario: req.usuario });
}

module.exports = { handleLogin, handleMe };
