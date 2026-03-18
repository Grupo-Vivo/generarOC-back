const { verificarToken } = require('../services/authService');
const logger = require('../utils/logger');

function requireAuth(req, res, next) {
  const header = req.headers['authorization'];
  if (!header?.startsWith('Bearer ')) {
    return res.status(401).json({ ok: false, error: 'No autenticado. Se requiere token.' });
  }
  const token = header.slice(7);
  try {
    req.usuario = verificarToken(token); // { usuarioKey, roles, mustChangePassword }
    next();
  } catch (err) {
    const esExpirado = err.name === 'TokenExpiredError';
    logger.debug('Token inválido:', err.message);
    return res.status(401).json({
      ok: false,
      error: esExpirado ? 'Sesión expirada. Vuelve a iniciar sesión.' : 'Token inválido.',
    });
  }
}

module.exports = { requireAuth };
