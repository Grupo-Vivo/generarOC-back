/**
 * airtableAuth.middleware.js
 *
 * Valida que la petición al endpoint de Airtable incluya el API Key correcto.
 *
 * Airtable debe enviar el header:
 *   X-Api-Key: <valor de AIRTABLE_API_KEY en .env>
 *
 * Si el header falta o no coincide → 401 Unauthorized.
 */
const logger = require('../utils/logger');

module.exports = function airtableAuth(req, res, next) {
  const key       = req.headers['x-api-key'];
  const expected  = process.env.AIRTABLE_API_KEY;

  if (!expected) {
    logger.error('[airtableAuth] AIRTABLE_API_KEY no está definido en .env');
    return res.status(500).json({ ok: false, error: 'Endpoint no configurado en el servidor.' });
  }

  if (!key || key !== expected) {
    logger.warn(`[airtableAuth] Intento con API Key inválido — IP: ${req.ip}`);
    return res.status(401).json({ ok: false, error: 'API Key inválido o ausente.' });
  }

  // Inyectar identidad sintética para que comprasService sepa el origen
  req.origen     = 'AIRTABLE';
  req.usuarioKey = 'airtable';  // se guarda en usuario_key del historial
  next();
};
