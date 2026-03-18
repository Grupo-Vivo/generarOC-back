/**
 * airtable.routes.js
 *
 * Rutas del endpoint de integración con Airtable.
 * Todas protegidas por airtableAuth (X-Api-Key header).
 *
 * POST /api/airtable/oc  — Recibir y generar una OC desde Airtable
 * GET  /api/airtable/health — Verificar que el endpoint está activo
 */
const express        = require('express');
const airtableAuth   = require('../middleware/airtableAuth.middleware');
const { recibirDeAirtable } = require('../controllers/airtableController');

const router = express.Router();

// Health check (sin auth para facilitar monitoreo desde Airtable)
router.get('/health', (req, res) =>
  res.json({ ok: true, servicio: 'airtable-oc', version: '1.0' })
);

// Todas las demás rutas requieren API Key
router.use(airtableAuth);

// POST /api/airtable/oc
router.post('/oc', recibirDeAirtable);

module.exports = router;
