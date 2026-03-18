require('dotenv').config();

const express = require('express');
const cors    = require('cors');
const helmet  = require('helmet');

const { initPool,      closePool }      = require('./config/db');
const { initUsersPool, closeUsersPool } = require('./config/dbUsers');
const { requireAuth }  = require('./middleware/auth.middleware');
const authRoutes     = require('./routes/auth.routes');
const ocRoutes       = require('./routes/oc.routes');
const airtableRoutes = require('./routes/airtable.routes');
const pdfRoutes = require('./routes/pdf.router');
const logger     = require('./utils/logger');

const app  = express();
const PORT = parseInt(process.env.PORT, 10) || 3001;

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(helmet());
app.use(cors({ origin: process.env.CORS_ORIGIN || 'http://localhost:3000' }));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use((req, res, next) => {
  logger.debug(`${req.method} ${req.path}`);
  next();
});

// ── Rutas ─────────────────────────────────────────────────────────────────────
app.use('/api/auth',     authRoutes);                 // POST /login, GET /me
app.use('/api/oc',       requireAuth, ocRoutes);      // Todas protegidas con JWT
app.use('/api/airtable', airtableRoutes);              // Protegido con X-Api-Key
app.use('/api/pdf', pdfRoutes);              // Protegido con X-Api-Key

// ── Errores ───────────────────────────────────────────────────────────────────
app.use((req, res) =>
  res.status(404).json({ ok: false, error: 'Ruta no encontrada.' })
);
app.use((err, req, res, next) => {
  logger.error('Error no controlado:', err);
  res.status(500).json({ ok: false, error: err.message || 'Error interno.' });
});

// ── Inicio ────────────────────────────────────────────────────────────────────
async function start() {
  try {
    await Promise.all([initPool(), initUsersPool()]);
    app.listen(PORT, () => {
      logger.info(`Servidor iniciado en puerto ${PORT}`);
      logger.info(`Sybase DSN: ${process.env.ODBC_DSN} | SQL Server: ${process.env.MSSQL_HOST}`);
    });
  } catch (err) {
    logger.error('Error al iniciar:', err);
    process.exit(1);
  }
}

const shutdown = async () => {
  await Promise.all([closePool(), closeUsersPool()]);
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT',  shutdown);

start();
module.exports = app;
