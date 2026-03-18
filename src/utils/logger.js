/**
 * logger.js
 *
 * Logger estructurado para Enkontrol OC.
 * Compatible con pm2 — todos los logs van a stdout/stderr además de archivos.
 *
 * Formato en consola / pm2:
 *   [2026-03-06 14:22:01] [ERROR] [ocService] Mensaje del error
 *   [2026-03-06 14:22:01] [INFO]  [authService] Login OK: israel.castro
 *
 * Formato en archivo (JSON, para herramientas de análisis):
 *   {"timestamp":"2026-03-06 14:22:01","level":"error","modulo":"ocService","message":"...","stack":"..."}
 *
 * USO:
 *   const logger = require('../utils/logger');
 *
 *   // Sin módulo (general)
 *   logger.info('Servidor iniciado en puerto 3001');
 *   logger.error('Error grave', { stack: err.stack });
 *
 *   // Con módulo (recomendado — aparece entre corchetes en el log)
 *   const log = logger.child({ modulo: 'ocService' });
 *   log.info('Batch iniciado: 5 bloques');
 *   log.error(`Bloque B1: ${err.message}`, { stack: err.stack });
 *
 *   // Shorthand estático (el módulo se infiere del mensaje si empieza con [NombreModulo])
 *   logger.error('[pdfService] No se pudo generar PDF: ' + err.message);
 */

const winston   = require('winston');
const path      = require('path');
const fs        = require('fs');

const LOG_DIR   = process.env.LOG_DIR   || './logs';
const LOG_LEVEL = process.env.LOG_LEVEL || 'info';
const APP_NAME  = process.env.APP_NAME  || 'enkontrol-oc';

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

// ─── Formato consola / pm2 ────────────────────────────────────────────────────
// [2026-03-06 14:22:01] [ERROR] [ocService] Descripción   (stack si hay)
const consoleFmt = winston.format.printf(({ timestamp, level, message, modulo, stack, ...meta }) => {
  const lvl   = level.toUpperCase().padEnd(5);
  const mod   = modulo ? `[${modulo}] ` : '';
  const extra = Object.keys(meta).length ? ' ' + JSON.stringify(meta) : '';
  const base  = `[${timestamp}] [${lvl}] ${mod}${message}${extra}`;
  // Stack trace en líneas separadas, identado
  return stack ? `${base}\n${String(stack).split('\n').map(l => '    ' + l).join('\n')}` : base;
});

// ─── Formato archivo (JSON estructurado) ─────────────────────────────────────
const fileFmt = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

// ─── Transports ──────────────────────────────────────────────────────────────
const transports = [
  // Consola — pm2 captura stdout/stderr de aquí
  new winston.transports.Console({
    format: winston.format.combine(
      winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
      winston.format.errors({ stack: true }),
      // Colorize solo en desarrollo
      ...(process.env.NODE_ENV !== 'production' ? [winston.format.colorize({ all: false })] : []),
      consoleFmt
    ),
  }),

  // Archivo de errores — solo level 'error'
  new winston.transports.File({
    filename: path.join(LOG_DIR, 'error.log'),
    level:    'error',
    format:   fileFmt,
    maxsize:  10 * 1024 * 1024,  // 10 MB
    maxFiles: 5,
    tailable: true,
  }),

  // Archivo combinado — todos los niveles
  new winston.transports.File({
    filename: path.join(LOG_DIR, 'combined.log'),
    format:   fileFmt,
    maxsize:  20 * 1024 * 1024,  // 20 MB
    maxFiles: 5,
    tailable: true,
  }),
];

// ─── Instancia principal ──────────────────────────────────────────────────────
const logger = winston.createLogger({
  level:             LOG_LEVEL,
  defaultMeta:       { app: APP_NAME },
  transports,
  exceptionHandlers: [
    new winston.transports.File({ filename: path.join(LOG_DIR, 'exceptions.log'), format: fileFmt }),
    new winston.transports.Console({ format: winston.format.combine(
      winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
      consoleFmt
    )}),
  ],
  rejectionHandlers: [
    new winston.transports.File({ filename: path.join(LOG_DIR, 'exceptions.log'), format: fileFmt }),
  ],
});

module.exports = logger;
