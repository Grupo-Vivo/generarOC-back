/**
 * db.js — Conexión al ERP Enkontrol (Sybase vía ODBC)
 *
 * SETUP LINUX:
 *   sudo apt-get install unixodbc unixodbc-dev freetds-bin freetds-dev tdsodbc
 *   Configurar /etc/odbcinst.ini y /etc/odbc.ini con el DSN del .env
 *
 * SETUP WINDOWS:
 *   Instalar driver ODBC Sybase/SAP y crear DSN de sistema en el Panel de Control
 */
const odbc   = require('odbc');
const logger = require('../utils/logger');

let pool = null;

async function initPool() {
  if (pool) return pool;
  const connectionString =
    `DSN=${process.env.ODBC_DSN};` +
    `UID=${process.env.ODBC_USER};` +
    `PWD=${process.env.ODBC_PASSWORD};`;

  pool = await odbc.pool({
    connectionString,
    initialSize: 2,
    incrementSize: 1,
    maxSize: parseInt(process.env.ODBC_POOL_SIZE, 10) || 5,
    connectionTimeout: parseInt(process.env.ODBC_CONNECT_TIMEOUT, 10) || 10000,
  });
  logger.info('Pool ODBC (Sybase) inicializado');
  return pool;
}

async function getConnection() {
  if (!pool) await initPool();
  return pool.connect();
}

/** Query sin transacción */
async function query(sql, params = []) {
  const conn = await getConnection();
  try {
    logger.debug('ODBC:', sql.replace(/\s+/g, ' ').trim().substring(0, 120));
    return await conn.query(sql, params);
  } finally {
    await conn.close();
  }
}

/**
 * Ejecuta fn(conn) dentro de una transacción.
 * Rollback automático si fn lanza error.
 */
async function withTransaction(fn) {
  const conn = await getConnection();
  try {
    await conn.beginTransaction();
    const result = await fn(conn);
    await conn.commit();
    return result;
  } catch (err) {
    await conn.rollback();
    logger.warn('TX Sybase rolled back:', err.message);
    throw err;
  } finally {
    await conn.close();
  }
}

async function closePool() {
  if (pool) {
    await pool.close();
    pool = null;
    logger.info('Pool ODBC cerrado');
  }
}

module.exports = { initPool, getConnection, query, withTransaction, closePool };
