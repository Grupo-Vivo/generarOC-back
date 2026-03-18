/**
 * dbUsers.js — Conexión a SQL Server (DWH_SERVICIOS)
 *
 * Tablas usadas del schema TI:
 *   TI.USERS        — credenciales (Usuario_Key, PasswordHash, PasswordSalt...)
 *   TI.USER_ROLES   — asignación usuario ↔ rol (IsActive, ExpiresAt)
 *   TI.ROLES        — roles por aplicación (RoleKey, IsActive)
 *   TI.APPLICATIONS — registro de aplicaciones (ApplicationKey, IsActive)
 */
const sql    = require('mssql');
const logger = require('../utils/logger');

let pool = null;

function config() {
  return {
    server:   process.env.MSSQL_HOST,
    port:     parseInt(process.env.MSSQL_PORT, 10) || 1433,
    database: process.env.MSSQL_DATABASE || 'DWH_SERVICIOS',
    user:     process.env.MSSQL_USER,
    password: process.env.MSSQL_PASSWORD,
    options: {
      encrypt: false,
      trustServerCertificate: true,
      enableArithAbort: true,
    },
    pool: { max: 5, min: 1, idleTimeoutMillis: 30000 },
  };
}

async function initUsersPool() {
  if (pool) return pool;
  pool = await sql.connect(config());
  logger.info('Pool SQL Server (DWH_SERVICIOS) inicializado');
  return pool;
}

async function closeUsersPool() {
  if (pool) {
    await pool.close();
    pool = null;
    logger.info('Pool SQL Server cerrado');
  }
}

/**
 * Ejecuta una query parametrizada en SQL Server.
 * @param {string} queryStr  Query con @nombre como placeholders
 * @param {Object} params    { nombre: { type: sql.NVarChar, value: '...' } }
 */
async function queryUsers(queryStr, params = {}) {
  if (!pool) await initUsersPool();
  const request = pool.request(); 
  for (const [name, { type, value }] of Object.entries(params)) {
    request.input(name, type, value);
  }
 
  logger.debug('MSSQL:', queryStr.replace(/\s+/g, ' ').trim().substring(0, 120));
  
  try{
    const result = await request.query(queryStr); 
    return result.recordset;
  }catch(err){
    logger.error('[MMSQL] ', err["message"]);
  }

}

module.exports = { initUsersPool, closeUsersPool, queryUsers, sql };
