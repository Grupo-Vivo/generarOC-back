/**
 * authService.js
 *
 * Autenticación contra DWH_SERVICIOS (SQL Server) — tablas TI.*
 *
 * Flujo de login:
 *   1. Buscar usuario por Usuario_Key en TI.USERS
 *   2. Verificar cuenta no bloqueada (AccountLockedUntil)
 *   3. Verificar password con bcrypt (PasswordHash)
 *   4. Verificar que tenga rol activo en TI.USER_ROLES para APP_KEY
 *   5. Emitir JWT con { usuarioKey, roles }
 *   6. Actualizar LastLoginAt y reiniciar FailedLoginAttempts
 */
const bcrypt = require('bcryptjs');
const jwt    = require('jsonwebtoken');
const { queryUsers, sql } = require('../config/dbUsers');
const logger = require('../utils/logger');

const MAX_INTENTOS_FALLIDOS = 5;
const MINUTOS_BLOQUEO       = 300;

// ─── JWT ─────────────────────────────────────────────────────────────────────

function generarToken(payload) {
  return jwt.sign(payload, process.env.JWT_SECRET, {
    expiresIn: process.env.JWT_EXPIRES_IN || '8h',
  });
}

function verificarToken(token) {
  return jwt.verify(token, process.env.JWT_SECRET);
}

// ─── LOGIN ────────────────────────────────────────────────────────────────────

async function login(usuarioKey, password, clientIp = null) {
  if (!usuarioKey || !password) {
    throw new Error('Usuario y contraseña son requeridos.');
  }

  const key = usuarioKey.trim();
  const ERR = 'Usuario o contraseña incorrectos.'; // mensaje genérico (evita user enumeration)

  // 1. Buscar usuario
  const users = await queryUsers(
    `SELECT UserId, Usuario_Key, PasswordHash,
            MustChangePassword, FailedLoginAttempts, AccountLockedUntil
     FROM TI.USERS
     WHERE Usuario_Key = @key`,
    { key: { type: sql.NChar(20), value: key } }
  );

  if (!users.length) {
    logger.warn(`Login fallido — no encontrado: ${key}`);
    throw new Error(ERR);
  }

  const user = users[0];

  // 2. Verificar bloqueo
  if (user.AccountLockedUntil && new Date(user.AccountLockedUntil) > new Date()) {
    const hasta = new Date(user.AccountLockedUntil).toLocaleTimeString('es-MX');
    throw new Error(`Cuenta bloqueada hasta las ${hasta}. Contacta al administrador.`);
  }

  // 3. Verificar password
  const ok = await bcrypt.compare(password, user.PasswordHash);
  if (!ok) {
    actualizarIntentosFallidos(user.UserId, user.FailedLoginAttempts || 0);
    logger.warn(`Login fallido — password incorrecto: ${key}`);
    throw new Error(ERR);
  }

  // 4. Verificar acceso a la aplicación
  const appKey = process.env.APP_KEY || 'enkontrol-oc-masiva';
  const roles = await queryUsers(
    `SELECT r.RoleName, r.RoleKey
     FROM TI.USER_ROLES ur
     JOIN TI.ROLES       r ON r.RoleId        = ur.RoleId
     JOIN TI.APPLICATIONS a ON a.ApplicationId = r.ApplicationId
     WHERE ur.Usuario_Key = @key
       AND a.ApplicationKey = @appKey
       AND ur.IsActive = 1
       AND r.IsActive  = 1
       AND a.IsActive  = 1
       AND (ur.ExpiresAt IS NULL OR ur.ExpiresAt > GETDATE())`,
    {
      key:    { type: sql.NChar(20),    value: key },
      appKey: { type: sql.NVarChar(50), value: appKey },
    }
  );

  if (!roles.length) {
    logger.warn(`Login denegado — sin rol en "${appKey}": ${key}`);
    throw new Error('No tienes permisos para acceder a esta aplicación. Contacta al administrador.');
  }

  // 5. Obtener id_Ek del ERP (empleado numérico en Enkontrol)
  //    TI.USUARIO_TI.id_Ek → join por Usuario_Key
  //    Se incluye en el token para usarlo como comprador / empleado_modifica
  let idEk = null;
  try {
    const ekRows = await queryUsers(
      `SELECT id_Ek FROM DWH_VIVO.Catalogos.USUARIO_TI WHERE Usuario_Key = @key`,
      { key: { type: sql.NChar(20), value: key } }
    );
    if (ekRows.length && ekRows[0].id_Ek != null) {
      idEk = Number(ekRows[0].id_Ek);
    }
  } catch (e) {
    // No es crítico — si la tabla no existe o el usuario no tiene id_Ek,
    // el sistema usará ERP_USUARIO_BATCH como fallback
    logger.warn(`No se pudo obtener id_Ek para ${key}: ${e.message}`);
  }

  // 6. Actualizar último acceso (fire and forget)
  actualizarUltimoAcceso(user.UserId, clientIp);

  // 7. Emitir token
  const tokenPayload = {
    usuarioKey:         user.Usuario_Key.trim(),
    mustChangePassword: !!user.MustChangePassword,
    roles:              roles.map(r => r.RoleKey),
    idEk,   // empleado numérico del ERP (comprador / empleado_modifica)
  };

  const token = generarToken(tokenPayload);
  logger.info(`Login OK: ${key} | roles: ${tokenPayload.roles.join(', ')} | idEk: ${idEk}`);

  return {
    token,
    usuario: {
      usuarioKey:         tokenPayload.usuarioKey,
      mustChangePassword: tokenPayload.mustChangePassword,
      roles:              tokenPayload.roles,
      idEk,
    },
  };
}

// ─── HELPERS INTERNOS ────────────────────────────────────────────────────────

async function actualizarIntentosFallidos(userId, intentosActuales) {
  try {
    const nuevos  = intentosActuales + 1;
    const bloqueo = nuevos >= MAX_INTENTOS_FALLIDOS
      ? `DATEADD(MINUTE, ${MINUTOS_BLOQUEO}, GETDATE())`
      : 'NULL';
    await queryUsers(
      `UPDATE TI.USERS
       SET FailedLoginAttempts = @n,
           AccountLockedUntil  = ${bloqueo},
           UpdatedAt = GETDATE()
       WHERE UserId = @id`,
      {
        n:  { type: sql.Int, value: nuevos },
        id: { type: sql.Int, value: userId },
      }
    );
  } catch (e) { logger.error('Error actualizando intentos fallidos:', e.message); }
}

async function actualizarUltimoAcceso(userId, ip) {
  try {
    await queryUsers(
      `UPDATE TI.USERS
       SET FailedLoginAttempts = 0,
           AccountLockedUntil  = NULL,
           LastLoginAt         = GETDATE(),
           LastLoginIP         = @ip,
           UpdatedAt           = GETDATE()
       WHERE UserId = @id`,
      {
        id:  { type: sql.Int,          value: userId },
        ip:  { type: sql.NVarChar(45), value: ip || null },
      }
    );
  } catch (e) { logger.error('Error actualizando último acceso:', e.message); }
}

module.exports = { login, verificarToken };
