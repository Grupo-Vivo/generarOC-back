const path = require('path');
const fs   = require('fs');
const { v4: uuidv4 } = require('uuid');
const { parseExcel, getColumnMap } = require('../services/excelParser');
const { procesarBatch }            = require('../services/ocService');
const logger = require('../utils/logger');

// Sesiones temporales en memoria (preview antes de confirmar generación)
const sessions = new Map();
const UNA_HORA = 36_000_000;

function limpiarSesionesViejas() {
  for (const [id, s] of sessions.entries()) {
    if (Date.now() - s.createdAt > UNA_HORA) {
      sessions.delete(id);
      try { fs.unlinkSync(s.filePath); } catch (_) {}
    }
  }
}

// ── POST /api/oc/upload ───────────────────────────────────────────────────────
async function uploadExcel(req, res) {
  try {
    if (!req.file)
      return res.status(400).json({ ok: false, error: 'No se recibió ningún archivo.' });

    const { blocks, warnings, errors } = parseExcel(req.file.path);

    if (errors.length) {
      try { fs.unlinkSync(req.file.path); } catch (_) {}
      return res.status(422).json({
        ok: false,
        error: 'El archivo tiene errores que deben corregirse antes de continuar.',
        errores: errors, warnings,
      });
    }

    if (!blocks.length) {
      try { fs.unlinkSync(req.file.path); } catch (_) {}
      return res.status(422).json({ ok: false, error: 'No se encontraron bloques válidos.', warnings });
    }

    const sessionId = uuidv4();
    sessions.set(sessionId, { blocks, filePath: req.file.path, createdAt: Date.now() });
    limpiarSesionesViejas();

    const preview = blocks.map(b => ({
      bloqueId:      b.bloqueId,
      rowStart:      b.rowStart,
      cc:            b.header.cc,
      proveedor:     b.header.proveedor,
      moneda:        b.header.moneda || 'MN',
      fecha:         b.header.fecha,
      numLineas:     b.lines.length,
      autorecepcion: b.header.bit_autorecepcion === 'S',
      retenciones:   (b.header._retenciones || []).length,
      pagos:         (b.header._pagos || []).length,
    }));

    return res.json({
      ok: true,
      sessionId,
      totalBloques: blocks.length,
      totalLineas:  blocks.reduce((s, b) => s + b.lines.length, 0),
      warnings,
      bloques: preview,
    });
  } catch (err) {
    logger.error('uploadExcel error:', err);
    return res.status(500).json({ ok: false, error: err.message });
  }
}

// ── POST /api/oc/generar (SSE) ────────────────────────────────────────────────
async function generarOC(req, res) {
  const { sessionId, bloquesSeleccionados } = req.body;

  if (!sessionId)
    return res.status(400).json({ ok: false, error: 'sessionId requerido.' });

  const session = sessions.get(sessionId);
  if (!session)
    return res.status(404).json({
      ok: false,
      error: 'Sesión no encontrada o expirada. Vuelve a subir el archivo.',
    });

  let { blocks } = session;
  if (bloquesSeleccionados?.length) {
    const sel = new Set(bloquesSeleccionados.map(String));
    blocks = blocks.filter(b => sel.has(String(b.bloqueId)));
  }
  if (!blocks.length)
    return res.status(400).json({ ok: false, error: 'Sin bloques para procesar.' });

  // SSE headers
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  const send = (event, data) =>
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);

  send('inicio', { totalBloques: blocks.length, usuario: req.usuario?.usuarioKey });

  try {
    const idEk      = req.usuario?.idEk      || null;
    const usuarioKey = req.usuario?.usuarioKey || null;
    const resultado = await procesarBatch(blocks, (bloqueId, estado, datos) => {
      send('progreso', { bloqueId, estado, datos });
    }, idEk, usuarioKey);
    send('completado', resultado);
    sessions.delete(sessionId);
    try { fs.unlinkSync(session.filePath); } catch (_) {}
  } catch (err) {
    logger.error('generarOC error fatal:', err);
    send('error_fatal', { mensaje: err.message });
  }

  res.end();
}

// ── GET /api/oc/historial ────────────────────────────────────────────────────
async function getHistorial(req, res) {
  const { listarOCs } = require('../services/comprasService');
  const { cc, proveedor, numero, fecha_desde, fecha_hasta, pagina, por_pagina } = req.query;
  try {
    const resultado = await listarOCs({
      cc, proveedor, numero,
      fecha_desde, fecha_hasta,
      usuario_key: req.query.solo_mias === '1' ? req.usuario?.usuarioKey : undefined,
      pagina:     pagina     ? parseInt(pagina, 10)     : 1,
      por_pagina: por_pagina ? parseInt(por_pagina, 10) : 50,
    });
    return res.json({ ok: true, ...resultado });
  } catch (err) {
    logger.error('[ocController] getHistorial:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
}

// ── GET /api/oc/columnas ──────────────────────────────────────────────────────
function getColumnas(req, res) {
  const REQUERIDOS = new Set(['bloque','cc','proveedor','insumo','cantidad','precio','fecha_entrega']);
  const columnas = Object.entries(getColumnMap()).map(([campo, col]) => ({
    campo,
    nombreColumna: col,
    requerido: REQUERIDOS.has(campo),
  }));
  return res.json({ ok: true, columnas });
}

// ── POST /api/oc/borrador ────────────────────────────────────────────────────
// Recibe el sessionId del Excel, valida, guarda borradores y valida presupuesto.
// Reemplaza el paso de "generar directo desde Excel".
async function guardarBorradores(req, res) {
  const { sessionId, bloquesSeleccionados } = req.body;

  if (!sessionId)
    return res.status(400).json({ ok: false, error: 'sessionId requerido.' });

  const session = sessions.get(sessionId);
  if (!session)
    return res.status(404).json({ ok: false, error: 'Sesión no encontrada o expirada.' });

  let { blocks } = session;
  if (bloquesSeleccionados?.length) {
    const sel = new Set(bloquesSeleccionados.map(String)); 
    blocks = blocks.filter(b => sel.has(String(b.bloqueId)));
  } 
  if (!blocks.length)
    return res.status(400).json({ ok: false, error: 'Sin bloques seleccionados.' });

  const { guardarBorrador } = require('../services/comprasService');
  const { validarBloque }   = require('../validators/erpValidators');
  const { validarPptoBloque, getAnioValidacion } = require('../validators/pptoValidator');
  const { withTransaction } = require('../config/db');

  const usuarioKey = req.usuario?.usuarioKey || 'batch';
  const idEk       = req.usuario?.idEk       || null;
  const resultados = [];

  for (const block of blocks) {
    try { 
      // 1. Validar catálogos en Sybase y enriquecer el bloque
      const numCia = parseInt(process.env.ERP_NUM_CIA, 10) || 1;
      let bloqueValidado;
      await withTransaction(async conn => {
        bloqueValidado = await validarBloque(conn, block, numCia);
        // 2. Validar presupuesto
        const anio = 0; //await getAnioValidacion(conn);
        const pptoResult = await validarPptoBloque(
          conn, bloqueValidado.header.cc, bloqueValidado.lines, anio
        );
        bloqueValidado.lines     = pptoResult.lines;
        bloqueValidado.pptoOk    = pptoResult.pptoOk;
        bloqueValidado.pptoResumen = pptoResult.resumenPpto;
        bloqueValidado._provData = bloqueValidado._provData;
      });
      // 3. Guardar borrador en SQL Server
      const id = await guardarBorrador(
        { ...block, ...bloqueValidado }, { pptoOk: bloqueValidado.pptoOk }, usuarioKey, idEk
      );
      // Construir partidas_det completas (cantidad+precio+importe+ppto) para el frontend
      const partidasDet = bloqueValidado.lines.map(l => ({
        partida:           l.partida,
        insumo:            l.insumo,
        descripcion:       l._insumoDesc || l.descripcion || '',
        descripcion_det:   l.descripcion_det || '',
        cantidad:          l.cantidad,
        precio:            l.precio,
        importe:           l._importe ?? (l.cantidad * l.precio),
        iva:               l._iva     ?? 0,
        porcent_iva:       l.porcent_iva ?? 16,
        fecha_entrega:     l.fecha_entrega,
        unidad:            l._unidad  || l.unidad || '',
        area:              l.area     ?? null,
        cuenta:            l.cuenta   ?? null,
        ppto_status:       l.ppto_status       || 'NO_APLICA',
        ppto_disponible:   l.ppto_disponible   ?? null,
        ppto_autorizado:   l.ppto_autorizado   ?? null,
        ppto_comprometido: l.ppto_comprometido ?? null,
        ppto_msg:          l.ppto_msg          || '',
      }));
      resultados.push({
        ok: true, bloqueId: block.bloqueId, id,
        pptoOk:     bloqueValidado.pptoOk,
        partidasDet,
        cc:         bloqueValidado.header.cc,
        proveedor:  bloqueValidado.header.proveedor,
        provNombre: bloqueValidado._provData?.nombre?.trim() || '',
        total:      bloqueValidado.header.total,
        partidas:   bloqueValidado.lines.length,
        retenciones:bloqueValidado.header._retencionesEnriquecidas?.length || 0,
        autorecepcion: bloqueValidado.header.bit_autorecepcion === 'S',
      });
    } catch (err) {
      logger.error('[ocController] guardarBorrador:', err.message);
      resultados.push({ ok: false, bloqueId: block.bloqueId, error: err.message });
    }
  }

  sessions.delete(sessionId);
  try { fs.unlinkSync(session.filePath); } catch (_) {}

  return res.json({ ok: true, resultados });
}

// ── POST /api/oc/borrador/:id/revalidar ──────────────────────────────────────
async function revalidarBorrador(req, res) {
  const id = Number(req.params.id);
  const { revalidarPpto } = require('../services/comprasService');
  const { withTransaction } = require('../config/db');
  try {
    const numCia = parseInt(process.env.ERP_NUM_CIA, 10) || 1;
    let resultado;
    await withTransaction(async conn => {
      resultado = await revalidarPpto(id, conn, numCia);
    });
    return res.json({ ok: true, ...resultado });
  } catch (err) {
    logger.error('[ocController] revalidarBorrador:', err.message);
    return res.status(500).json({ ok: false, error: err.message });
  }
}

// ── POST /api/oc/borrador/:id/confirmar ──────────────────────────────────────
async function confirmarBorradorCtrl(req, res) {
  const id         = Number(req.params.id);
  const usuarioKey = req.usuario?.usuarioKey || 'batch';
  const idEk       = req.usuario?.idEk       || null;
  const { confirmarBorrador } = require('../services/comprasService');
  const { withTransaction }   = require('../config/db');
  const numCia = parseInt(process.env.ERP_NUM_CIA, 10) || 1;

  // SSE para progreso en tiempo real
  res.setHeader('Content-Type',  'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection',    'keep-alive');
  res.flushHeaders();

  const send = (event, data) =>
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);

  send('inicio', { id });
  try {
    let resultado;
    await withTransaction(async conn => {
      resultado = await confirmarBorrador(id, conn, usuarioKey, idEk, numCia);
    });
    send('completado', { ok: true, ...resultado });
  } catch (err) {
    logger.error('[ocController] confirmarBorrador:', err.message);
    send('error', { ok: false, error: err.message });
  }
  res.end();
}

// ── DELETE /api/oc/borrador/:id ───────────────────────────────────────────────
async function cancelarBorradorCtrl(req, res) {
  const id = Number(req.params.id);
  const usuarioKey = req.usuario?.usuarioKey || 'batch';
  const { cancelarBorrador } = require('../services/comprasService');
  try {
    await cancelarBorrador(id, usuarioKey);
    return res.json({ ok: true });
  } catch (err) {
    logger.error('[ocController] cancelarBorrador:', err.message);
    return res.status(400).json({ ok: false, error: err.message });
  }
}

module.exports = { uploadExcel, generarOC, getColumnas, getHistorial,
                   guardarBorradores, revalidarBorrador,
                   confirmarBorradorCtrl, cancelarBorradorCtrl };

