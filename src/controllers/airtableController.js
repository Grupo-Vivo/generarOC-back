/**
 * airtableController.js
 *
 * Endpoint para recibir OCs desde Airtable y generarlas directamente en Sybase.
 *
 * POST /api/airtable/oc
 * Header: X-Api-Key: <AIRTABLE_API_KEY>
 *
 * ── FLUJO ────────────────────────────────────────────────────────────────────
 *   1. Validar API Key (middleware)
 *   2. Validar estructura del JSON de entrada
 *   3. Transformar al formato interno de bloque
 *   4. Validar catálogos en Sybase (erpValidators)
 *   5. Validar presupuesto (pptoValidator)
 *      └ Si falla → 422 con detalle por línea (NO guarda nada)
 *   6. Generar OC en Sybase (ocService)
 *   7. Guardar en compras.ORDENES_COMPRA con origen='AIRTABLE'
 *   8. Responder 200 con número de OC generada
 *
 * ── JSON DE ENTRADA (desde Airtable) ─────────────────────────────────────────
 * {
 *   "cc":          "M00",
 *   "proveedor":   7347,
 *   "fecha":       "2026-03-18",          // opcional, default hoy
 *   "moneda":      "MXN",                 // opcional, default MXN
 *   "comentarios": "OC generada desde Airtable",
 *   "uso_cfdi":    "G03",                 // opcional
 *   "metodo_pago": "PPD",                 // opcional
 *   "libre_abordo": 2,                    // opcional
 *   "almacen":     91,                    // opcional
 *   "bit_autorecepcion": "S",             // opcional
 *   "almacen_autorecepcion": 91,          // requerido si bit_autorecepcion=S
 *   "empleado_autorecepcion": 131,        // opcional
 *   "pagos": [                            // opcional
 *     { "dias_pago": 0, "porcentaje": 100, "comentarios": "" }
 *   ],
 *   "retenciones": [                      // opcional
 *     { "id_cpto": 8, "porc": 10 }
 *   ],
 *   "partidas": [                         // requerido, mínimo 1
 *     {
 *       "insumo":        6740002,
 *       "descripcion_det": "Entrega electrónica",  // opcional
 *       "cantidad":      1,
 *       "precio":        790,
 *       "fecha_entrega": "2026-03-20",    // requerido
 *       "porcent_iva":   0,               // opcional, default 16
 *       "area":          null,            // opcional
 *       "cuenta":        null,            // opcional
 *       "frente":        null,            // opcional
 *       "partida_obra":  null             // opcional
 *     }
 *   ]
 * }
 *
 * ── RESPUESTA OK (200) ───────────────────────────────────────────────────────
 * {
 *   "ok": true,
 *   "cc": "M00",
 *   "numero": 20612,
 *   "numero_formateado": "M00-020612",
 *   "total": 790.00,
 *   "partidas": 1,
 *   "ppto_resumen": [ { "partida":1,"insumo":6740002,"ppto_status":"OK",... } ]
 * }
 *
 * ── RESPUESTA ERROR PPTO (422) ───────────────────────────────────────────────
 * {
 *   "ok": false,
 *   "error": "Presupuesto insuficiente en 1 línea(s)",
 *   "detalle_ppto": [
 *     { "partida":1,"insumo":6740002,"ppto_status":"EXCEDIDO","ppto_msg":"..." }
 *   ]
 * }
 *
 * ── RESPUESTA ERROR VALIDACIÓN (422) ─────────────────────────────────────────
 * {
 *   "ok": false,
 *   "error": "Proveedor 9999 no existe."
 * }
 */

const logger = require('../utils/logger');
const MODULO = 'airtableController';

// ─── Validación de estructura del JSON entrante ───────────────────────────────
function validarEstructura(body) {
  const errores = [];

  if (!body.cc)                               errores.push('"cc" es requerido.');
  if (!body.proveedor)                        errores.push('"proveedor" es requerido.');
  if (!Array.isArray(body.partidas) || !body.partidas.length)
                                              errores.push('"partidas" debe ser un arreglo con al menos 1 elemento.');

  (body.partidas || []).forEach((p, i) => {
    const n = i + 1;
    if (!p.insumo)                            errores.push(`Partida ${n}: "insumo" es requerido.`);
    if (!p.cantidad || Number(p.cantidad) <= 0)
                                              errores.push(`Partida ${n}: "cantidad" debe ser mayor a 0.`);
    if (p.precio === undefined || p.precio === null || Number(p.precio) < 0)
                                              errores.push(`Partida ${n}: "precio" es requerido y debe ser ≥ 0.`);
    if (!p.fecha_entrega)                     errores.push(`Partida ${n}: "fecha_entrega" es requerida (YYYY-MM-DD).`);
  });

  if (body.bit_autorecepcion === 'S' && !body.almacen_autorecepcion)
    errores.push('"almacen_autorecepcion" es requerido cuando bit_autorecepcion = "S".');

  return errores;
}

// ─── Transforma el JSON de Airtable al formato interno de bloque ──────────────
function airtableABloque(body, idx = 1) {
  const hoy    = new Date().toISOString().slice(0, 10);
  const bloqueId = `AIRTABLE-${Date.now()}-${idx}`;

  // Mapear pagos
  const _pagos = (body.pagos || []).map((pg, i) => ({
    orden:       i + 1,
    dias_pago:   Number(pg.dias_pago   ?? 0),
    porcentaje:  Number(pg.porcentaje  ?? 100),
    importe:     0,   // se calculará con el total real
    comentarios: pg.comentarios || '',
  }));

  // Si no vienen pagos, crear uno por defecto: 100% a 0 días
  if (!_pagos.length) {
    _pagos.push({ orden: 1, dias_pago: 0, porcentaje: 100, importe: 0, comentarios: '' });
  }

  // Mapear retenciones
  const _retenciones = (body.retenciones || []).map((r, i) => ({
    id_cpto: r.id_cpto,
    porc:    r.porc || null,
    orden:   i + 1,
  }));

  // Construir lines
  const lines = (body.partidas || []).map((p, i) => ({
    _excelRow:       i + 2,  // fila ficticia para mensajes de error
    insumo:          Number(p.insumo),
    descripcion_det: p.descripcion_det || '',
    cantidad:        Number(p.cantidad),
    precio:          Number(p.precio),
    fecha_entrega:   p.fecha_entrega,
    porcent_iva:     p.porcent_iva !== undefined ? Number(p.porcent_iva) : 16,
    area:            p.area    ?? null,
    cuenta:          p.cuenta  ?? null,
    frente:          p.frente  ?? null,
    partida_obra:    p.partida_obra ?? null,
  }));

  const header = {
    cc:                     String(body.cc).trim(),
    proveedor:              Number(body.proveedor),
    fecha:                  body.fecha || hoy,
    moneda:                 body.moneda || 'MXN',
    tipo_cambio:            body.tipo_cambio || null,
    comentarios:            body.comentarios || '',
    libre_abordo:           body.libre_abordo ?? 2,
    almacen:                body.almacen ?? null,
    uso_cfdi:               body.uso_cfdi || null,
    metodo_pago:            body.metodo_pago || null,
    bienes_servicios:       body.bienes_servicios || 'B',
    concepto_factura:       body.concepto_factura || null,
    id_lugar:               body.id_lugar ?? null,
    bit_autorecepcion:      body.bit_autorecepcion || null,
    almacen_autorecepcion:  body.almacen_autorecepcion ?? null,
    empleado_autorecepcion: body.empleado_autorecepcion ?? null,
    _pagos,
    _retenciones,
  };

  return { bloqueId, header, lines, rowStart: 2 };
}

// ─── HANDLER PRINCIPAL ────────────────────────────────────────────────────────
async function recibirDeAirtable(req, res) {
  const body = req.body;

  // 1. Validar estructura
  const erroresEstructura = validarEstructura(body);
  if (erroresEstructura.length) {
    return res.status(422).json({
      ok:     false,
      error:  'JSON inválido',
      detalle: erroresEstructura,
    });
  }

  const { withTransaction } = require('../config/db');
  const { validarBloque }   = require('../validators/erpValidators');
  const { validarPptoBloque, getAnioValidacion } = require('../validators/pptoValidator');
  const { procesarBatch }   = require('../services/ocService');
  const { guardarBorrador } = require('../services/comprasService');
  const { queryUsers, sql } = require('../config/dbUsers');

  const numCia = parseInt(process.env.ERP_NUM_CIA, 10) || 1;
  const bloque = airtableABloque(body);

  logger.info(`[${MODULO}] Solicitud recibida — CC=${bloque.header.cc} proveedor=${bloque.header.proveedor} partidas=${bloque.lines.length}`);

  let bloqueValidado, pptoResult;

  try {
    // 2. Validar catálogos + presupuesto dentro de una conexión Sybase
    await withTransaction(async conn => {
      bloqueValidado = await validarBloque(conn, bloque, numCia);

      // 3. Validar presupuesto
      const anio = await getAnioValidacion(conn);
      pptoResult  = await validarPptoBloque(
        conn, bloqueValidado.header.cc, bloqueValidado.lines, anio
      );
      bloqueValidado.lines    = pptoResult.lines;
      bloqueValidado.pptoOk   = pptoResult.pptoOk;
      bloqueValidado._provData = bloqueValidado._provData;
    });
  } catch (err) {
    // Error de catálogo (proveedor inexistente, insumo cancelado, etc.)
    logger.warn(`[${MODULO}] Error de validación: ${err.message}`);
    return res.status(422).json({ ok: false, error: err.message });
  }

  // 4. Bloquear si presupuesto insuficiente → 422 con detalle por línea
  if (!pptoResult.pptoOk) {
    const bloqueadas = pptoResult.resumenPpto.filter(r => r.bloqueado);
    logger.warn(`[${MODULO}] Presupuesto insuficiente — ${bloqueadas.length} línea(s) bloqueadas`);
    return res.status(422).json({
      ok:           false,
      error:        `Presupuesto insuficiente en ${bloqueadas.length} línea(s). Agrega una aditiva en Enkontrol e intenta de nuevo.`,
      detalle_ppto: pptoResult.resumenPpto.map(r => ({
        partida:           r.partida,
        insumo:            r.insumo,
        descripcion:       r.descripcion || '',
        ppto_status:       r.ppto_status,
        ppto_disponible:   r.ppto_disponible,
        ppto_autorizado:   r.ppto_autorizado,
        ppto_comprometido: r.ppto_comprometido,
        ppto_msg:          r.ppto_msg,
        bloqueado:         r.bloqueado,
      })),
    });
  }

  // 5. Generar OC en Sybase
  let resultado;
  try {
    const resultados = await procesarBatch(
      [{ ...bloque, ...bloqueValidado }],
      () => {},
      null,          // idEk — Airtable no tiene empleado del ERP
      'airtable'     // usuarioKey para el historial
    );

    if (resultados.errores.length > 0) {
      throw new Error(resultados.errores[0].mensaje);
    }
    resultado = resultados.exitosos[0];
  } catch (err) {
    logger.error(`[${MODULO}] Error al generar en Sybase: ${err.message}`, { stack: err.stack });
    return res.status(500).json({ ok: false, error: `Error al generar OC en ERP: ${err.message}` });
  }

  // 6. Guardar en compras.ORDENES_COMPRA con origen='AIRTABLE'
  try {
    const partidasDet = bloqueValidado.lines.map(l => ({
      partida:           l.partida,
      insumo:            l.insumo,
      descripcion:       l._insumoDesc || '',
      descripcion_det:   l.descripcion_det || '',
      cantidad:          l.cantidad,
      precio:            l.precio,
      importe:           l._importe ?? (l.cantidad * l.precio),
      iva:               l._iva     ?? 0,
      porcent_iva:       l.porcent_iva ?? 16,
      fecha_entrega:     l.fecha_entrega,
      unidad:            l._unidad  || '',
      area:              l.area     ?? null,
      cuenta:            l.cuenta   ?? null,
      ppto_status:       l.ppto_status       || 'OK',
      ppto_disponible:   l.ppto_disponible   ?? null,
      ppto_autorizado:   l.ppto_autorizado   ?? null,
      ppto_comprometido: l.ppto_comprometido ?? null,
      ppto_msg:          l.ppto_msg          || '',
    }));

    // Enriquecer bloque con resultado para guardarBorrador
    const bloqueParaGuardar = {
      ...bloque,
      ...bloqueValidado,
      lines: bloqueValidado.lines,
      _provData: bloqueValidado._provData,
    };

    // INSERT directo como GENERADA (omite paso de borrador)
    await queryUsers(
      `INSERT INTO compras.ORDENES_COMPRA (
         cc, numero_erp, proveedor, proveedor_nombre,
         fecha_oc, moneda, tipo_cambio, sub_total, iva, total,
         partidas, retenciones, autorecepcion, st_impresa,
         usuario_key, id_ek, bloque_excel,
         estatus, fecha_generada, ppto_ok, ppto_validado_en,
         partidas_json, pagos_json, retenciones_json, bloque_json,
         libre_abordo, uso_cfdi, metodo_pago, almacen,
         bit_autorecepcion, almacen_autorecepcion, comentarios,
         origen, fecha_registro
       ) VALUES (
         @cc, @numero, @proveedor, @provNombre,
         @fechaOc, @moneda, @tipoCambio, @subTotal, @iva, @total,
         @partidas, @retenciones, @autorecepcion, 'I',
         'airtable', NULL, NULL,
         'GENERADA', GETDATE(), 1, GETDATE(),
         @partidasJson, @pagosJson, @retencionesJson, @bloqueJson,
         @libreAbordo, @usoCfdi, @metodoPago, @almacen,
         @bitAutorecepcion, @almacenAutorecepcion, @comentarios,
         'AIRTABLE', GETDATE()
       )`,
      {
        cc:                   { type: sql.NVarChar(10),  value: String(resultado.cc) },
        numero:               { type: sql.Int,            value: Number(resultado.numero) },
        proveedor:            { type: sql.Int,            value: Number(bloqueValidado.header.proveedor) },
        provNombre:           { type: sql.NVarChar(200),  value: bloqueValidado._provData?.nombre?.trim() || null },
        fechaOc:              { type: sql.Date,           value: new Date(bloqueValidado.header.fecha || new Date()) },
        moneda:               { type: sql.NVarChar(5),    value: bloqueValidado.header.moneda    || 'MXN' },
        tipoCambio:           { type: sql.Decimal(18,6),  value: Number(bloqueValidado.header.tipoCambio) || 1 },
        subTotal:             { type: sql.Decimal(18,4),  value: Number(bloqueValidado.header.subTotal)   || 0 },
        iva:                  { type: sql.Decimal(18,4),  value: Number(bloqueValidado.header.iva)         || 0 },
        total:                { type: sql.Decimal(18,4),  value: Number(bloqueValidado.header.total)       || 0 },
        partidas:             { type: sql.Int,            value: bloqueValidado.lines.length },
        retenciones:          { type: sql.Int,            value: bloqueValidado.header._retencionesEnriquecidas?.length || 0 },
        autorecepcion:        { type: sql.Bit,            value: bloqueValidado.header.bit_autorecepcion === 'S' ? 1 : 0 },
        partidasJson:         { type: sql.NVarChar(sql.MAX), value: JSON.stringify(partidasDet) },
        pagosJson:            { type: sql.NVarChar(sql.MAX), value: JSON.stringify(bloqueValidado.header._pagos || []) },
        retencionesJson:      { type: sql.NVarChar(sql.MAX), value: JSON.stringify(bloqueValidado.header._retencionesEnriquecidas || []) },
        bloqueJson:           { type: sql.NVarChar(sql.MAX), value: JSON.stringify(bloqueParaGuardar) },
        libreAbordo:          { type: sql.Int,            value: bloqueValidado.header.libre_abordo ? Number(bloqueValidado.header.libre_abordo) : null },
        usoCfdi:              { type: sql.NVarChar(10),   value: bloqueValidado.header.uso_cfdi     || null },
        metodoPago:           { type: sql.NVarChar(10),   value: bloqueValidado.header.metodo_pago  || null },
        almacen:              { type: sql.Int,            value: bloqueValidado.header.almacen      ? Number(bloqueValidado.header.almacen) : null },
        bitAutorecepcion:     { type: sql.NChar(1),       value: bloqueValidado.header.bit_autorecepcion === 'S' ? 'S' : null },
        almacenAutorecepcion: { type: sql.Int,            value: bloqueValidado.header.almacen_autorecepcion ? Number(bloqueValidado.header.almacen_autorecepcion) : null },
        comentarios:          { type: sql.NVarChar(500),  value: (bloqueValidado.header.comentarios || '').substring(0, 500) },
      }
    );

    logger.info(`[${MODULO}] OC registrada en historial: CC=${resultado.cc} N°=${resultado.numero} origen=AIRTABLE`);
  } catch (err) {
    // No crítico — la OC ya quedó en Sybase
    logger.error(`[${MODULO}] No se pudo registrar en historial (no crítico): ${err.message}`, { stack: err.stack });
  }

  // 7. Responder con éxito
  return res.status(200).json({
    ok:                 true,
    cc:                 resultado.cc,
    numero:             resultado.numero,
    numero_formateado:  `${resultado.cc}-${String(resultado.numero).padStart(6, '0')}`,
    total:              resultado.total,
    partidas:           resultado.partidas,
    ppto_resumen:       pptoResult.resumenPpto.map(r => ({
      partida:         r.partida,
      insumo:          r.insumo,
      ppto_status:     r.ppto_status,
      ppto_disponible: r.ppto_disponible,
      ppto_msg:        r.ppto_msg,
    })),
  });
}

module.exports = { recibirDeAirtable };
