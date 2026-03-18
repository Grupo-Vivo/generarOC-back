/**
 * comprasService.js
 *
 * Maneja compras.ORDENES_COMPRA en SQL Server (DWH_SERVICIOS).
 *
 * FLUJO:
 *   1. guardarBorrador()   — INSERT con estatus='BORRADOR' tras subir Excel
 *   2. revalidarPpto()     — Re-valida presupuesto en Sybase sin cambiar estatus
 *   3. confirmarBorrador() — Re-valida ppto y lanza ocService si pasa
 *   4. cancelarBorrador()  — UPDATE estatus='CANCELADA'
 *   5. listarOCs()         — SELECT con filtros y paginación
 *   6. registrarOC()       — UPDATE estatus='GENERADA' (llamado por ocService)
 */

const { queryUsers, sql } = require('../config/dbUsers');
const logger = require('../utils/logger');
const MODULO = 'comprasService';

// ─────────────────────────────────────────────────────────────────────────────
// GUARDAR BORRADOR
// Llamado después del parse+validación de catálogos, antes de generar en Sybase.
// Inserta una fila por bloque con el detalle de partidas en JSON.
// ─────────────────────────────────────────────────────────────────────────────
async function guardarBorrador(bloque, pptoResult, usuarioKey, idEk = null, origen = 'EXCEL') {
  const { header, lines } = bloque;

  // Serializar partidas con resultado de presupuesto
  const partidasJson = JSON.stringify(
    lines.map(l => ({
      partida:           l.partida,
      insumo:            l.insumo,
      descripcion:       l._insumoDesc || l.descripcion || '',
      descripcion_det:   l.descripcion_det || '',
      cantidad:          l.cantidad,
      precio:            l.precio,
      importe:           l._importe || (l.cantidad * l.precio),
      iva:               l._iva     || 0,
      porcent_iva:       l.porcent_iva ?? 16,
      fecha_entrega:     l.fecha_entrega,
      unidad:            l._unidad  || l.unidad || '',
      area:              l.area     ?? null,
      cuenta:            l.cuenta   ?? null,
      frente:            l.frente   ?? null,
      partida_obra:      l.partida_obra ?? null,
      // Resultado de presupuesto
      ppto_status:       l.ppto_status       || 'NO_APLICA',
      ppto_disponible:   l.ppto_disponible   ?? null,
      ppto_autorizado:   l.ppto_autorizado   ?? null,
      ppto_comprometido: l.ppto_comprometido ?? null,
      ppto_msg:          l.ppto_msg          || '',
    }))
  );

  const rows = await queryUsers(
    `INSERT INTO compras.ORDENES_COMPRA (
       cc, proveedor, proveedor_nombre,
       fecha_oc, moneda, tipo_cambio,
       sub_total, iva, total,
       partidas, retenciones, autorecepcion,
       usuario_key, id_ek, bloque_excel,
       estatus, ppto_ok, ppto_validado_en,
       partidas_json, pagos_json, retenciones_json, bloque_json,
       libre_abordo, uso_cfdi, metodo_pago, almacen,
       bit_autorecepcion, almacen_autorecepcion,
       comentarios, origen, fecha_registro
     ) OUTPUT INSERTED.id VALUES (
       @cc, @proveedor, @provNombre,
       @fechaOc, @moneda, @tipoCambio,
       @subTotal, @iva, @total,
       @partidas, @retenciones, @autorecepcion,
       @usuarioKey, @idEk, @bloqueExcel,
       'BORRADOR', @pptoOk, GETDATE(),
       @partidasJson, @pagosJson, @retencionesJson, @bloqueJson,
       @libreAbordo, @usoCfdi, @metodoPago, @almacen,
       @bitAutorecepcion, @almacenAutorecepcion,
       @comentarios, @origen, GETDATE()
     )`,
    {
      cc:                   { type: sql.NVarChar(10),  value: String(header.cc) },
      proveedor:            { type: sql.Int,            value: Number(header.proveedor) },
      provNombre:           { type: sql.NVarChar(200),  value: bloque._provData?.nombre?.trim() || null },
      fechaOc:              { type: sql.Date,           value: header.fecha ? new Date(header.fecha) : new Date() },
      moneda:               { type: sql.NVarChar(5),    value: header.moneda    || 'MN' },
      tipoCambio:           { type: sql.Decimal(18,6),  value: Number(header.tipoCambio) || 1 },
      subTotal:             { type: sql.Decimal(18,4),  value: Number(header.subTotal)   || 0 },
      iva:                  { type: sql.Decimal(18,4),  value: Number(header.iva)         || 0 },
      total:                { type: sql.Decimal(18,4),  value: Number(header.total)       || 0 },
      partidas:             { type: sql.Int,            value: lines.length },
      retenciones:          { type: sql.Int,            value: header._retencionesEnriquecidas?.length || 0 },
      autorecepcion:        { type: sql.Bit,            value: header.bit_autorecepcion === 'S' ? 1 : 0 },
      usuarioKey:           { type: sql.NVarChar(20),   value: String(usuarioKey) },
      idEk:                 { type: sql.Int,            value: idEk ? Number(idEk) : null },
      bloqueExcel:          { type: sql.NVarChar(100),  value: bloque.bloqueId ? String(bloque.bloqueId) : null },
      pptoOk:               { type: sql.Bit,            value: pptoResult.pptoOk ? 1 : 0 },
      partidasJson:         { type: sql.NVarChar(sql.MAX), value: partidasJson },
      pagosJson:            { type: sql.NVarChar(sql.MAX), value: JSON.stringify(header._pagos || []) },
      retencionesJson:      { type: sql.NVarChar(sql.MAX), value: JSON.stringify(header._retencionesEnriquecidas || []) },
      bloqueJson:           { type: sql.NVarChar(sql.MAX), value: JSON.stringify(bloque) },
      libreAbordo:          { type: sql.Int,            value: header.libre_abordo ? Number(header.libre_abordo) : null },
      usoCfdi:              { type: sql.NVarChar(10),   value: header.uso_cfdi     || null },
      metodoPago:           { type: sql.NVarChar(10),   value: header.metodo_pago  || null },
      almacen:              { type: sql.Int,            value: header.almacen      ? Number(header.almacen) : null },
      bitAutorecepcion:     { type: sql.NChar(1),       value: header.bit_autorecepcion === 'S' ? 'S' : null },
      almacenAutorecepcion: { type: sql.Int,            value: header.almacen_autorecepcion ? Number(header.almacen_autorecepcion) : null },
      comentarios:          { type: sql.NVarChar(500),  value: header.comentarios  || null },
      origen:               { type: sql.NVarChar(15),   value: origen },
    }
  );

  const id = rows[0]?.id; 
  logger.info(`[${MODULO}] Borrador guardado id=${id} CC=${header.cc} ppto_ok=${pptoResult.pptoOk}`);
  return id;
}

// ─────────────────────────────────────────────────────────────────────────────
// RE-VALIDAR PRESUPUESTO (sin cambiar estatus)
// Llamado desde el endpoint de re-validación y desde confirmarBorrador.
// ─────────────────────────────────────────────────────────────────────────────
async function revalidarPpto(id, conn, numCia) {
  const { validarPptoBloque, getAnioValidacion } = require('../validators/pptoValidator');

  // Leer bloque_json guardado
  const rows = await queryUsers(
    `SELECT bloque_json, estatus FROM compras.ORDENES_COMPRA WHERE id = @id`,
    { id: { type: sql.Int, value: Number(id) } }
  );
  if (!rows.length) throw new Error(`Borrador id=${id} no encontrado.`);
  if (rows[0].estatus === 'GENERADA')
    throw new Error(`OC id=${id} ya fue generada — no se puede re-validar.`);

  const bloque = JSON.parse(rows[0].bloque_json);
  const anio   = 0; // await getAnioValidacion(conn);
  const { lines, pptoOk, resumenPpto } = await validarPptoBloque(
    conn, bloque.header.cc, bloque.lines, anio
  );

  // Actualizar partidas_json y ppto_ok en SQL Server
  const partidasActualizadas = JSON.parse(
    await queryUsers(`SELECT partidas_json FROM compras.ORDENES_COMPRA WHERE id = @id`,
      { id: { type: sql.Int, value: Number(id) } })
    .then(r => r[0]?.partidas_json || '[]')
  );

  // Merge ppto result into existing partidas
  const partidasMerged = partidasActualizadas.map(p => {
    const res = resumenPpto.find(r => r.partida === p.partida) || {};
    return {
      ...p,
      ppto_status:       res.ppto_status       || p.ppto_status,
      ppto_disponible:   res.ppto_disponible   ?? p.ppto_disponible,
      ppto_autorizado:   res.ppto_autorizado   ?? p.ppto_autorizado,
      ppto_comprometido: res.ppto_comprometido ?? p.ppto_comprometido,
      ppto_msg:          res.ppto_msg          || p.ppto_msg,
    };
  });

  await queryUsers(
    `UPDATE compras.ORDENES_COMPRA
     SET ppto_ok = @pptoOk, ppto_validado_en = GETDATE(),
         partidas_json = @partidasJson
     WHERE id = @id`,
    {
      id:           { type: sql.Int,               value: Number(id) },
      pptoOk:       { type: sql.Bit,               value: pptoOk ? 1 : 0 },
      partidasJson: { type: sql.NVarChar(sql.MAX),  value: JSON.stringify(partidasMerged) },
    }
  );

  logger.info(`[${MODULO}] Re-validación ppto id=${id}: ${pptoOk ? 'OK' : 'BLOQUEADO'}`);
  return { pptoOk, resumenPpto, partidas: partidasMerged };
}

// ─────────────────────────────────────────────────────────────────────────────
// CONFIRMAR BORRADOR → Genera en Sybase
// 1. Re-valida presupuesto (siempre, datos frescos de Sybase)
// 2. Si pasa → llama procesarBatch con el bloque de SQL Server
// 3. UPDATE estatus='GENERADA' o 'ERROR'
// ─────────────────────────────────────────────────────────────────────────────
async function confirmarBorrador(id, conn, usuarioKey, idEk, numCia) {
  const { procesarBatch } = require('./ocService');
  const { getAnioValidacion, validarPptoBloque } = require('../validators/pptoValidator');

  // Leer borrador
  const rows = await queryUsers(
    `SELECT bloque_json, estatus, usuario_key FROM compras.ORDENES_COMPRA WHERE id = @id`,
    { id: { type: sql.Int, value: Number(id) } }
  );
  if (!rows.length) throw new Error(`Borrador id=${id} no encontrado.`);
  const { estatus, bloque_json } = rows[0];
  if (estatus !== 'BORRADOR')
    throw new Error(`El registro id=${id} tiene estatus "${estatus}" — solo se pueden confirmar BORRADORES.`);

  const bloque = JSON.parse(bloque_json);

  // Re-validar presupuesto con datos frescos de Sybase
  const anio = 0; //await getAnioValidacion(conn);
  const { lines: linesConPpto, pptoOk, resumenPpto } = await validarPptoBloque(
    conn, bloque.header.cc, bloque.lines, anio
  );

  // Actualizar ppto en SQL Server (independientemente del resultado)
  const partidasMerged = linesConPpto.map(l => ({
    partida:           l.partida,
    insumo:            l.insumo,
    descripcion:       l._insumoDesc || '',
    descripcion_det:   l.descripcion_det || '',
    cantidad:          l.cantidad,
    precio:            l.precio,
    importe:           l._importe || (l.cantidad * l.precio),
    iva:               l._iva || 0,
    porcent_iva:       l.porcent_iva ?? 16,
    fecha_entrega:     l.fecha_entrega,
    unidad:            l._unidad || l.unidad || '',
    area:              l.area ?? null,
    cuenta:            l.cuenta ?? null,
    frente:            l.frente ?? null,
    partida_obra:      l.partida_obra ?? null,
    ppto_status:       l.ppto_status       || 'NO_APLICA',
    ppto_disponible:   l.ppto_disponible   ?? null,
    ppto_autorizado:   l.ppto_autorizado   ?? null,
    ppto_comprometido: l.ppto_comprometido ?? null,
    ppto_msg:          l.ppto_msg          || '',
  }));

  await queryUsers(
    `UPDATE compras.ORDENES_COMPRA
     SET ppto_ok = @pptoOk, ppto_validado_en = GETDATE(), partidas_json = @partidasJson
     WHERE id = @id`,
    {
      id:           { type: sql.Int,              value: Number(id) },
      pptoOk:       { type: sql.Bit,              value: pptoOk ? 1 : 0 },
      partidasJson: { type: sql.NVarChar(sql.MAX), value: JSON.stringify(partidasMerged) },
    }
  );

  // Bloquear si presupuesto insuficiente
  if (!pptoOk) {
    const bloqueadas = resumenPpto.filter(r => r.bloqueado);
    throw new Error(
      `Presupuesto insuficiente en ${bloqueadas.length} línea(s). ` +
      `Agrega aditiva y vuelve a confirmar. ` +
      `Detalle: ${bloqueadas.map(b => `Insumo ${b.insumo}: ${b.ppto_msg}`).join(' | ')}`
    );
  }

  // Generar en Sybase usando el bloque enriquecido con las lines actualizadas
  const bloqueActualizado = { ...bloque, lines: linesConPpto };
  let resultado;
  try {
    const resultados = await procesarBatch(
      [bloqueActualizado], () => {}, idEk, usuarioKey
    );
    if (resultados.errores.length > 0) {
      const msg = resultados.errores[0].mensaje;
      await queryUsers(
        `UPDATE compras.ORDENES_COMPRA SET estatus = 'ERROR', error_msg = @msg WHERE id = @id`,
        {
          id:  { type: sql.Int,          value: Number(id) },
          msg: { type: sql.NVarChar(500), value: msg.substring(0, 500) },
        }
      );
      throw new Error(msg);
    }
    resultado = resultados.exitosos[0];
  } catch (err) {
    if (!err.message.includes('Presupuesto')) {
      await queryUsers(
        `UPDATE compras.ORDENES_COMPRA SET estatus = 'ERROR', error_msg = @msg WHERE id = @id`,
        {
          id:  { type: sql.Int,          value: Number(id) },
          msg: { type: sql.NVarChar(500), value: err.message.substring(0, 500) },
        }
      );
    }
    throw err;
  }

  // Marcar como GENERADA
  await queryUsers(
    `UPDATE compras.ORDENES_COMPRA
     SET estatus = 'GENERADA', numero_erp = @numero,
         fecha_generada = GETDATE(), error_msg = NULL
     WHERE id = @id`,
    {
      id:     { type: sql.Int, value: Number(id) },
      numero: { type: sql.Int, value: Number(resultado.numero) },
    }
  );

  logger.info(`[${MODULO}] Borrador id=${id} confirmado → OC ${resultado.cc}-${resultado.numero}`);
  return resultado;
}

// ─────────────────────────────────────────────────────────────────────────────
// CANCELAR BORRADOR
// ─────────────────────────────────────────────────────────────────────────────
async function cancelarBorrador(id, usuarioKey) {
  const rows = await queryUsers(
    `SELECT estatus, usuario_key FROM compras.ORDENES_COMPRA WHERE id = @id`,
    { id: { type: sql.Int, value: Number(id) } }
  );
  if (!rows.length) throw new Error(`Borrador id=${id} no encontrado.`);
  if (rows[0].estatus === 'GENERADA')
    throw new Error('No se puede cancelar una OC ya generada.');

  await queryUsers(
    `UPDATE compras.ORDENES_COMPRA SET estatus = 'CANCELADA' WHERE id = @id`,
    { id: { type: sql.Int, value: Number(id) } }
  );
  logger.info(`[${MODULO}] Borrador id=${id} cancelado por ${usuarioKey}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// REGISTRAR OC GENERADA (llamado por ocService tras INSERT exitoso en Sybase)
// En el nuevo flujo esto ya no hace INSERT — solo actualiza si el registro existe,
// o inserta si vino de un flujo legacy sin borrador.
// ─────────────────────────────────────────────────────────────────────────────
async function registrarOC(resultado, usuarioKey, idEk = null) {
  const { cc, numero, proveedor, provNombre, partidas, total,
          retenciones, autorecepcion, bloqueId, moneda, tipoCambio,
          subTotal, iva, fechaOc } = resultado;
  try {
    // ¿Ya existe un borrador para este bloque?
    const existente = await queryUsers(
      `SELECT id FROM compras.ORDENES_COMPRA
       WHERE bloque_excel = @bloqueId AND estatus = 'BORRADOR'`,
      { bloqueId: { type: sql.NVarChar(100), value: bloqueId ? String(bloqueId) : '' } }
    );
    if (existente.length) {
      // confirmarBorrador ya hizo el UPDATE — nada más que hacer
      return;
    }
    // Flujo legacy (sin borrador previo) → INSERT directo como GENERADA
    const existe = await queryUsers(
      `SELECT 1 FROM compras.ORDENES_COMPRA WHERE cc = @cc AND numero_erp = @numero`,
      {
        cc:     { type: sql.NVarChar(10), value: String(cc) },
        numero: { type: sql.Int,          value: Number(numero) },
      }
    );
    if (existe.length) return;

    await queryUsers(
      `INSERT INTO compras.ORDENES_COMPRA
         (cc, numero_erp, proveedor, proveedor_nombre,
          fecha_oc, moneda, tipo_cambio, sub_total, iva, total,
          partidas, retenciones, autorecepcion, st_impresa,
          usuario_key, id_ek, bloque_excel,
          estatus, fecha_generada, origen, fecha_registro)
       VALUES
         (@cc, @numero, @proveedor, @provNombre,
          @fechaOc, @moneda, @tipoCambio, @subTotal, @iva, @total,
          @partidas, @retenciones, @autorecepcion, 'I',
          @usuarioKey, @idEk, @bloqueExcel,
          'GENERADA', GETDATE(), 'EXCEL', GETDATE())`,
      {
        cc:           { type: sql.NVarChar(10),  value: String(cc) },
        numero:       { type: sql.Int,            value: Number(numero) },
        proveedor:    { type: sql.Int,            value: Number(proveedor) },
        provNombre:   { type: sql.NVarChar(200),  value: provNombre || null },
        fechaOc:      { type: sql.Date,           value: fechaOc ? new Date(fechaOc) : new Date() },
        moneda:       { type: sql.NVarChar(5),    value: moneda    || 'MN' },
        tipoCambio:   { type: sql.Decimal(18,6),  value: Number(tipoCambio) || 1 },
        subTotal:     { type: sql.Decimal(18,4),  value: Number(subTotal)   || 0 },
        iva:          { type: sql.Decimal(18,4),  value: Number(iva)        || 0 },
        total:        { type: sql.Decimal(18,4),  value: Number(total)      || 0 },
        partidas:     { type: sql.Int,            value: Number(partidas)   || 0 },
        retenciones:  { type: sql.Int,            value: Number(retenciones)|| 0 },
        autorecepcion:{ type: sql.Bit,            value: autorecepcion ? 1 : 0 },
        usuarioKey:   { type: sql.NVarChar(20),   value: String(usuarioKey) },
        idEk:         { type: sql.Int,            value: idEk ? Number(idEk) : null },
        bloqueExcel:  { type: sql.NVarChar(100),  value: bloqueId ? String(bloqueId) : null },
      }
    );
    logger.info(`[${MODULO}] OC registrada (legacy): CC=${cc} N°=${numero}`);
  } catch (err) {
    logger.error(`[${MODULO}] registrarOC falló (no crítico): ${err.message}`, { stack: err.stack });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// LISTAR OCs con filtros y paginación
// ─────────────────────────────────────────────────────────────────────────────
async function listarOCs(filtros = {}) {
  const {
    cc, proveedor, numero, fecha_desde, fecha_hasta,
    usuario_key, estatus,
    pagina = 1, por_pagina = 50,
  } = filtros;

  const where  = ['1=1'];
  const params = {};

  if (cc)          { where.push('cc = @cc');                   params.cc          = { type: sql.NVarChar(10), value: String(cc).trim() }; }
  if (proveedor)   { where.push('proveedor = @proveedor');     params.proveedor   = { type: sql.Int,          value: Number(proveedor) }; }
  if (numero)      { where.push('numero_erp = @numero');       params.numero      = { type: sql.Int,          value: Number(numero) }; }
  if (fecha_desde) { where.push('fecha_oc >= @fecha_desde');   params.fecha_desde = { type: sql.Date,         value: new Date(fecha_desde) }; }
  if (fecha_hasta) { where.push('fecha_oc <= @fecha_hasta');   params.fecha_hasta = { type: sql.Date,         value: new Date(fecha_hasta) }; }
  if (usuario_key) { where.push('usuario_key = @usuario_key'); params.usuario_key = { type: sql.NVarChar(20), value: String(usuario_key).trim() }; }
  if (estatus)     { where.push('estatus = @estatus');         params.estatus     = { type: sql.NVarChar(15), value: String(estatus) }; }
  if (filtros.origen) { where.push('origen = @origen');           params.origen      = { type: sql.NVarChar(15), value: String(filtros.origen) }; }

  const offset = (pagina - 1) * por_pagina;

  const totalRows = await queryUsers(
    `SELECT COUNT(*) AS total FROM compras.ORDENES_COMPRA WHERE ${where.join(' AND ')}`,
    params
  );
  const total = totalRows[0]?.total || 0;

  const rows = await queryUsers(
    `SELECT id, cc, numero_erp, proveedor, proveedor_nombre,
            fecha_oc, moneda, tipo_cambio, sub_total, iva, total,
            partidas, retenciones, autorecepcion, estatus,
            ppto_ok, ppto_validado_en,
            partidas_json,
            usuario_key, id_ek, bloque_excel, origen, fecha_registro, fecha_generada,
            error_msg
     FROM compras.ORDENES_COMPRA
     WHERE ${where.join(' AND ')}
     ORDER BY fecha_registro DESC
     OFFSET @offset ROWS FETCH NEXT @porPagina ROWS ONLY`,
    {
      ...params,
      offset:    { type: sql.Int, value: offset },
      porPagina: { type: sql.Int, value: por_pagina },
    }
  );

  // Parsear partidas_json para cada fila
  const registros = rows.map(r => ({
    ...r,
    partidas_json: undefined,
    partidas_det: r.partidas_json ? (() => { try { return JSON.parse(r.partidas_json); } catch { return []; } })() : [],
  }));

  return { total, pagina, por_pagina, paginas: Math.ceil(total / por_pagina), registros };
}

module.exports = { guardarBorrador, revalidarPpto, confirmarBorrador, cancelarBorrador, registrarOC, listarOCs };
