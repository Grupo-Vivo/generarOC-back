/**
 * ocService.js
 *
 * Generación de OC en Enkontrol.
 * INSERTs verificados contra CREATE TABLE reales.
 *
 * ── NUEVO vs versión anterior ────────────────────────────────────────────────
 * insertarPagos()         — INSERT so_orden_compra_pago (forma de pago)
 * insertarRetenciones()   — INSERT so_ordenc_retenciones
 * marcarImpresa()         — UPDATE so_orden_compra SET st_impresa='I'
 * ejecutarAutorecepcion() — Autorecepción de insumos no inventariables al imprimir
 *
 * insertarCabeceraOC: ahora incluye almacen, uso_cfdi, cfd_metodo_pago_sat,
 *   concepto_factura, id_lugar, bit_autorecepcion, almacen_autorecepcion,
 *   empleado_autorecepcion (campos del INSERT real del ERP)
 *
 * insertarPartidasOC: ahora incluye acum_ant, max_orig, max_ppto,
 *   frente, partida_obra, cant_canc, imp_canc, fecha_recibido
 */
const logger = require('../utils/logger');
const { validarBloque } = require('../validators/erpValidators');
const { registrarOC }   = require('./comprasService');
const dayjs  = require('dayjs');
const minMax = require('dayjs/plugin/minMax');
dayjs.extend(minMax);

const NUM_CIA       = () => parseInt(process.env.ERP_NUM_CIA,       10) || 1;

function round4(n) { return Math.round(Number(n) * 10000) / 10000; }

// ─────────────────────────────────────────────────────────────────────────────
// NUMERACIÓN — MAX+1 con detección de colisión dentro de la TX
// ─────────────────────────────────────────────────────────────────────────────

async function obtenerSiguienteNumeroOC(conn, cc) {
  const MAX_INTENTOS = 10;
  for (let i = 0; i < MAX_INTENTOS; i++) {
    const maxRows = await conn.query(
      `SELECT MAX(numero) AS ultimo FROM so_orden_compra WHERE cc = ?`, [cc]
    );
    const candidato = (Number(maxRows[0]?.ultimo) || 0) + 1;
    const existe = await conn.query(
      `SELECT 1 FROM so_orden_compra WHERE cc = ? AND numero = ?`, [cc, candidato]
    );
    if (!existe.length) {
      logger.debug(`Número OC asignado: CC=${cc} → ${candidato}`);
      return candidato;
    }
    logger.warn(`Colisión OC: CC=${cc} número=${candidato}, reintento ${i + 1}`);
  }
  throw new Error(`No se pudo obtener número OC para CC=${cc} tras ${MAX_INTENTOS} intentos.`);
}

// ─────────────────────────────────────────────────────────────────────────────
// INSERT so_orden_compra
// Columnas tomadas del INSERT real capturado en el Wireshark
// ─────────────────────────────────────────────────────────────────────────────

async function insertarCabeceraOC(conn, cc, numero, header, idEk) {
  const hoy = dayjs().format('YYYY-MM-DD');
  const horaActual = dayjs().format('HH:mm:ss');

  const libreAbordo    = header.libre_abordo != null ? Number(header.libre_abordo) : 1;
  const tipoOcReq      = '1';   // 1 NORMAL, 2 URGENTE, 3 CRITICO
  const comprador      = idEk;
  const comentarios    = String(header.comentarios || '').substring(0, 50);
  const estatusInicial = ''

  await conn.query(`
    INSERT INTO so_orden_compra (
      cc, numero, fecha,
      libre_abordo, tipo_oc_req,
      comprador, proveedor,
      moneda, tipo_cambio,
      sub_total, iva, total,
      sub_tot_rec, iva_rec, total_rec,
      sub_tot_ajus, iva_ajus, total_ajus,
      st_impresa, estatus,
      comentarios,
      solicito, vobo, autorizo,
      sub_tot_canc, iva_canc, total_canc,
      embarquese,
      total_fac, total_pag,
      porcent_iva,
      empleado_modifica, fecha_modifica, hora_modifica,
      tc_cc,
      bit_autorecepcion, almacen_autorecepcion, empleado_autorecepcion,
      rentencion_antes_iva, rentencion_despues_iva,
      bienes_servicios,
      concepto_factura,
      imprime_porcentaje,
      almacen,
      id_lugar,
      parametro_firma,
      empleado_autoriza, usuario_autoriza, fecha_autoriza,
      exento_iva,
      uso_cfdi, paramuso_cfdi, cfd_metodo_pago_sat
    ) VALUES (
      ?,?,?,
      ?,?,
      ?,?,
      ?,?,
      ?,?,?,
      0,0,0,
      0,0,0,
      'N',?,
      ?,
      ?,?,?,
      0,0,0,
      ?,
      0,0,
      ?,
      ?,?,?,
      ?,
      ?,?,?,
      0,0,
      ?,
      ?,
      null,
      ?,
      ?,
      '0',
      ?,?,?,
      0,
      ?,?,?
    )`,
    [
      cc, numero, hoy,
      libreAbordo, tipoOcReq,
      comprador, header.proveedor,
      header.moneda    || 1,
      header.tipoCambio || 1,
      header.subTotal, header.iva, header.total,
      estatusInicial,
      comentarios,
      header.solicito  || null,
      header.vobo      || null,
      header.autorizo  || null,
      header.embarquese || null,
      header.porcentIva || 16,
      comprador, hoy, horaActual,  // empleado_modifica, fecha_modifica, hora_modifica
      header.tipoCambio || 1,            // tc_cc
      header.bit_autorecepcion === 'S' ? 'S' : null,
      header.almacen_autorecepcion ? Number(header.almacen_autorecepcion) : null,
      header.empleado_autorecepcion ? Number(header.empleado_autorecepcion) : null,
      header.bienes_servicios || null,
      header.concepto_factura ? String(header.concepto_factura).substring(0, 40) : null,
      header.almacen ? Number(header.almacen) : null,
      header.id_lugar ? Number(header.id_lugar) : null,
      null,                              // empleado_autoriza
      null,                              // usuario_autoriza
      null,                              // fecha_autoriza
      header.uso_cfdi        ? String(header.uso_cfdi).substring(0, 3)       : null,
      header.uso_cfdi        ? String(header.uso_cfdi).substring(0, 3)       : 'N', // paramuso_cfdi
      header.metodo_pago     ? String(header.metodo_pago).substring(0, 3)    : null,
    ]
  );

  logger.debug(`so_orden_compra insertada: CC=${cc} N°=${numero} estatus=${estatusInicial}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// INSERT so_orden_compra_det
// Columnas tomadas del INSERT real capturado en el Wireshark
// ─────────────────────────────────────────────────────────────────────────────

async function insertarPartidasOC(conn, cc, numero, lines) {
  for (const line of lines) {
    const importe = line._importe ?? round4(line.cantidad * line.precio);
    const iva     = line._iva     ?? round4(importe * (line.porcent_iva ?? 16) / 100);
    const fecha = dayjs.max(dayjs(), dayjs(line.fecha_entrega)).format("YYYY-MM-DD");
    try{
    await conn.query(`
      INSERT INTO so_orden_compra_det (
        cc, numero, partida, insumo,
        fecha_entrega,
        cantidad, precio, importe,
        ajuste_cant, ajuste_imp,
        num_requisicion, part_requisicion,
        cant_recibida, imp_recibido,
        acum_ant, max_orig, max_ppto,
        fecha_recibido, cant_canc, imp_canc,
        area, cuenta, obra,
        multi_cc, frente, partida_obra,
        iva, porcent_iva,
        autoriza_precio, fecha_aut_precio,
        autoriza_cantidad, fecha_aut_cantidad,
        excede_ppto
      ) VALUES (
        ?,?,?,?,
        ?,
        ?,?,?,
        0,0,
        ?,?,
        0,0,
        0,0,0,
        NULL,NULL,NULL,
        ?,?,?,
        ?,?,?,
        ?,?,
        NULL,NULL,
        NULL,NULL,
        'N'
      )`,
      [
        cc, numero, line.partida, line.insumo,
        fecha,
        line.cantidad, line.precio, importe,
        Number(line.num_requisicion  || 0),
        Number(line.part_requisicion || 0),
        line.area        != null ? Number(line.area)        : null,
        line.cuenta      != null ? Number(line.cuenta)      : null,
        line.obra        || null,
        line.multi_cc    || null,
        line.frente      != null ? Number(line.frente)      : null,
        line.partida_obra != null ? Number(line.partida_obra) : null,
        iva, line.porcent_iva ?? 16,
      ]
    );

    // Descripción libre (so_orden_det_linea)
    if (line.descripcion_det?.trim()) {
      await conn.query(
        `INSERT INTO so_orden_det_linea (cc, numero, partida, descripcion)
         VALUES (?, ?, ?, ?)`,
        [cc, numero, line.partida, line.descripcion_det.trim()]
      );
    }
  }catch(err){
    logger.error(err);
  }

    logger.debug(`  partida=${line.partida} insumo=${line.insumo} cant=${line.cantidad} importe=${importe}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// INSERT so_orden_compra_pago  (forma de pago)
// ─────────────────────────────────────────────────────────────────────────────

async function insertarPagos(conn, cc, numero, pagos, total) {
  if (!pagos?.length) return;

  for (const [idx, pago] of pagos.entries()) {
    const partida   = idx + 1;
    const importe   = round4(total * (pago.porcentaje / 100));
    const fechaPago = dayjs().add(pago.dias_pago, 'day').format('YYYY-MM-DD');

    await conn.query(`
      INSERT INTO so_orden_compra_pago
        (cc, numero, partida, dias_pago, fecha_pago, comentarios, estatus, porcentaje, importe)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        cc, numero, partida,
        pago.dias_pago,
        fechaPago,
        String(pago.comentarios || '').substring(0, 50),
        pago.estatus || 'P',
        pago.porcentaje,
        importe,
      ]
    );
    logger.debug(`  pago ${partida}: ${pago.dias_pago} días · ${pago.porcentaje}% · $${importe}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// INSERT so_ordenc_retenciones
// Columnas del INSERT real: cc, numero, id_cpto, orden, cantidad, porc_ret,
//   importe, facturado, retenido, aplica, calc_iva, forma_pago,
//   tm_descto, afecta_fac, afecta_oc
// ─────────────────────────────────────────────────────────────────────────────

async function insertarRetenciones(conn, cc, numero, retenciones, totalesOC) {
  if (!retenciones?.length) return;
  let totalRet = 0;
  for (const ret of retenciones) {
    // El importe de retención se calcula sobre el total de la OC
    const baseCalculo = totalesOC.subTotal;
    const importe = round4(baseCalculo * (ret.porc_ret / 100));
    totalRet += importe;
    await conn.query(`
      INSERT INTO so_ordenc_retenciones
        (cc, numero, id_cpto, orden, cantidad, porc_ret, importe,
         facturado, retenido, aplica, calc_iva, forma_pago,
         tm_descto, afecta_fac, afecta_oc)
      VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)`,
      [
        cc, numero,
        ret.id_cpto,
        ret.orden,
        baseCalculo,         // cantidad = base de cálculo
        ret.porc_ret,
        importe,
        importe,            // importe retenido
        ret.aplica   || null,
        ret.calc_iva || null,
        ret.forma_pago || null,
        ret.tm_descto  != null ? Number(ret.tm_descto) : null,
        ret.afecta_fac || null,
        ret.afecta_oc  || null,
      ]
    );
    //Actualiza encabezado de orden de compra con retención
    const importeRet = round4(totalesOC.total - totalRet);
    await conn.query(`
      UPDATE so_orden_compra SET rentencion_despues_iva = ?, total = ? WHERE cc = ? AND numero = ?`,
      [
        importe, importeRet, cc, numero
      ]
    );

    logger.debug(`  retención ${ret.id_cpto} (${ret._desc}): ${ret.porc_ret}% = $${importe}`);

    return totalRet;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// MARCAR IMPRESA — UPDATE so_orden_compra SET st_impresa='I'
// Equivale al "Imprimir" del ERP que cambia el estatus y dispara autorecepción
// ─────────────────────────────────────────────────────────────────────────────

async function marcarImpresa(conn, cc, numero) {
  await conn.query(
    `UPDATE so_orden_compra SET st_impresa = 'I' WHERE cc = ? AND numero = ?`,
    [cc, numero]
  );
  logger.debug(`so_orden_compra marcada como impresa: CC=${cc} N°=${numero}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// AUTORECEPCIÓN
// Solo aplica para insumos NO inventariables (grupos_insumo.inventariado = N)
//
// Reproduce la secuencia del ERP al imprimir con autorecepción activa:
//   1. UPDATE so_orden_compra_det → cant_recibida, imp_recibido, fecha_recibido
//   2. UPDATE so_orden_compra     → sub_tot_rec, iva_rec, total_rec, estatus='T'
//   3. UPDATE so_explos_mat       → acumula cant_recibida, imp_recibido
//   4. UPDATE so_movimientos_noinv (si existe el movimiento) / INSERT (si no)
//   5. CALL sin_sp_recalcula_importes_noinv
//
// NOTA: los pasos 4 y 5 requieren manejo de remision (MAX+1 en so_movimientos_noinv).
//       Los implementamos con la misma estrategia anti-colisión que el número de OC.
// ─────────────────────────────────────────────────────────────────────────────

async function ejecutarAutorecepcion(conn, cc, numero, header, lines, idEk) {
  const hoy = dayjs().format('YYYY-MM-DD');

  // Filtrar solo líneas de insumos NO inventariables
  const lineasNoInv = lines.filter(l => l._inventariado === 'N');
  if (!lineasNoInv.length) {
    logger.info(`Autorecepción CC=${cc} N°=${numero}: sin insumos no inventariables.`);
    return;
  }

  const almacen  = Number(header.almacen_autorecepcion);
  const anioActual = dayjs().year();

  // 1. UPDATE so_orden_compra_det — recibir todas las partidas no inventariables
  await conn.query(
    `UPDATE so_orden_compra_det
     SET cant_recibida = cantidad,
         imp_recibido  = importe,
         fecha_recibido = ?
     WHERE cc = ? AND numero = ?
       AND insumo IN (${lineasNoInv.map(() => '?').join(',')})`,
    [hoy, cc, numero, ...lineasNoInv.map(l => l.insumo)]
  );

  // Recalcular totales recibidos de toda la OC
  const totRec = await conn.query(
    `SELECT ISNULL(SUM(importe),0) AS sub_tot_rec,
            ISNULL(SUM(iva),0)     AS iva_rec
     FROM so_orden_compra_det WHERE cc = ? AND numero = ?`,
    [cc, numero]
  );
  const subTotRec = round4(Number(totRec[0]?.sub_tot_rec || 0));
  const ivaRec    = round4(Number(totRec[0]?.iva_rec     || 0));

  // 2. UPDATE so_orden_compra
  await conn.query(
    `UPDATE so_orden_compra
     SET sub_tot_rec = ?, iva_rec = ?, total_rec = ?, estatus = 'T'
     WHERE cc = ? AND numero = ?`,
    [subTotRec, ivaRec, round4(subTotRec + ivaRec), cc, numero]
  );
  logger.info(`Autorecepción: so_orden_compra estatus='T' CC=${cc} N°=${numero}`);

  // 3. UPDATE so_explos_mat por cada línea no inventariable
  for (const line of lineasNoInv) {
    await conn.query(
      `UPDATE so_explos_mat
       SET cant_recibida = COALESCE(cant_recibida, 0) + ?,
           imp_recibido  = COALESCE(imp_recibido,  0) + ?
       WHERE cc = ? AND insumo = ? AND year_explos = ?`,
      [line.cantidad, line._importe, cc, line.insumo, anioActual]
    );
  }

  // 4. Obtener o crear remisión en so_movimientos_noinv
  const maxRem = await conn.query(
    `SELECT MAX(remision) AS ultimo FROM so_movimientos_noinv
     WHERE almacen = ? AND tipo_mov = 1`, [almacen]
  );
  const remision = (Number(maxRem[0]?.ultimo) || 0) + 1;

  const mes = dayjs().month() + 1;
  const horaActual = dayjs().format('HH:mm:ss');
  const res = await conn.query(
    `INSERT INTO so_movimientos_noinv(
      almacen, tipo_mov, remision, 
      cc, compania, periodo, 
      ano, orden_ct, frente, 
      fecha, proveedor, total, 
      estatus, transferida, poliza, 
      empleado, alm_destino, cc_destino,
      comentarios, tipo_trasp, 
      tipo_cambio, hora, fecha_modifica, 
      empleado_modifica, tc_cc, fecha_creacion
    )VALUES(
      ?, 1, ?, 
      ?, 1, ?, 
      ?, ?, 0, 
      ?, ?, ?, 
      'N', 'N', 0, 
      ?, 0, '', 
      '', 0, 
      1, ?, ?, 
      ?, 1, GETDATE()
    )`, 
    [ almacen, remision, 
      cc, mes, 
      anioActual, numero, 
      hoy, header.proveedor, subTotRec, 
      idEk, 
      horaActual, hoy, 
      idEk]
  );// almacen, remision, cc, periodo, ano, orden_ct, fecha, proveedor, total,
    // empleado, hora, fecha_modifica, empleado_modifica

  for (const line of lineasNoInv) {
    const importe = line._importe ?? round4(line.cantidad * line.precio);
    await conn.query(
      `INSERT INTO so_movimientos_noinv_det(
        almacen, tipo_mov, remision, partida, insumo, comentarios, area, cuenta, cantidad, precio, importe, partida_oc, remision2, multi_cc
      )VALUES(
        ?, 1, ?, ?, ?, '', null, null, ?, ?, ?, ?, ?, ?
      )`, 
      [almacen, remision, line.partida, line.insumo, line.cantidad, line.precio, importe, line.partida, remision, cc]
    );
    
  }
  // 5. Llamar al stored procedure de recálculo
  try {
    await conn.query(
      `SELECT sin_sp_recalcula_importes_noinv(?, 1, ?) FROM dummy`,
      [almacen, remision]
    );
    logger.info(`sin_sp_recalcula_importes_noinv(${almacen}, 1, ${remision}) OK`);

  } catch (err) {
    // No es crítico — la OC ya quedó recibida
    logger.warn(`sin_sp_recalcula_importes_noinv falló (no crítico): ${err.message}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// PROCESAMIENTO DE UN BLOQUE
// ─────────────────────────────────────────────────────────────────────────────

async function procesarBloque(conn, block, idEk) {
  const datos  = await validarBloque(conn, block, NUM_CIA());
  const { header, lines } = datos;

  const numero = await obtenerSiguienteNumeroOC(conn, header.cc);
  
  // 1. Cabecera
  await insertarCabeceraOC(conn, header.cc, numero, header, idEk);

  // 2. Partidas + líneas de descripción
  await insertarPartidasOC(conn, header.cc, numero, lines);

  // 3. Forma de pago
  await insertarPagos(conn, header.cc, numero, header._pagos, header.total);

  // 4. Retenciones
  const totalRet = await insertarRetenciones(conn, header.cc, numero, header._retencionesEnriquecidas, {
    subTotal: header.subTotal,
    iva:      header.iva,
    total:    header.total,
  });

  // 5. Marcar impresa (equivalente al "Imprimir" del ERP → st_impresa='I')
  await marcarImpresa(conn, header.cc, numero);

  // 6. Autorecepción (si aplica)
  if (header.bit_autorecepcion === 'S') { 
    await ejecutarAutorecepcion(conn, header.cc, numero, header, lines, idEk);
  }

  logger.info(
    `✓ OC CC=${header.cc} N°=${numero} | ${lines.length} partidas | $${header.total - totalRet}` +
    (header._retencionesEnriquecidas?.length ? ` | ${header._retencionesEnriquecidas.length} ret.` : '') +
    (header.bit_autorecepcion === 'S' ? ' | AUTORECEPCIÓN' : '')
  );

  return {
    bloqueId:      block.bloqueId,
    cc:            header.cc,
    numero,
    partidas:      lines.length,
    moneda:        header.moneda    || 'MN',
    tipoCambio:    header.tipoCambio || 1,
    subTotal:      header.subTotal,
    iva:           header.iva,
    total:         header.total - totalRet,
    fechaOc:       dayjs().format('YYYY-MM-DD'),
    proveedor:     header.proveedor,
    provNombre:    datos._provData?.nombre?.trim(),
    retenciones:   header._retencionesEnriquecidas?.length || 0,
    autorecepcion: header.bit_autorecepcion === 'S',
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// BATCH
// ─────────────────────────────────────────────────────────────────────────────

async function procesarBatch(blocks, onProgress = () => {}, idEk = null, usuarioKey = null) {
  const { withTransaction } = require('../config/db');
  const exitosos = [], errores = [];
  logger.info(`Batch iniciado: ${blocks.length} bloque(s)`);

  for (const block of blocks) {
    onProgress(block.bloqueId, 'procesando', null);
    try { 
      const resultado = await withTransaction(conn => procesarBloque(conn, block, idEk));
      exitosos.push(resultado);
      onProgress(block.bloqueId, 'exitoso', resultado);
      // Registrar en historial SQL Server (no crítico — no revierte si falla)
      registrarOC(resultado, usuarioKey || 'batch', idEk).catch(() => {});
    } catch (err) {
      const error = {
        bloqueId:  block.bloqueId,
        rowStart:  block.rowStart,
        cc:        block.header?.cc,
        proveedor: block.header?.proveedor,
        mensaje:   err.message,
      };
      errores.push(error);
      onProgress(block.bloqueId, 'error', error);
      logger.error(`✗ Bloque ${block.bloqueId}: ${err.message}`);
    }
  }

  logger.info(`Batch fin: ${exitosos.length} OK / ${errores.length} error de ${blocks.length}`);
  return {
    exitosos, errores,
    total:    blocks.length,
    totalOk:  exitosos.length,
    totalErr: errores.length,
  };
}

module.exports = { procesarBatch };
