/**
 * pptoValidator.js
 *
 * Validación de presupuesto de obra por línea de OC.
 * Reproduce la lógica exacta del ERP Enkontrol capturada en Wireshark.
 *
 * ÁRBOL DE DECISIÓN (por línea/insumo):
 *
 *  1. cc.st_ppto             ¿El CC tiene presupuesto activo?
 *     └ 'N' o NULL → NO_APLICA (sin validación)
 *
 *  2. so_validaciones_compras.valida_cc
 *     └ 'N' → NO_APLICA
 *
 *  3. grupos_insumo.valida_ppto
 *     └ 'N' → NO_APLICA
 *
 *  4. grupos_insumo.bit_ppto  ¿Qué se valida?
 *     'O' → importe   'C' → cantidad   otros → NO_APLICA
 *
 *  5. parametros_cia id=13   ¿Validar por año? (valor > 0 → año actual, 0 → todo)
 *
 *  6. so_explos_mat           Presupuesto autorizado del insumo en el CC
 *     └ 0 → SIN_PPTO (bloqueado)
 *
 *  7. so_orden_compra_det     Cantidad/importe ya comprometida en otras OCs
 *
 *  8. si_movimientos tipo=2   Entradas de almacén (transferencias recibidas)
 *
 *  9. si_movimientos tipo=52  Otro tipo de movimiento de salida
 *
 *  Disponible = Autorizado - Comprometido - Consumido(tipo2) + Consumido(tipo52)
 *  Si Disponible < cantidad/importe de esta línea → EXCEDIDO (bloqueado)
 *
 * RESULTADO por línea:
 *   ppto_status:      'OK' | 'SIN_PPTO' | 'EXCEDIDO' | 'NO_APLICA'
 *   ppto_disponible:  número (puede ser negativo si ya está excedido)
 *   ppto_autorizado:  número
 *   ppto_comprometido:número
 *   ppto_msg:         string descriptivo para el usuario
 *   bloqueado:        boolean (true = no se puede generar la OC)
 */

const logger = require('../utils/logger');
const MODULO = 'pptoValidator';

// ─── Helpers ──────────────────────────────────────────────────────────────────

function round4(n) { return Math.round(Number(n) * 10000) / 10000; }

/**
 * Obtiene el año de validación según parámetro 13.
 * valor > 0 → usar año actual, 0 → sin filtro de año
 */
async function getAnioValidacion(conn) {
  const rows = await conn.query(
    `SELECT FIRST valor FROM parametros_cia WHERE id_parametro = 13 AND sistema = 'SOC'`
  );
  const val = rows.length ? Number(rows[0].valor) : 0;
  return val > 0 ? new Date().getFullYear() : 0;
}

/**
 * Presupuesto autorizado del insumo en el CC (so_explos_mat).
 * Incluye aditivas y deducciones: cantidad + aditiva_cant - deduc_cant
 */
async function getPptoAutorizado(conn, cc, insumo) {
  const rows = await conn.query(
    `SELECT ISNULL(SUM(
        (COALESCE(cantidad, 0)) +
        COALESCE(aditiva_cant, 0) -
        COALESCE(deduc_cant, 0)
      ), 0) AS ppto_cant
     FROM so_explos_mat
     WHERE cc = ? AND insumo = ?`,
    [cc, insumo]
  );
  return round4(Number(rows[0]?.ppto_cant || 0));
}

/**
 * Cantidad/importe ya comprometida en otras OCs (so_orden_compra_det).
 * Excluye la OC actual si se pasa ocActual (formato "CC000000").
 */
async function getCantidadComprometidaOC(conn, cc, insumo, anio, ocActual = '') {
  const rows = await conn.query(
    `SELECT ISNULL(SUM(so_orden_compra_det.cantidad), 0) AS cant_comp
     FROM so_orden_compra_det, so_orden_compra
     WHERE so_orden_compra_det.numero = so_orden_compra.numero
       AND so_orden_compra_det.CC     = so_orden_compra.CC
       AND so_orden_compra_det.insumo = ?
       AND (
         (ISNULL(so_orden_compra_det.multi_cc, '') = '' AND so_orden_compra_det.cc = ?)
         OR
         (ISNULL(so_orden_compra_det.multi_cc, '') <> '' AND so_orden_compra_det.multi_cc = ?)
       )
       AND ((? > 0 AND ? = YEAR(so_orden_compra.fecha)) OR ? = 0)
       AND STRING(so_orden_compra_det.cc,
             REPLICATE('0', 6 - LEN(so_orden_compra_det.numero)),
             so_orden_compra_det.numero) <> ?`,
    [insumo, cc, cc, anio, anio, anio, ocActual || '']
  );
  return round4(Number(rows[0]?.cant_comp || 0));
}

/**
 * Movimientos de almacén tipo 2 (transferencias de entrada con destino).
 */
async function getMovimientosAlmacen(conn, cc, insumo, anio, tipoMov) {
  const rows = await conn.query(
    `SELECT ISNULL(SUM(simd.cantidad), 0) AS cant_mov,
            ISNULL(SUM(simd.cantidad * (
              IF sim.tipo_mov >= 51 THEN simd.precio * -1 ELSE simd.precio END IF
            ) / (IF COALESCE(sim.tc_cc, 0) = 0 THEN 1 ELSE sim.tc_cc END IF)), 0) AS imp_mov
     FROM si_movimientos sim, si_movimientos_det simd
     WHERE sim.almacen   = simd.almacen
       AND sim.tipo_mov  = simd.tipo_mov
       AND sim.numero    = simd.numero
       AND sim.cc        = ?
       AND simd.insumo   = ?
       AND ((? > 0 AND ? = sim.ano) OR ? = 0)
       AND simd.tipo_mov = ?
       AND COALESCE(sim.numero_destino, 0) > 0`,
    [cc, insumo, anio, anio, anio, tipoMov]
  );
  return {
    cantidad: round4(Number(rows[0]?.cant_mov || 0)),
    importe:  round4(Number(rows[0]?.imp_mov  || 0)),
  };
}

// ─── VALIDACIÓN PRINCIPAL ─────────────────────────────────────────────────────

/**
 * Valida el presupuesto de UNA línea de OC.
 *
 * @param {object} conn       Conexión Sybase
 * @param {string} cc         Centro de costo
 * @param {object} line       Línea del Excel enriquecida por erpValidators
 * @param {object} grupoData  Datos de grupos_insumo (tipo_insumo, grupo_insumo, etc.)
 * @param {number} anio       Año de validación (de getAnioValidacion)
 * @param {string} ocActual   "CC000000" para excluir al re-validar una OC existente
 *
 * @returns {{ ppto_status, ppto_disponible, ppto_autorizado, ppto_comprometido, ppto_msg, bloqueado }}
 */
async function validarLineaPpto(conn, cc, line, grupoData, anio, ocActual = '') {
  const insumo = line.insumo;
  const base = {
    ppto_status:       'NO_APLICA',
    ppto_disponible:   null,
    ppto_autorizado:   null,
    ppto_comprometido: null,
    ppto_msg:          'Sin validación de presupuesto para este insumo/CC',
    bloqueado:         false,
  };

  // ── Paso 1: ¿El CC tiene presupuesto activo? ─────────────────────────────
  const ccRows = await conn.query(
    `SELECT ISNULL(st_ppto, 'N') AS st_ppto,
            ISNULL(bit_ppto_mensual, 'N') AS bit_ppto_mensual
     FROM cc WHERE cc = ?`, [cc]
  );
  if (!ccRows.length || ccRows[0].st_ppto !== 'S') {
    return { ...base, ppto_msg: 'CC no valida presupuesto (st_ppto ≠ S)' };
  }

  // ── Paso 2: ¿El CC tiene validación de compras habilitada? ───────────────
  // EL VALOR ES 'N' SIEMPRE ///
  /* const valCC = await conn.query(
    `SELECT ISNULL(valida_cc, 'N') AS valida_cc
     FROM so_validaciones_compras WHERE cc = ?`, [cc]
  );
  if (!valCC.length || valCC[0].valida_cc !== 'S') {
    return { ...base, ppto_msg: 'CC no configurado para validar presupuesto en compras' };
  } */

  // ── Paso 3 y 4: ¿El grupo del insumo valida presupuesto y cómo? ──────────
  const { valida_ppto, bit_ppto, valida_ppto_cantidad } = grupoData;

  if (valida_ppto !== 'S') {
    return { ...base, ppto_msg: 'Grupo de insumo no requiere validación de presupuesto' };
  }

  // bit_ppto: 'O' = importe, 'C' = cantidad
  const validarPorCantidad = (bit_ppto === 'C' || valida_ppto_cantidad === 'S');
  const validarPorImporte  = (bit_ppto === 'O');

  if (!validarPorCantidad && !validarPorImporte) {
    return { ...base, ppto_msg: 'Grupo de insumo sin tipo de validación definido (bit_ppto)' };
  }

  // ── Paso 5: Año de validación ─────────────────────────────────────────────
  // (ya viene resuelto como parámetro)

  // ── Paso 6: Presupuesto autorizado en so_explos_mat ───────────────────────
  const pptoAutorizado = await getPptoAutorizado(conn, cc, insumo);

  if (pptoAutorizado <= 0) {
    return {
      ppto_status:       'SIN_PPTO',
      ppto_disponible:   0,
      ppto_autorizado:   0,
      ppto_comprometido: null,
      ppto_msg:          `Insumo ${insumo} no tiene presupuesto autorizado en CC ${cc} (año ${anio > 0 ? anio : 'todos'})`,
      bloqueado:         true,
    };
  }

  // ── Pasos 7-9: Comprometido y consumido ──────────────────────────────────
  const comprometidoOC = await getCantidadComprometidaOC(conn, cc, insumo, anio, ocActual);
  const mov2  = await getMovimientosAlmacen(conn, cc, insumo, anio, 2);
  const mov52 = await getMovimientosAlmacen(conn, cc, insumo, anio, 52);

  // Disponible = Autorizado - Comprometido(OCs) - Entradas(tipo2) + Salidas(tipo52)
  const totalComprometido = round4(comprometidoOC + mov2.cantidad - mov52.cantidad);
  const disponible        = round4(pptoAutorizado - totalComprometido);

  // Valor a comparar según tipo de validación
  const valorLinea = validarPorCantidad ? Number(line.cantidad) : round4(line._importe || (line.cantidad * line.precio));

  if (disponible < valorLinea) {
    const tipo = validarPorCantidad ? 'cantidad' : 'importe';
    return {
      ppto_status:       'EXCEDIDO',
      ppto_disponible:   disponible,
      ppto_autorizado:   pptoAutorizado,
      ppto_comprometido: totalComprometido,
      ppto_msg:          `Presupuesto excedido: disponible=${disponible} < requerido=${valorLinea} (${tipo})`,
      bloqueado:         true,
    };
  }

  return {
    ppto_status:       'OK',
    ppto_disponible:   disponible,
    ppto_autorizado:   pptoAutorizado,
    ppto_comprometido: totalComprometido,
    ppto_msg:          `OK — disponible: ${disponible}`,
    bloqueado:         false,
  };
}

/**
 * Valida el presupuesto de TODAS las líneas de un bloque.
 * Devuelve las líneas enriquecidas con info de presupuesto y un flag global.
 *
 * @returns {{ lines: [], pptoOk: boolean, resumenPpto: [] }}
 */
async function validarPptoBloque(conn, cc, lines, anio, ocActual = '') {
  const resumen = [];
  let pptoOk = true;

  for (const line of lines) {
    // Los datos de grupos_insumo ya vienen en line._grupoData (ver erpValidators)
    const grupoData = line._grupoData || {};

    let resultado;
    try {
      resultado = await validarLineaPpto(conn, cc, line, grupoData, anio, ocActual);
    } catch (err) {
      logger.warn(`[${MODULO}] Error validando ppto línea ${line.partida} insumo ${line.insumo}: ${err.message}`);
      resultado = {
        ppto_status:   'NO_APLICA',
        ppto_msg:      `Error en validación: ${err.message}`,
        bloqueado:     false,
      };
    }

    if (resultado.bloqueado) pptoOk = false;

    // Enriquecer la línea con resultado de presupuesto
    Object.assign(line, {
      ppto_status:       resultado.ppto_status,
      ppto_disponible:   resultado.ppto_disponible,
      ppto_autorizado:   resultado.ppto_autorizado,
      ppto_comprometido: resultado.ppto_comprometido,
      ppto_msg:          resultado.ppto_msg,
    });

    resumen.push({
      partida:     line.partida,
      insumo:      line.insumo,
      descripcion: line._insumoDesc || '',
      ...resultado,
    });
  }

  logger.info(
    `[${MODULO}] CC=${cc} — ppto ${pptoOk ? 'OK' : 'BLOQUEADO'} ` +
    `(${resumen.filter(r => r.bloqueado).length} línea(s) bloqueadas de ${lines.length})`
  );

  return { lines, pptoOk, resumenPpto: resumen };
}

module.exports = { validarPptoBloque, getAnioValidacion };
