/**
 * erpValidators.js
 *
 * Validaciones de negocio de Enkontrol para creación de OC.
 *
 * Nuevas validaciones vs versión anterior:
 *   validarAlmacen()     — si_almacen (para autorecepción)
 *   validarRetenciones() — so_retenciones (catálogo de retenciones)
 *   validarInsumoInventariable() — grupos_insumo.inventariado (autorecepción solo no-inv)
 */
const logger = require('../utils/logger');
const dayjs  = require('dayjs');

// ─── Parámetros ───────────────────────────────────────────────────────────────

async function getParametro(conn, sistema, numCia, idParametro) {
  const rows = await conn.query(
    `SELECT valor FROM parametros_cia
     WHERE sistema = ? AND num_cia = ? AND id_parametro = ?`,
    [sistema, String(numCia).padStart(2, '0'), idParametro]
  );
  return rows.length ? rows[0].valor : null;
}

async function getDatosCompania(conn) {
  const rows = await conn.query(`SELECT * FROM datos_compania`);
  if (!rows.length) throw new Error('No se encontró configuración en datos_compania.');
  return rows[0];
}

// ─── PERIODO ──────────────────────────────────────────────────────────────────
async function validarMesContable(conn) {
  //if (!fecha) throw new Error('fecha es requerida.');
  const year = dayjs().year(); // Ejemplo: 2026
  const mes = dayjs().month() + 1; // Enero es 0, por eso sumamos 1 [1]
  const rows = await conn.query(
    `SELECT * FROM sc_mesproc
     WHERE year = ?
     AND mes = ?`, [year, mes]
  );
  if (!rows.length) throw new Error(`No existe periodo contable.`);
  const per = rows[0];
  if (per.soc.trim() !== 'N')
    throw new Error(`Periodo contable cerrado.`);
  return per; // el periodo debe estar abierto
}

// ─── CC ───────────────────────────────────────────────────────────────────────

async function validarCC(conn, cc) {
  if (!cc) throw new Error('Centro de costo es requerido.');
  const rows = await conn.query(
    `SELECT cc, descripcion, valida_vigencia, fecha_inicio, fecha_fin, cve_moneda
     FROM cc WHERE cc = ?`, [cc]
  );
  if (!rows.length) throw new Error(`Centro de costo "${cc}" no existe.`);
  const row = rows[0];
  if (row.valida_vigencia === 'S') {
    const hoy = new Date();
    if (row.fecha_inicio && new Date(row.fecha_inicio) > hoy)
      throw new Error(`CC "${cc}" aún no está vigente (inicio: ${row.fecha_inicio}).`);
    if (row.fecha_fin && new Date(row.fecha_fin) < hoy)
      throw new Error(`CC "${cc}" ya venció (fin: ${row.fecha_fin}).`);
  }
  return row;
}

// ─── Proveedor ────────────────────────────────────────────────────────────────
// sp_proveedores: cancelado ('C'=cancelado), activo (varchar 1)

async function validarProveedor(conn, numpro) {
  if (!numpro) throw new Error('Proveedor es requerido.');
  const rows = await conn.query(
    `SELECT numpro, nombre, nomcorto, cancelado, activo, rfc, moneda
     FROM sp_proveedores WHERE numpro = ?`, [numpro]
  );
  if (!rows.length) throw new Error(`Proveedor ${numpro} no existe.`);
  const p = rows[0];
  if (p.cancelado === 'C')
    throw new Error(`Proveedor ${numpro} (${p.nombre?.trim()}) está cancelado.`);
  if (p.activo !== null && p.activo !== undefined && !['A','S','1',''].includes(p.activo?.trim()))
    throw new Error(`Proveedor ${numpro} no está activo (activo="${p.activo}").`);
  await validarEFOS(conn, numpro, p.rfc);
  return p;
}

async function validarEFOS(conn, numpro, rfc) {
  if (!rfc?.trim()) return;
  const rfcLimpio = rfc.trim();
  const efos = await conn.query(`SELECT 1 FROM ek_efos WHERE rfc = ?`, [rfcLimpio]);
  if (efos.length)
    throw new Error(`Proveedor ${numpro} (RFC ${rfcLimpio}) aparece en lista EFOS del SAT.`);
  const noloc = await conn.query(`SELECT 1 FROM ek_nolocalizados WHERE rfc = ?`, [rfcLimpio]);
  if (noloc.length)
    throw new Error(`Proveedor ${numpro} (RFC ${rfcLimpio}) aparece en lista No Localizados SAT.`);
}

// ─── Insumo ───────────────────────────────────────────────────────────────────
// insumos: cancelado char ('A'=activo, cualquier otro=cancelado)

async function validarInsumo(conn, insumo) {
  if (!insumo) throw new Error('Insumo es requerido.');
  const rows = await conn.query(
    `SELECT i.insumo, i.descripcion, i.unidad, i.cancelado, i.tipo, i.grupo,
            gi.inventariado,
            gi.valida_ppto, gi.bit_ppto,
            gi.valida_ppto_precio, gi.valida_ppto_cantidad, gi.valida_ppto_importe
     FROM insumos i
     JOIN grupos_insumo gi ON gi.tipo_insumo = i.tipo AND gi.grupo_insumo = i.grupo
     WHERE i.insumo = ?`, [insumo]
  );
  if (!rows.length) throw new Error(`Insumo ${insumo} no existe.`);
  const ins = rows[0];
  if (ins.cancelado && ins.cancelado.trim() !== 'A')
    throw new Error(`Insumo ${insumo} (${ins.descripcion?.trim()}) está cancelado.`);
  return ins; // .inventariado y .valida_ppto / .bit_ppto son clave para ppto y autorecepción
}

// ─── Almacén ──────────────────────────────────────────────────────────────────
// Usado para autorecepción y para el campo almacen de la OC

async function validarAlmacen(conn, almacen, cc = null) {
  if (!almacen) return null;
  const rows = await conn.query(
    `SELECT almacen, descripcion, valida_almacen_cc FROM si_almacen WHERE almacen = ?`,
    [almacen]
  );
  if (!rows.length) throw new Error(`Almacén ${almacen} no existe.`);
  return rows[0];
}

// ─── Moneda ───────────────────────────────────────────────────────────────────

async function validarMoneda(conn, clave) {
  if (!clave) throw new Error('Moneda es requerida.');
  const rows = await conn.query(`SELECT clave FROM moneda WHERE moneda = ? OR clave = ?`, [clave, clave]);
  if (!rows.length) throw new Error(`Moneda "${clave}" no existe.`);
  return rows[0].clave;
}

async function getTipoCambio(conn, clave) {
  if (!clave || clave.trim() === 'MN') return 1;
  const rows = await conn.query(
    `SELECT TOP 1 tipo_cambio FROM tipo_cambio WHERE moneda = ? ORDER BY fecha DESC`, [clave]
  );
  if (!rows.length) throw new Error(`Sin tipo de cambio registrado para moneda "${clave}".`);
  return Number(rows[0].tipo_cambio);
}

// ─── Empleados ────────────────────────────────────────────────────────────────

async function validarEmpleado(conn, empleado, rol) {
  if (!empleado) return null;
  const rows = await conn.query(
    `SELECT empleado, descripcion FROM empleados WHERE empleado = ? `, [empleado]
  );
  if (!rows.length)
    throw new Error(`${rol || 'Empleado'} ${empleado} no existe o no está activo.`);
  return rows[0];
}

// ─── Área-Cuenta ─────────────────────────────────────────────────────────────

async function validarAreaCuenta(conn, area, cuenta) {
  if (!area || !cuenta) return null;
  const rows = await conn.query(
    `SELECT area, cuenta FROM si_area_cuenta WHERE area = ? AND cuenta = ?`, [area, cuenta]
  );
  if (!rows.length)
    throw new Error(`Área-Cuenta ${area}-${cuenta} no existe.`);
  return rows[0];
}

// ─── Retenciones ─────────────────────────────────────────────────────────────
// Consulta el catálogo so_retenciones para obtener datos que se insertan en so_ordenc_retenciones

async function validarYEnriquecerRetencion(conn, retencion, numpro) {
  const { id_cpto, porc } = retencion;
  const rows = await conn.query(
    `SELECT id_cpto, desc_ret, porc_cant, porc_default, aplica,
            naturaleza_ret, insumo, forma_pago, calc_iva, afecta_fac,
            TM_DESCTO, bit_afecta_oc as afecta_oc
     FROM so_retenciones WHERE id_cpto = ?`, [id_cpto]
  );
  if (!rows.length)
    throw new Error(`Retención con id_cpto=${id_cpto} no existe en el catálogo.`);
  const cat = rows[0];

  // Si el proveedor tiene porcentaje específico, usarlo
  let porcFinal = porc || Number(cat.porc_default) || 0;
  if (numpro) {
    const provRet = await conn.query(
      `SELECT porc_ret FROM sp_prov_retenciones WHERE numpro = ? AND id_cpto = ?`,
      [numpro, id_cpto]
    );
    if (provRet.length && provRet[0].porc_ret) porcFinal = Number(provRet[0].porc_ret);
  }

  return {
    id_cpto,
    orden:       retencion.orden || 1,
    porc_ret:    porcFinal,
    aplica:      cat.aplica,
    calc_iva:    cat.calc_iva,
    forma_pago:  cat.forma_pago,
    tm_descto:   cat.TM_DESCTO,
    afecta_fac:  cat.afecta_fac,
    afecta_oc:   cat.afecta_oc,
    _desc:       cat.desc_ret,
  };
}

// ─── Totales ──────────────────────────────────────────────────────────────────

function calcularTotales(lines, tipoCambio = 1) {
  let subTotal = 0, ivaTotal = 0;
  for (const line of lines) {
    const importe = round4(line.cantidad * line.precio);
    const porcIva = Number(line.porcent_iva ?? 16);
    const iva     = round4(importe * porcIva / 100);
    line._importe = importe;
    line._iva     = iva;
    subTotal += importe;
    ivaTotal += iva;
  }
  subTotal = round4(subTotal * tipoCambio);
  ivaTotal = round4(ivaTotal * tipoCambio);
  return { subTotal, iva: ivaTotal, total: round4(subTotal + ivaTotal) };
}

function round4(n) { return Math.round(Number(n) * 10000) / 10000; }

// ─── Validación completa de bloque ────────────────────────────────────────────

async function validarBloque(conn, block, numCia) {
  const { header, lines } = block;
  logger.debug(`Validando bloque ${block.bloqueId}...`);

  const [ccData, provData, moneda] = await Promise.all([
    validarCC(conn, header.cc),
    validarProveedor(conn, header.proveedor),
    validarMoneda(conn, header.moneda || 'MN'),
    validarMesContable(conn),
  ]);
  
  const tipoCambio = header.tipoCambio
    ? Number(header.tipoCambio)
    : await getTipoCambio(conn, header.moneda);

  await Promise.all([
    validarEmpleado(conn, header.comprador, 'Comprador'),
    validarEmpleado(conn, header.solicito,  'Solicitante'),
    validarEmpleado(conn, header.vobo,      'Visto Bueno'),
    validarEmpleado(conn, header.autorizo,  'Autorizador'),
    header.almacen ? validarAlmacen(conn, header.almacen, header.cc) : null,
  ]);

  // ── Validar autorecepción ────────────────────────────────────────────────
  let autoRecepcionValida = false;
  if (header.bit_autorecepcion === 'S') {
    if (!header.almacen_autorecepcion)
      throw new Error(`Bloque ${block.bloqueId}: se marcó autorecepción pero falta "almacen_autorecepcion".`);
    await validarAlmacen(conn, header.almacen_autorecepcion, header.cc);
    if (header.empleado_autorecepcion)
      await validarEmpleado(conn, header.empleado_autorecepcion, 'Empleado autorecepción');
    autoRecepcionValida = true;
  }

  // ── Retenciones ───────────────────────────────────────────────────────────
  const retencionesEnriquecidas = [];
  for (const ret of (header._retenciones || [])) {
    const retEnr = await validarYEnriquecerRetencion(conn, ret, header.proveedor);
    retencionesEnriquecidas.push(retEnr);
  }

  // ── Líneas ────────────────────────────────────────────────────────────────
  const linesEnriquecidas = [];
  for (const [idx, line] of lines.entries()) {
    if (!line.fecha_entrega)
      throw new Error(`Bloque ${block.bloqueId}, fila ${line._excelRow}: "fecha_entrega" requerida.`);

    const insumoData = await validarInsumo(conn, line.insumo);

    // Si hay autorecepción, verificar que todos los insumos son NO inventariables
    if (autoRecepcionValida && insumoData.inventariado === 'I') {
      logger.warn(
        `Bloque ${block.bloqueId}: insumo ${line.insumo} es inventariable — ` +
        `se excluirá de la autorecepción.`
      );
    }

    if (line.area && line.cuenta)
      await validarAreaCuenta(conn, line.area, line.cuenta);

    linesEnriquecidas.push({
      ...line,
      partida:        idx + 1,
      porcent_iva:    line.porcent_iva ?? 16,
      _insumoDesc:    insumoData.descripcion?.trim(),
      _unidad:        insumoData.unidad?.trim(),
      _inventariado:  insumoData.inventariado,
      // Datos del grupo para validación de presupuesto
      _grupoData: {
        valida_ppto:          insumoData.valida_ppto          || 'N',
        bit_ppto:             insumoData.bit_ppto             || 'O',
        valida_ppto_precio:   insumoData.valida_ppto_precio   || 'N',
        valida_ppto_cantidad: insumoData.valida_ppto_cantidad || 'N',
        valida_ppto_importe:  insumoData.valida_ppto_importe  || 'N',
      },
    });
  }

  const totales = calcularTotales(linesEnriquecidas, tipoCambio);

  // ── Parámetros de autorización ────────────────────────────────────────────
  const sistema   = process.env.ERP_SISTEMA || 'SOC';
  const numCiaStr = String(numCia).padStart(2, '0');
  const [p15, p102, datosComp] = await Promise.all([
    getParametro(conn, sistema, numCiaStr, 15),
    getParametro(conn, sistema, numCiaStr, 102),
    getDatosCompania(conn),
  ]);
  const requiereAutorizacion =
    p15 === 'S' || p102 === '1' || datosComp?.bit_autoriza_oc === 'S';

  logger.debug(`Bloque ${block.bloqueId} OK — total: $${totales.total} auth: ${requiereAutorizacion}`);

  return {
    header: {
      ...header,
      tipoCambio,
      porcentIva: linesEnriquecidas[0]?.porcent_iva ?? 16,
      moneda: moneda,
      ...totales,
      requiereAutorizacion,
      _retencionesEnriquecidas: retencionesEnriquecidas,
    },
    lines:    linesEnriquecidas,
    _provData: provData,
    _ccData:   ccData,
  };
}

module.exports = {
  validarBloque, validarCC, validarProveedor, validarInsumo,
  validarMoneda, validarEmpleado, validarAlmacen, calcularTotales,
};
