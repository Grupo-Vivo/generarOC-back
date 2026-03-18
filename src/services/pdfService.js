/**
 * pdfService.js
 *
 * Genera el PDF de una OC consultando Sybase y llamando al script Python.
 *
 * Flujo:
 *   1. Consulta so_orden_compra + joins para obtener todos los datos
 *   2. Consulta partidas, pagos, retenciones
 *   3. Consulta datos_compania (empresa facturadora)
 *   4. Arma el JSON y llama a generar_oc_pdf.py via child_process
 *   5. Retorna el buffer del PDF
 */
const path     = require('path');
const fs       = require('fs');
const os       = require('os');
const { execFile } = require('child_process');
const { promisify } = require('util');
const execFileAsync = promisify(execFile);
const logger   = require('../utils/logger');

// Ruta al script Python (mismo directorio que el backend)
const PYTHON_SCRIPT = path.join(__dirname, '..', '..', 'scripts', 'generar_oc_pdf.py');
const PYTHON_BIN    = process.env.PYTHON_BIN || 'python3';

// ─────────────────────────────────────────────────────────────────────────────
// Consultas
// ─────────────────────────────────────────────────────────────────────────────

async function consultarCabecera(conn, cc, numero) {
  const rows = await conn.query(`
    SELECT
      oc.cc, oc.numero, oc.fecha, oc.proveedor, oc.moneda, oc.tipo_cambio,
      oc.sub_total, oc.iva, oc.total, oc.porcent_iva,
      oc.comentarios, oc.solicito, oc.vobo, oc.autorizo,
      oc.embarquese, oc.almacen, oc.bienes_servicios,
      oc.uso_cfdi, oc.cfd_metodo_pago_sat as metodo_pago,
      oc.comprador, oc.libre_abordo, oc.bit_autorecepcion,
      oc.rentencion_antes_iva, oc.rentencion_despues_iva,
      cc.descripcion as cc_desc,
      la.descripcion as libre_abordo_desc,
      prov.nombre    as prov_nombre,
      prov.direccion as prov_dir,
      prov.ciudad    as prov_ciudad_id,
      prov.telefono1 as prov_tel,
      prov.fax       as prov_fax,
      ciu.desc_ciudad as prov_ciudad,
      est.desc_estado as prov_estado,
      pai.desc_pais   as prov_pais,
      emp_comp.descripcion as comprador_nombre,
      pst_comp.descripcion as comprador_puesto,
      emp_sol.descripcion  as elaboro_nombre,
      pst_sol.descripcion  as elaboro_puesto,
      emp_rev.descripcion  as reviso_nombre,
      pst_rev.descripcion  as reviso_puesto,
      emp_aut.descripcion  as autorizo_nombre,
      pst_aut.descripcion  as autorizo_puesto
    FROM so_orden_compra oc
    JOIN cc        ON cc.cc         = oc.cc
    JOIN sp_proveedores prov ON prov.numpro = oc.proveedor
    LEFT JOIN ciudades  ciu  ON ciu.ciudad   = prov.ciudad
    LEFT JOIN estados   est  ON est.estado   = ciu.estado
    LEFT JOIN paises    pai  ON pai.pais      = est.pais
    LEFT JOIN so_libre_abordo la ON la.numero  = oc.libre_abordo
    LEFT JOIN empleados emp_comp ON emp_comp.empleado = oc.comprador
    LEFT JOIN si_puestos pst_comp ON pst_comp.puesto  = emp_comp.puesto
    LEFT JOIN empleados emp_sol  ON emp_sol.empleado  = oc.solicito
    LEFT JOIN si_puestos pst_sol ON pst_sol.puesto    = emp_sol.puesto
    LEFT JOIN empleados emp_rev  ON emp_rev.empleado  = oc.vobo
    LEFT JOIN si_puestos pst_rev ON pst_rev.puesto    = emp_rev.puesto
    LEFT JOIN empleados emp_aut  ON emp_aut.empleado  = oc.autorizo
    LEFT JOIN si_puestos pst_aut ON pst_aut.puesto    = emp_aut.puesto
    WHERE oc.cc = ? AND oc.numero = ?`,
    [cc, numero]
  );
  if (!rows.length) throw new Error(`OC ${cc}-${numero} no encontrada.`);
  return rows[0];
}

async function consultarPartidas(conn, cc, numero) {
  return conn.query(`
    SELECT
      det.partida, det.insumo, det.fecha_entrega,
      det.cantidad, det.precio, det.importe, det.porcent_iva,
      det.area, det.cuenta, det.obra,
      ins.descripcion as insumo_desc,
      ins.unidad,
      lin.descripcion as descripcion_det
    FROM so_orden_compra_det det
    JOIN insumos ins ON ins.insumo = det.insumo
    LEFT JOIN so_orden_det_linea lin
           ON lin.cc = det.cc AND lin.numero = det.numero AND lin.partida = det.partida
    WHERE det.cc = ? AND det.numero = ?
    ORDER BY det.partida`,
    [cc, numero]
  );
}

async function consultarPagos(conn, cc, numero) {
  return conn.query(`
    SELECT pago.partida, pago.dias_pago, pago.porcentaje, pago.importe,
           pago.fecha_pago, pago.comentarios, pago.estatus
    FROM so_orden_compra_pago pago
    WHERE pago.cc = ? AND pago.numero = ?
    ORDER BY pago.partida`,
    [cc, numero]
  );
}

async function consultarRetenciones(conn, cc, numero) {
  return conn.query(`
    SELECT
      ort.id_cpto, ort.orden, ort.porc_ret,
      ort.importe * -1 AS importe,   -- negativo = reduce el total
      ort.cantidad,
      ret.desc_ret, ret.naturaleza_ret
    FROM so_ordenc_retenciones ort
    JOIN so_retenciones ret ON ret.id_cpto = ort.id_cpto
    WHERE ort.cc = ? AND ort.numero = ?
    ORDER BY ort.orden`,
    [cc, numero]
  );
}

async function consultarEmpresa(conn) {
  const rows = await conn.query(`
    SELECT nombre, direccion, rfc FROM datos_compania`);
  return rows[0] || { nombre: '', direccion: '', rfc: '' };
}

// ─────────────────────────────────────────────────────────────────────────────
// Descripción del movimiento de pago
// ─────────────────────────────────────────────────────────────────────────────
function descMovimiento(dias) {
  const d = Number(dias);
  if (d === 0) return 'Después de Recibir Factura';
  if (d < 0)   return 'Anticipado';
  return `A ${d} días`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Formatea fecha Sybase → "DD/Mmm/YYYY"
// ─────────────────────────────────────────────────────────────────────────────
const MESES = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'];
function fmtFecha(f) {
  if (!f) return '';
  const d = new Date(f);
  if (isNaN(d)) return String(f).substring(0, 10);
  return `${String(d.getDate()).padStart(2,'0')}/${MESES[d.getMonth()]}/${d.getFullYear()}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// FUNCIÓN PRINCIPAL — arma el JSON y llama al script Python
// ─────────────────────────────────────────────────────────────────────────────
async function generarPdfOC(conn, cc, numero, logoPath = null) {
  logger.info(`Generando PDF OC ${cc}-${numero}`);

  const [cab, partidas, pagos, retenciones, empresa] = await Promise.all([
    consultarCabecera(conn, cc, numero),
    consultarPartidas(conn, cc, numero),
    consultarPagos(conn, cc, numero),
    consultarRetenciones(conn, cc, numero),
    consultarEmpresa(conn),
  ]);

  const data = {
    empresa: {
      nombre:    empresa.nombre?.trim()    || '',
      direccion: empresa.direccion?.trim() || '',
      rfc:       empresa.rfc?.trim()       || '',
      logo_path: logoPath || null,
    },
    oc: {
      cc:                   cab.cc,
      numero:               Number(cab.numero),
      fecha:                fmtFecha(cab.fecha),
      cc_desc:              cab.cc_desc?.trim() || '',
      libre_abordo_num:     cab.libre_abordo,
      libre_abordo_desc:    cab.libre_abordo_desc?.trim() || '',
      proveedor_num:        cab.proveedor,
      proveedor_nombre:     cab.prov_nombre?.trim() || '',
      proveedor_direccion:  cab.prov_dir?.trim() || '',
      proveedor_ciudad:     cab.prov_ciudad?.trim() || '',
      proveedor_pais:       `${cab.prov_estado?.trim() || ''} ${cab.prov_pais?.trim() || ''}`.trim(),
      proveedor_tel:        cab.prov_tel?.trim() || '',
      proveedor_fax:        cab.prov_fax?.trim() || '',
      comprador_num:        cab.comprador,
      comprador_nombre:     cab.comprador_nombre?.trim() || '',
      comprador_puesto:     cab.comprador_puesto?.trim() || '',
      comentarios:          cab.comentarios?.trim() || '',
      requisicion:          '',
      embarquese:           cab.embarquese?.trim() || '',
      almacen:              cab.almacen != null ? String(cab.almacen) : '',
      bienes_servicios:     cab.bienes_servicios?.trim() || '',
      uso_cfdi:             cab.uso_cfdi?.trim() || '',
      metodo_pago:          cab.metodo_pago?.trim() || '',
      moneda:               cab.moneda?.trim() || 'MXN',
      moneda_desc:          cab.moneda?.trim() === 'MXN' ? 'PESOS' : (cab.moneda?.trim() || ''),
      tipo_cambio:          Number(cab.tipo_cambio) || 1,
      porcent_iva:          Number(cab.porcent_iva) || 0,
      sub_total:            Number(cab.sub_total)   || 0,
      iva:                  Number(cab.iva)          || 0,
      total:                Number(cab.total)        || 0,
      elaboro_nombre:       cab.elaboro_nombre?.trim() || '',
      elaboro_puesto:       cab.elaboro_puesto?.trim() || '',
      reviso_nombre:        cab.reviso_nombre?.trim()  || '',
      reviso_puesto:        cab.reviso_puesto?.trim()  || '',
      autorizo_nombre:      cab.autorizo_nombre?.trim() || '',
      autorizo_puesto:      cab.autorizo_puesto?.trim() || '',
    },
    partidas: partidas.map(pd => ({
      partida:        Number(pd.partida),
      insumo:         pd.insumo,
      descripcion:    pd.insumo_desc?.trim() || '',
      descripcion_det:pd.descripcion_det?.trim() || '',
      area:           pd.area  != null ? pd.area  : null,
      cuenta:         pd.cuenta != null ? pd.cuenta : null,
      fecha_entrega:  fmtFecha(pd.fecha_entrega),
      cantidad:       Number(pd.cantidad),
      unidad:         pd.unidad?.trim() || '',
      precio:         Number(pd.precio),
      importe:        Number(pd.importe),
    })),
    pagos: pagos.map(pg => ({
      dias_pago:   Number(pg.dias_pago),
      movimiento:  descMovimiento(pg.dias_pago),
      porcentaje:  Number(pg.porcentaje),
      importe:     Number(pg.importe),
    })),
    retenciones: retenciones.map(r => ({
      id_cpto:  r.id_cpto,
      desc_ret: r.desc_ret?.trim() || '',
      porc_ret: Number(r.porc_ret),
      importe:  Number(r.importe),   // ya viene negativo desde el SQL
      cantidad: Number(r.cantidad),
    })),
  };

  // Escribe JSON temporal y llama al script Python
  const tmpJson = path.join(os.tmpdir(), `oc_${cc}_${numero}_${Date.now()}.json`);
  const tmpPdf  = path.join(os.tmpdir(), `oc_${cc}_${numero}_${Date.now()}.pdf`);

  try {
    fs.writeFileSync(tmpJson, JSON.stringify(data), 'utf8');

    await execFileAsync(PYTHON_BIN, [PYTHON_SCRIPT, tmpJson, tmpPdf], {
      timeout: 30_000,
    });

    const buffer = fs.readFileSync(tmpPdf);
    logger.info(`PDF generado OK: ${tmpPdf} (${buffer.length} bytes)`);
    return buffer;

  } finally {
    try { fs.unlinkSync(tmpJson); } catch (_) {}
    try { fs.unlinkSync(tmpPdf);  } catch (_) {}
  }
}

module.exports = { generarPdfOC };
