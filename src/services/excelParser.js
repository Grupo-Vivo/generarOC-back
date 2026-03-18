/**
 * excelParser.js
 *
 * ── COLUMNAS NUEVAS vs versión anterior ──────────────────────────────────────
 * Cabecera: almacen, uso_cfdi, metodo_pago, concepto_factura, id_lugar,
 *           bit_autorecepcion, almacen_autorecepcion, empleado_autorecepcion
 * Detalle:  frente, partida_obra
 *
 * PAGOS (hasta 5 plazos por bloque, leídos del primer registro):
 *   pago_1_dias, pago_1_pct, pago_1_comentarios
 *   pago_2_dias, pago_2_pct, pago_2_comentarios  ...hasta pago_5_*
 *   Alternativa de un solo plazo: pago_dias, pago_pct, pago_comentarios
 *
 * RETENCIONES (hasta 5 por bloque, leídas del primer registro):
 *   ret_1_id_cpto, ret_1_porc
 *   ret_2_id_cpto, ret_2_porc  ...hasta ret_5_*
 */
const XLSX   = require('xlsx');
const logger = require('../utils/logger');

const COLUMN_MAP = {
  bloque:                'bloque',
  // ── Cabecera ─────────────────────────────────────────────────────────────
  cc:                    'cc',
  proveedor:             'proveedor',
  moneda:                'moneda',
  tipo_cambio:           'tipo_cambio',
  fecha:                 'fecha',
  comprador:             'comprador',
  solicito:              'solicito',
  vobo:                  'vobo',
  autorizo:              'autorizo',
  comentarios:           'comentarios',
  libre_abordo:          'libre_abordo',
  embarquese:            'embarquese',
  bienes_servicios:      'bienes_servicios',
  almacen:               'almacen',
  uso_cfdi:              'uso_cfdi',
  metodo_pago:           'metodo_pago',
  concepto_factura:      'concepto_factura',
  id_lugar:              'id_lugar',
  // ── Autorecepción ────────────────────────────────────────────────────────
  bit_autorecepcion:     'bit_autorecepcion',
  almacen_autorecepcion: 'almacen_autorecepcion',
  empleado_autorecepcion:'empleado_autorecepcion',
  // ── Detalle ──────────────────────────────────────────────────────────────
  insumo:                'insumo',
  cantidad:              'cantidad',
  precio:                'precio',
  fecha_entrega:         'fecha_entrega',
  porcent_iva:           'porcent_iva',
  area:                  'area',
  cuenta:                'cuenta',
  obra:                  'obra',
  multi_cc:              'multi_cc',
  frente:                'frente',
  partida_obra:          'partida_obra',
  descripcion_det:       'descripcion_det',
  num_requisicion:       'num_requisicion',
  part_requisicion:      'part_requisicion',
};

const CAMPOS_CABECERA = [
  'cc','proveedor','moneda','tipo_cambio','fecha','comprador','solicito',
  'vobo','autorizo','comentarios','libre_abordo','embarquese','bienes_servicios',
  'almacen','uso_cfdi','metodo_pago','concepto_factura','id_lugar',
  'bit_autorecepcion','almacen_autorecepcion','empleado_autorecepcion',
];
const CAMPOS_DETALLE = [
  'insumo','cantidad','precio','fecha_entrega','porcent_iva',
  'area','cuenta','obra','multi_cc','frente','partida_obra',
  'descripcion_det','num_requisicion','part_requisicion',
];
const CAMPOS_REQUERIDOS = ['bloque','cc','proveedor','insumo','cantidad','precio','fecha_entrega'];

const MAX_PAGOS       = 5;
const MAX_RETENCIONES = 5;

function normalizar(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'string') { const t = val.trim(); return t === '' ? null : t; }
  return val;
}

function leerPagos(row) {
  const pagos = [];
  for (let i = 1; i <= MAX_PAGOS; i++) {
    const dias = normalizar(row[`pago_${i}_dias`]);
    const pct  = normalizar(row[`pago_${i}_pct`]);
    if (dias === null && pct === null) continue;
    pagos.push({
      orden:       i,
      dias_pago:   Number(dias || 0),
      porcentaje:  Number(pct  || 100),
      comentarios: normalizar(row[`pago_${i}_comentarios`]) || '',
      estatus:     'P',
    });
  }
  // Columna única de pago si no hay multi
  if (!pagos.length) {
    const dias = normalizar(row['pago_dias']);
    if (dias !== null) {
      pagos.push({
        orden:       1,
        dias_pago:   Number(dias),
        porcentaje:  Number(normalizar(row['pago_pct']) || 100),
        comentarios: normalizar(row['pago_comentarios']) || '',
        estatus:     'P',
      });
    }
  }
  return pagos;
}

function leerRetenciones(row) {
  const rets = [];
  for (let i = 1; i <= MAX_RETENCIONES; i++) {
    const id_cpto = normalizar(row[`ret_${i}_id_cpto`]);
    if (!id_cpto) continue;
    rets.push({
      orden:   i,
      id_cpto: Number(id_cpto),
      porc:    Number(normalizar(row[`ret_${i}_porc`]) || 0),
    });
  }
  return rets;
}

function parseExcel(filePath) {
  logger.info('Parseando Excel:', filePath);
  let workbook;
  try {
    workbook = XLSX.readFile(filePath, { cellDates: true, defval: null });
  } catch (err) {
    throw new Error(`No se pudo leer el archivo Excel: ${err.message}`);
  }

  const sheet   = workbook.Sheets[workbook.SheetNames[0]];
  const rawRows = XLSX.utils.sheet_to_json(sheet, { defval: null, raw: false });

  if (!rawRows.length) throw new Error('El archivo Excel está vacío.');

  const colsPresentes = Object.keys(rawRows[0]);
  const faltantes = CAMPOS_REQUERIDOS
    .map(f => COLUMN_MAP[f])
    .filter(col => !colsPresentes.includes(col));

  if (faltantes.length)
    throw new Error(
      `Columnas requeridas faltantes: [${faltantes.join(', ')}].\n` +
      `Encontradas: [${colsPresentes.join(', ')}]`
    );

  const warnings = [], errors = [], blockMap = new Map();

  rawRows.forEach((row, idx) => {
    const excelRow  = idx + 2;
    const bloqueRaw = normalizar(row[COLUMN_MAP.bloque]);

    if (!bloqueRaw) {
      warnings.push(`Fila ${excelRow}: "bloque" vacío — fila ignorada.`);
      return;
    }

    const bloqueId = String(bloqueRaw).trim();

    if (!blockMap.has(bloqueId)) {
      const header = {};
      CAMPOS_CABECERA.forEach(f => { header[f] = normalizar(row[COLUMN_MAP[f]]); });
      header._pagos       = leerPagos(row);
      header._retenciones = leerRetenciones(row);
      blockMap.set(bloqueId, { bloqueId, rowStart: excelRow, header, lines: [] });
    }

    const line = { _excelRow: excelRow };
    CAMPOS_DETALLE.forEach(f => { line[f] = normalizar(row[COLUMN_MAP[f]]); });

    if (!line.insumo) {
      warnings.push(`Fila ${excelRow} (bloque ${bloqueId}): "insumo" vacío — ignorada.`);
      return;
    }
    if (!line.fecha_entrega) {
      errors.push(`Fila ${excelRow} (bloque ${bloqueId}): "fecha_entrega" requerida.`);
      return;
    }
    if (isNaN(Number(line.cantidad)) || Number(line.cantidad) <= 0) {
      errors.push(`Fila ${excelRow} (bloque ${bloqueId}): cantidad inválida "${line.cantidad}".`);
      return;
    }
    if (isNaN(Number(line.precio)) || Number(line.precio) < 0) {
      errors.push(`Fila ${excelRow} (bloque ${bloqueId}): precio inválido "${line.precio}".`);
      return;
    }

    line.cantidad    = Number(line.cantidad);
    line.precio      = Number(line.precio);
    line.porcent_iva = line.porcent_iva != null ? Number(line.porcent_iva) : 16;

    blockMap.get(bloqueId).lines.push(line);
  });

  const blocks = [];
  blockMap.forEach((block, key) => {
    if (!block.lines.length)
      warnings.push(`Bloque "${key}": sin líneas válidas — ignorado.`);
    else
      blocks.push(block);
  });

  logger.info(`Excel parseado: ${blocks.length} bloques, ${warnings.length} warns, ${errors.length} errors.`);
  return { blocks, warnings, errors };
}

function getColumnMap() { return { ...COLUMN_MAP }; }
module.exports = { parseExcel, getColumnMap, COLUMN_MAP };
