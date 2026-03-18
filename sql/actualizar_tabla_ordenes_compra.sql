-- =============================================================================
-- Actualización de compras.ORDENES_COMPRA para flujo borrador → generada
-- Base de datos: DWH_SERVICIOS (SQL Server)
-- Ejecutar UNA sola vez. Es seguro re-ejecutar (usa IF NOT EXISTS).
-- =============================================================================

-- ── 1. Columnas de control de flujo ──────────────────────────────────────────
-- BORRADOR  → subido desde Excel, pendiente de confirmar
-- GENERADA  → insertada en Sybase OK
-- CANCELADA → descartada por el usuario
-- ERROR     → intento de generación fallido (detalle en error_msg)
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'estatus')
    ALTER TABLE compras.ORDENES_COMPRA ADD estatus NVARCHAR(15) NOT NULL DEFAULT 'BORRADOR';
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'error_msg')
    ALTER TABLE compras.ORDENES_COMPRA ADD error_msg NVARCHAR(500) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'fecha_generada')
    ALTER TABLE compras.ORDENES_COMPRA ADD fecha_generada DATETIME2 NULL;
GO

-- ── 2. Detalle de partidas en JSON ────────────────────────────────────────────
-- Una fila de cabecera por OC; el detalle de partidas va en JSON.
-- partidas_json: [{ partida, insumo, descripcion, cantidad, precio, importe,
--   iva, porcent_iva, fecha_entrega, unidad, area, cuenta, frente, partida_obra,
--   ppto_status ("OK"|"SIN_PPTO"|"EXCEDIDO"|"NO_APLICA"),
--   ppto_disponible, ppto_comprometido, ppto_msg }]
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'partidas_json')
    ALTER TABLE compras.ORDENES_COMPRA ADD partidas_json NVARCHAR(MAX) NULL;
GO

-- ── 3. Validación de presupuesto ──────────────────────────────────────────────
-- ppto_ok = 1 cuando TODAS las líneas tienen presupuesto OK → se puede confirmar
-- ppto_ok = 0 → una o más líneas bloqueadas, requiere aditiva antes de confirmar
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'ppto_ok')
    ALTER TABLE compras.ORDENES_COMPRA ADD ppto_ok BIT NOT NULL DEFAULT 0;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'ppto_validado_en')
    ALTER TABLE compras.ORDENES_COMPRA ADD ppto_validado_en DATETIME2 NULL;
GO

-- ── 4. Cabecera completa ──────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'libre_abordo')
    ALTER TABLE compras.ORDENES_COMPRA ADD libre_abordo INT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'uso_cfdi')
    ALTER TABLE compras.ORDENES_COMPRA ADD uso_cfdi NVARCHAR(10) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'metodo_pago')
    ALTER TABLE compras.ORDENES_COMPRA ADD metodo_pago NVARCHAR(10) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'almacen')
    ALTER TABLE compras.ORDENES_COMPRA ADD almacen INT NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'comentarios')
    ALTER TABLE compras.ORDENES_COMPRA ADD comentarios NVARCHAR(500) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'pagos_json')
    ALTER TABLE compras.ORDENES_COMPRA ADD pagos_json NVARCHAR(MAX) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'retenciones_json')
    ALTER TABLE compras.ORDENES_COMPRA ADD retenciones_json NVARCHAR(MAX) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'bit_autorecepcion')
    ALTER TABLE compras.ORDENES_COMPRA ADD bit_autorecepcion NCHAR(1) NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'almacen_autorecepcion')
    ALTER TABLE compras.ORDENES_COMPRA ADD almacen_autorecepcion INT NULL;
GO
-- bloque_json: bloque completo serializado para regenerar sin el Excel original
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'bloque_json')
    ALTER TABLE compras.ORDENES_COMPRA ADD bloque_json NVARCHAR(MAX) NULL;
GO

-- ── 5. Índice por estatus ─────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'IX_OC_ESTATUS')
    CREATE INDEX IX_OC_ESTATUS ON compras.ORDENES_COMPRA (estatus, fecha_registro DESC);
GO

PRINT 'Tabla compras.ORDENES_COMPRA actualizada correctamente.';
GO

-- ── 6. Correcciones sobre la estructura original ──────────────────────────────
-- Si se instaló la tabla original (v anterior), aplicar estos cambios:

-- numero_erp: cambiar a NULL para permitir borradores sin número ERP
IF EXISTS (
  SELECT 1 FROM sys.columns
  WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA')
    AND name = 'numero_erp'
    AND is_nullable = 0
)
BEGIN
    -- Quitar constraint UNIQUE anterior si existe
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'UQ_OC_CC_NUMERO')
        ALTER TABLE compras.ORDENES_COMPRA DROP CONSTRAINT UQ_OC_CC_NUMERO;

    ALTER TABLE compras.ORDENES_COMPRA ALTER COLUMN numero_erp INT NULL;
    PRINT 'numero_erp cambiado a NULL OK';
END
GO

-- Crear índice único FILTRADO (solo filas con numero_erp no nulo = GENERADAS)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'UQ_OC_CC_NUMERO_ERP')
    CREATE UNIQUE INDEX UQ_OC_CC_NUMERO_ERP
      ON compras.ORDENES_COMPRA (cc, numero_erp)
      WHERE numero_erp IS NOT NULL;
GO

-- st_impresa: cambiar a NULL (se asigna solo al confirmar)
IF EXISTS (
  SELECT 1 FROM sys.columns
  WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA')
    AND name = 'st_impresa'
    AND is_nullable = 0
)
BEGIN
    ALTER TABLE compras.ORDENES_COMPRA ALTER COLUMN st_impresa NCHAR(1) NULL;
    PRINT 'st_impresa cambiado a NULL OK';
END
GO

PRINT 'Correcciones de estructura aplicadas.';
GO

-- ── 7. Campo origen ───────────────────────────────────────────────────────────
-- Identifica desde dónde se creó la OC
-- EXCEL    → subida desde la app web (flujo Excel → borrador → confirmar)
-- AIRTABLE → recibida desde el endpoint de Airtable
-- API      → cualquier otro cliente de la API
IF NOT EXISTS (
  SELECT 1 FROM sys.columns
  WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'origen'
)
    ALTER TABLE compras.ORDENES_COMPRA
      ADD origen NVARCHAR(15) NOT NULL DEFAULT 'EXCEL';
GO

-- Índice para filtrar por origen
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('compras.ORDENES_COMPRA') AND name = 'IX_OC_ORIGEN')
    CREATE INDEX IX_OC_ORIGEN ON compras.ORDENES_COMPRA (origen);
GO

PRINT 'Campo origen agregado correctamente.';
GO
