-- =============================================================================
-- DDL: compras.ORDENES_COMPRA
-- Base de datos: DWH_SERVICIOS (SQL Server)
-- Ejecutar UNA sola vez antes de iniciar la aplicación.
-- =============================================================================

-- Crear schema si no existe
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'compras')
    EXEC('CREATE SCHEMA compras');
GO

CREATE TABLE compras.ORDENES_COMPRA (
    -- PK autoincremental
    id                  INT             IDENTITY(1,1)   NOT NULL,

    -- Datos de la OC en Enkontrol (Sybase)
    cc                  NVARCHAR(10)    NOT NULL,           -- Centro de costo
    numero_erp          INT             NULL,               -- NULL hasta que se confirma en Sybase
    proveedor           INT             NOT NULL,           -- Clave proveedor
    proveedor_nombre    NVARCHAR(200)   NULL,               -- Nombre proveedor (desnormalizado)
    fecha_oc            DATE            NOT NULL,           -- Fecha de la OC (YYYY-MM-DD)
    moneda              NVARCHAR(5)     NOT NULL DEFAULT 'MXN',
    tipo_cambio         DECIMAL(18,6)   NOT NULL DEFAULT 1,
    sub_total           DECIMAL(18,4)   NOT NULL DEFAULT 0,
    iva                 DECIMAL(18,4)   NOT NULL DEFAULT 0,
    total               DECIMAL(18,4)   NOT NULL DEFAULT 0,
    partidas            INT             NOT NULL DEFAULT 0, -- Número de líneas/partidas
    retenciones         INT             NOT NULL DEFAULT 0, -- Número de retenciones
    autorecepcion       BIT             NOT NULL DEFAULT 0, -- 1 = se autoreció al generar
    st_impresa          NCHAR(1)        NULL,               -- Se asigna 'I' al confirmar
    estatus             NVARCHAR(15)    NOT NULL DEFAULT 'BORRADOR', -- BORRADOR|GENERADA|CANCELADA|ERROR
    error_msg           NVARCHAR(500)   NULL,
    fecha_generada      DATETIME2       NULL,
    ppto_ok             BIT             NOT NULL DEFAULT 0,
    ppto_validado_en    DATETIME2       NULL,

    -- Trazabilidad de quién generó desde la app
    usuario_key         NVARCHAR(20)    NOT NULL,           -- Login del usuario (JWT)
    id_ek               INT             NULL,               -- Empleado en Enkontrol (TI.USUARIO_TI.id_Ek)
    bloque_excel        NVARCHAR(100)   NULL,               -- ID del bloque en el Excel subido

    -- Auditoría
    fecha_registro      DATETIME2       NOT NULL DEFAULT GETDATE(),

    CONSTRAINT PK_ORDENES_COMPRA PRIMARY KEY (id),
);
GO

-- Índice único filtrado: solo aplica cuando ya tiene número ERP (GENERADA)
-- Evita duplicados de OC reales sin bloquear múltiples borradores del mismo CC
CREATE UNIQUE INDEX UQ_OC_CC_NUMERO_ERP
  ON compras.ORDENES_COMPRA (cc, numero_erp)
  WHERE numero_erp IS NOT NULL;
GO

-- Índices de búsqueda (para los filtros de la pantalla de historial)
CREATE INDEX IX_OC_CC            ON compras.ORDENES_COMPRA (cc);
CREATE INDEX IX_OC_PROVEEDOR     ON compras.ORDENES_COMPRA (proveedor);
CREATE INDEX IX_OC_FECHA         ON compras.ORDENES_COMPRA (fecha_oc);
CREATE INDEX IX_OC_USUARIO       ON compras.ORDENES_COMPRA (usuario_key);
CREATE INDEX IX_OC_FECHA_REG     ON compras.ORDENES_COMPRA (fecha_registro DESC);
GO

PRINT 'Tabla compras.ORDENES_COMPRA creada correctamente.';
GO
