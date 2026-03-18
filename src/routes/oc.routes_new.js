const express = require('express');
const multer  = require('multer');
const path    = require('path');
const fs      = require('fs');
const { uploadExcel, generarOC, getColumnas, getHistorial,
        guardarBorradores, revalidarBorrador,
        confirmarBorradorCtrl, cancelarBorradorCtrl } = require('../controllers/ocController');
const { generarPdfOC } = require('../services/pdfService');
const db = require('../config/db');

const router = express.Router();

const upDir = process.env.UPLOAD_DIR || './uploads';
if (!fs.existsSync(upDir)) fs.mkdirSync(upDir, { recursive: true });

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, upDir),
  filename:    (req, file, cb) =>
    cb(null, `excel_${Date.now()}${path.extname(file.originalname)}`),
});

const upload = multer({
  storage,
  limits: { fileSize: (parseInt(process.env.MAX_FILE_SIZE_MB, 10) || 10) * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const ext = path.extname(file.originalname).toLowerCase();
    ['.xlsx', '.xls'].includes(ext)
      ? cb(null, true)
      : cb(new Error('Solo se aceptan archivos .xlsx y .xls'));
  },
});

router.get('/historial', getHistorial);

// ── Flujo borrador ──────────────────────────────────────────────────────────
// 1. Subir Excel            → POST /upload         (existente)
// 2. Guardar borradores     → POST /borrador        (nuevo)
// 3. Re-validar presupuesto → POST /borrador/:id/revalidar
// 4. Confirmar → Sybase     → POST /borrador/:id/confirmar (SSE)
// 5. Cancelar               → DELETE /borrador/:id
router.post('/borrador',                    guardarBorradores);
router.post('/borrador/:id/revalidar',      revalidarBorrador);
router.post('/borrador/:id/confirmar',      confirmarBorradorCtrl);
router.delete('/borrador/:id',              cancelarBorradorCtrl);
router.post('/upload',  upload.single('archivo'), uploadExcel);
router.post('/generar', generarOC);
router.get('/columnas', getColumnas);
router.get('/health',   (req, res) => res.json({ ok: true }));

// GET /api/oc/:cc/:numero/pdf — descarga el PDF de una OC ya generada
router.get('/:cc/:numero/pdf', async (req, res) => {
  const { cc, numero } = req.params;
  const logoPath = process.env.LOGO_PATH || null;
  try {
    // Usamos withTransaction para tener una conexión del pool Sybase
    let pdfBuffer;
    await db.withTransaction(async conn => {
      pdfBuffer = await generarPdfOC(conn, cc, Number(numero), logoPath);
    });
    const filename  = `OC_${cc}_${String(numero).padStart(6,'0')}.pdf`;
    res.setHeader('Content-Type',        'application/pdf');
    res.setHeader('Content-Disposition', `inline; filename="${filename}"`);
    res.setHeader('Content-Length',      pdfBuffer.length);
    res.end(pdfBuffer);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

module.exports = router;
