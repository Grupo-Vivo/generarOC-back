const express = require('express');
const { generarPdfOC } = require('../services/pdfService');
const db = require('../config/db');

const router = express.Router();

// GET /api/oc/:cc/:numero/pdf — descarga el PDF de una OC ya generada
router.get('/:cc/:numero', async (req, res) => {
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