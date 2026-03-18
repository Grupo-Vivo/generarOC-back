const express = require('express');
const { handleLogin, handleMe } = require('../controllers/authController');
const { requireAuth } = require('../middleware/auth.middleware');

const router = express.Router();
router.post('/login', handleLogin);
router.get('/me',     requireAuth, handleMe);

module.exports = router;
