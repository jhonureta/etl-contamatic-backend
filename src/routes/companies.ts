// src/server/routes/companies.ts
import { Router } from 'express';
import { systemworkPool } from '../config/db';

const router = Router();

// Lista empresas a migrar
router.get('/', async (req, res) => {
  try {
    const [rows] = await systemworkPool.query(
      `SELECT COD_EMPSYS, NOM_EMPSYS, BASE_EMP
       FROM empresas
       ORDER BY COD_EMPSYS ASC`,
    );
    res.json(rows);
  } catch (err: any) {
    console.error('Error en GET /companies', err);
    res.status(500).json({ message: 'Error interno' });
  }
});

export default router;
