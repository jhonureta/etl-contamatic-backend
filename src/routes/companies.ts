import { Router } from 'express';
import { systemworkPool } from '../config/db';

const router = Router();

router.get('/', async (req, res) => {
  try {
    const search = String(req.query.search ?? '').trim();
    const page   = Math.max(1, parseInt(String(req.query.page  ?? '1'),  10));
    const limit  = Math.min(100, Math.max(1, parseInt(String(req.query.limit ?? '10'), 10)));
    const offset = (page - 1) * limit;

    const like = `%${search}%`;

    const [[{ total }]] = await systemworkPool.query<any[]>(
      `SELECT COUNT(*) AS total FROM empresas WHERE NOM_EMPSYS LIKE ? OR RUC_EMPSYS LIKE ? OR BASE_EMP LIKE ?`,
      [like, like, like],
    );

    const [rows] = await systemworkPool.query(
      `SELECT COD_EMPSYS, NOM_EMPSYS, BASE_EMP, MIGRADA
       FROM empresas
       WHERE NOM_EMPSYS LIKE ? OR RUC_EMPSYS LIKE ? OR BASE_EMP LIKE ?
       ORDER BY COD_EMPSYS ASC
       LIMIT ? OFFSET ?`,
      [like, like, like, limit, offset],
    );

    res.json({ data: rows, total, page, limit });
  } catch (err: any) {
    console.error('Error en GET /companies', err);
    res.status(500).json({ message: 'Error interno' });
  }
});

export default router;
