import { Router } from 'express';
import { migrateCompany } from '../migration/migrateCompany';
import { createLegacyConnection } from '../config/db';

const router = Router();

router.post('/company/:codEmp', async (req, res) => {
  const codEmp = Number(req.params.codEmp);
  if (!codEmp) {
    return res.status(400).json({ message: 'codEmp inválido' });
  }

  try {
    console.log(`API -> migrando empresa COD_EMPSYS=${codEmp}`);
    const result = await migrateCompany(codEmp);
    return res.json({ ok: true,message: "Empresa migrada correctamente", result });
  } catch (err: any) {
    console.error('Error en /migrations/company/:codEmp', err);
    return res.status(500).json({ message: err.message || 'Error en migración' });
  }
});

export default router;
