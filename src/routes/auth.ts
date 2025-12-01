import { Router } from 'express';
import bcrypt from 'bcrypt';
import jwt from 'jsonwebtoken';
import { systemworkPool } from '../config/db';

const router = Router();
const JWT_SECRET = 'super-secret-change-me';

router.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    const [rows] = await systemworkPool.query(
      'SELECT id, username, password_hash, role, is_active FROM etl_users WHERE username = ?',
      [username],
    );

    const users = rows as any[];
    console.log(users);
    if (!users.length) {
      return res.status(401).json({ message: 'Credenciales inválidas' });
    }

    const user = users[0];

    if (!user.is_active) {
      return res.status(403).json({ message: 'Usuario inactivo' });
    }

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) {
      return res.status(401).json({ message: 'Credenciales inválidas' });
    }

    const token = jwt.sign(
      { id: user.id, username: user.username, role: user.role },
      JWT_SECRET,
      { expiresIn: '8h' },
    );

    return res.json({ token, user: { id: user.id, username: user.username, role: user.role } });
  } catch (err: any) {
    console.error('Error en /auth/login', err);
    return res.status(500).json({ message: 'Error interno' });
  }
});

export default router;
