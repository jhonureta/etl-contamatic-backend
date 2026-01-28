import { Request, Response, NextFunction } from 'express';
import jwt, { TokenExpiredError, JsonWebTokenError } from 'jsonwebtoken';

const JWT_SECRET =  'super-secret-change-me';

export interface AuthRequest extends Request {
  user?: { id: number; username: string; role: string };
}

export function authMiddleware(
  req: AuthRequest,
  res: Response,
  next: NextFunction,
) {
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    return res.status(401).json({ message: 'No token provided' });
  }

  const [scheme, token] = authHeader.split(' ');

  if (scheme !== 'Bearer' || !token) {
    return res.status(401).json({ message: 'Invalid auth header format' });
  }

  try {
    const payload = jwt.verify(token, JWT_SECRET) as any;

    req.user = {
      id: payload.id,
      username: payload.username,
      role: payload.role,
    };
    next();
  } catch (err) {
    console.error('>> jwt error:', err);

    if (err instanceof TokenExpiredError) {
      return res.status(401).json({ message: 'Token expired' });
    }
    if (err instanceof JsonWebTokenError) {
      return res.status(401).json({ message: 'Invalid token signature' });
    }

    return res.status(401).json({ message: 'Invalid token' });
  }
}
