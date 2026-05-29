import express from 'express';
import cors, { CorsOptions } from 'cors';
import authRouter from './routes/auth';
import companiesRouter from './routes/companies';
import migrationsRouter from './routes/migrations';
import { authMiddleware } from './middleware/auth';
import { env } from './config/env';

const app = express();

const corsOptions: CorsOptions = {
  origin: env.corsOrigins,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
};

app.use(cors(corsOptions));
app.use(express.json());

// Rutas públicas
app.use('/auth', authRouter);

// Rutas protegidas
app.use('/companies', authMiddleware, companiesRouter);
app.use('/migrations', authMiddleware, migrationsRouter);

export default app;
