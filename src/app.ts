// src/server/app.ts
import express from 'express';
import cors, { CorsOptions } from 'cors';
import authRouter from './routes/auth';
import companiesRouter from './routes/companies';
import migrationsRouter from './routes/migrations';
import { authMiddleware } from './middleware/auth';

const app = express();

const allowedOrigins = [
  'http://localhost:5173',
  'http://127.0.0.1:5173',
];

const corsOptions: CorsOptions = {
  origin: allowedOrigins,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
  // preflightContinue: false,        // por defecto
  // optionsSuccessStatus: 204,
};

app.use(cors(corsOptions));   // 👈 solo esto
app.use(express.json());

// Rutas públicas
app.use('/auth', authRouter);

// Rutas protegidas
app.use('/companies', authMiddleware, companiesRouter);
app.use('/migrations', authMiddleware, migrationsRouter);

export default app;
