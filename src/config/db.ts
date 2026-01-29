import mysql from 'mysql2/promise';
import { env } from './env';

export const systemworkPool = mysql.createPool({
  host: env.systemwork.host,
  port: env.systemwork.port,
  user: env.systemwork.user,
  password: env.systemwork.password,
  database: env.systemwork.database,
  multipleStatements: false,
});

export const erpPool = mysql.createPool({
  host: env.erp.host,
  port: env.erp.port,
  user: env.erp.user,
  password: env.erp.password,
  database: env.erp.database,
  multipleStatements: false,
});

/**
 * Conexión dinámica para BD de empresa antigua (BASE_EMP).
 */
/**
 * Pool dinámico para BD de empresa antigua (BASE_EMP).
 * Reemplaza createLegacyConnection() para evitar "connection is in closed state".
 */
export function createLegacyPool(opts: {
  host?: string;
  port?: number;
  user: string;
  password: string;
  database: string;
  connectionLimit?: number;
}) {
  return mysql.createPool({
    host: opts.host || env.systemwork.host,
    port: opts.port || env.systemwork.port,
    user: opts.user,
    password: opts.password,
    database: opts.database,
    waitForConnections: true,
    connectionLimit: opts.connectionLimit ?? 5,
    queueLimit: 0,
    multipleStatements: false,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0,
    connectTimeout: 30000,
  });
}