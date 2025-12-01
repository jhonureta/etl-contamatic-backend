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
 * Conexión dinámica para una BD de empresa antigua (BASE_EMP).
 */
export async function createLegacyConnection(opts: {
  host?: string;
  port?: number;
  user: string;
  password: string;
  database: string;
}) {
  return mysql.createConnection({
    host: opts.host || env.systemwork.host,
    port: opts.port || env.systemwork.port,
    user: opts.user,
    password: opts.password,
    database: opts.database,
    multipleStatements: false,
  });
}
