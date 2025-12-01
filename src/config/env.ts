import dotenv from 'dotenv';

dotenv.config();

export const env = {
  systemwork: {
    host: process.env.SYSTEMWORK_DB_HOST || 'localhost',
    port: Number(process.env.SYSTEMWORK_DB_PORT || 3306),
    user: process.env.SYSTEMWORK_DB_USER || 'root',
    password: process.env.SYSTEMWORK_DB_PASSWORD || '',
    database: process.env.SYSTEMWORK_DB_DATABASE || 'contamatic_systemwork',
  },
  erp: {
    host: process.env.ERP_DB_HOST || 'localhost',
    port: Number(process.env.ERP_DB_PORT || 3306),
    user: process.env.ERP_DB_USER || 'root',
    password: process.env.ERP_DB_PASSWORD || '',
    database: process.env.ERP_DB_DATABASE || 'contamatic_erp',
  },
};
