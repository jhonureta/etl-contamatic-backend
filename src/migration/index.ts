import { migrateCompany } from './migrateCompany';

async function main() {
  const arg = process.argv[2];

  if (!arg) {
    console.error('Uso: npm run migrate-company -- <COD_EMPSYS>');
    process.exit(1);
  }

  const codEmp = Number(arg);
  if (Number.isNaN(codEmp)) {
    console.error('COD_EMPSYS debe ser numérico');
    process.exit(1);
  }

  try {
    console.log(`Iniciando migración de empresa COD_EMPSYS=${codEmp}...`);
    await migrateCompany(codEmp);
    console.log('Migración terminada correctamente');
  } catch (err) {
    console.error('Error durante la migración:', err);
    process.exit(1);
  }
}

main();
