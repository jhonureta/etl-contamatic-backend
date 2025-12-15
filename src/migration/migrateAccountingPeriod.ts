/* import { erpPool } from '../config/db';
 */
export async function migrateAccountingPeriod(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {
    console.log("Migrando periodo contable...");

    const [rows] = await legacyConn.query(`SELECT cod_periodo as  COD_PERIODO, anioperiodo AS ANIO_PERIODO,numeromes as NUMERO_MES, fecha_inicio as FECHA_INICIO, fecha_cierre as FECHA_CIERRE, estado_cierre as ESTADO_CIERRE FROM contabilidad_periodo WHERE 1;`);
    const periodo = rows as any[];

    if (!periodo.length) {
        throw new Error(" -> No hay periodo contable para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapPeriodo: Record<number, number> = {};
    for (let i = 0; i < periodo.length; i += BATCH_SIZE) {
        const batch = periodo.slice(i, i + BATCH_SIZE);

        const values = batch.map(u => {

            return [
                u.ANIO_PERIODO,
                u.NUMERO_MES,
                u.FECHA_INICIO,
                u.FECHA_CIERRE,
                u.ESTADO_CIERRE,
                newCompanyId
            ];
        });
        try {
            const [res]: any = await conn.query(`
                 INSERT INTO accounting_period (ANIO_PERIODO, NUMERO_MES, FECHA_INICIO,FECHA_CIERRE, ESTADO_CIERRE, FK_COD_EMP ) VALUES ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapPeriodo[s.COD_PERIODO] = newId;
                newId++;
            }


            console.log(` -> Batch migrado: ${batch.length} sucursales`);
        } catch (err) {
            throw err;
        }
    }

    return mapPeriodo;
}


