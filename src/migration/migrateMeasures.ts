/* import { erpPool } from '../config/db';
 */
export async function migrateMeasures(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {
    console.log("Migrando medidas...");

    const [rows] = await legacyConn.query(`SELECT medidas.* FROM medidas ;`);
    const categorias = rows as any[];

    if (!categorias.length) {
        throw new Error(" -> No hay medidas para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapMeasures: Record<number, number> = {};
    for (let i = 0; i < categorias.length; i += BATCH_SIZE) {
        const batch = categorias.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {

            return [
                'UNIDAD',
                c.NOM_MED,
                c.FEC_MED,
                newCompanyId,
            ];
        });
        try {
            const [res]: any = await conn.query(`
                INSERT INTO product_attributes(PRODATR_TYPE, PRODATR_NAME, PRODATR_FEC_CREA, FK_COD_EMP)  VALUES ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapMeasures[s.COD_MED] = newId;
                newId++;
            }

            console.log(` -> Batch migrado: ${batch.length} medidas`);
        } catch (err) {
            throw err;
        }
    }

    return mapMeasures;
}
