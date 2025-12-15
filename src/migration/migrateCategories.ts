/* import { erpPool } from '../config/db';
 */
export async function migrateCategories(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {
    console.log("Migrando categorias...");

    const [rows] = await legacyConn.query(`SELECT categorias.* FROM categorias ;`);
    const categorias = rows as any[];

    if (!categorias.length) {
        throw new Error(" -> No hay categorias para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapCategories: Record<number, number> = {};
    for (let i = 0; i < categorias.length; i += BATCH_SIZE) {
        const batch = categorias.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {

            return [
                'CATEGORIA',
                c.NOM_CAT,
                c.FEC_CAT,
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
                mapCategories[s.id] = newId;
                newId++;
            }

            console.log(` -> Batch migrado: ${batch.length} categorias`);
        } catch (err) {
            throw err;
        }
    }

    return mapCategories;
}
