export async function migrateBrand(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<string, number>> {

    console.log("Migrando marcas...");

    // Obtiene marcas únicas normalizadas
    const [rows] = await legacyConn.query(`
        SELECT 
            COALESCE(NULLIF(TRIM(MARSUB_PRO), ''), 'SIN MARCA') AS marca
        FROM producto 
        GROUP BY marca;
    `);

    const marcas = rows as any[];

    if (!marcas.length) {
        throw new Error(" -> No hay marcas para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapBrand: Record<string, number> = {};
    let existeSinMarca = false;

    for (let i = 0; i < marcas.length; i += BATCH_SIZE) {
        const batch = marcas.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {
            if (c.marca === 'SIN MARCA') {
                existeSinMarca = true;
            }

            return [
                'MARCA',
                c.marca,
                new Date(),   // Fecha dinámica
                newCompanyId,
            ];
        });

        const [res]: any = await conn.query(
            `INSERT INTO product_attributes
                (PRODATR_TYPE, PRODATR_NAME, PRODATR_FEC_CREA, FK_COD_EMP)
             VALUES ?`,
            [values]
        );

        let newId = res.insertId;

        for (const item of batch) {
            mapBrand[item.marca] = newId;
            newId++;
        }

        console.log(` -> Batch migrado: ${batch.length} marcas`);
    }

    // Si no existía "SIN MARCA", lo inserta
    if (!existeSinMarca) {
        const values = [[
            'MARCA',
            'SIN MARCA',
            new Date(),
            newCompanyId,
        ]];

        const [res]: any = await conn.query(
            `INSERT INTO product_attributes
                (PRODATR_TYPE, PRODATR_NAME, PRODATR_FEC_CREA, FK_COD_EMP)
             VALUES ?`,
            [values]
        );

        mapBrand['SIN MARCA'] = res.insertId;
    }

    return mapBrand;
}
