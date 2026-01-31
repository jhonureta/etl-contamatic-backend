export async function migrateWarehouseDetails(
    legacyConn: any,
    conn: any,
    branchMap: any,
    mapProducts: any
): Promise<{ mapDetWare: Record<number, number>; prodWareDetailIdMap: Record<string, number> }> {
    console.log("Migrando detalle de bodega stock...");

    const [rows] = await legacyConn.query(`SELECT detallebodega.COD_DETBOD as WHDET_ID, FK_COD_PROBOD as FK_PROD_ID, bodegas.COD_BOD as FK_WH_ID, STO_PRODET as WHDET_STOCK, ESTA_PROD as PROD_STATE  FROM detallebodega inner join bodegas on bodegas.COD_BOD = detallebodega.FK_COD_DETBOD WHERE 1 ;`);
    const detale = rows as any[];

    if (!detale.length) {
        throw new Error(" -> No hay detalle para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapDetWare: Record<number, number> = {};
    const prodWareDetailIdMap: Record<string, number> = {};
    for (let i = 0; i < detale.length; i += BATCH_SIZE) {
        const batch = detale.slice(i, i + BATCH_SIZE);

        const values = batch.map(w => {

            const productoId = mapProducts[w.FK_PROD_ID];
            const bodegaId = branchMap[w.FK_WH_ID];

            console.log(w.FK_PROD_ID, w.FK_WH_ID);

            return [
                w.WHDET_STOCK,
                productoId,
                bodegaId,
                w.PROD_STATE
            ];
        });
        //PROD_ID
        try {
            const [res]: any = await conn.query(`
            INSERT INTO warehouse_detail(  
               WHDET_STOCK,
               FK_PROD_ID, 
               FK_WH_ID, 
               PROD_STATE) VALUES  ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapDetWare[s.WHDET_ID] = newId;
                prodWareDetailIdMap[`${s.FK_PROD_ID}:${s.FK_WH_ID}`] = newId;
                newId++;
            } 
            console.log(` -> Batch migrado: ${batch.length} detalle de bodega`);
        } catch (err) {
            throw err;
        }
    }

    return { mapDetWare, prodWareDetailIdMap};
}
