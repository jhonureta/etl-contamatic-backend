export async function migrateCostCenter(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<string, number>> {

    console.log("Migrando centro de costos...");

    // Obtiene centroCosto únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT id_centro as ID_CENTER, nombre_centro as NAME_CENTER, fecha_registro_centro as CENTER_REG, estado_centro as STATUS_CENTER, codigo_centro as CODE_CENTER, descr_centro as DESCR_CENTER FROM centro_costo WHERE 1;
    `);

    const centroCosto = rows as any[];

    if (!centroCosto.length) {
        throw new Error(" -> No hay centro de costo para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapCenterCost: Record<string, number> = {};

    for (let i = 0; i < centroCosto.length; i += BATCH_SIZE) {
        const batch = centroCosto.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {
            return [
                c.NAME_CENTER, 
                c.CENTER_REG, 
                c.STATUS_CENTER, 
                c.CODE_CENTER, 
                c.DESCR_CENTER, 
                newCompanyId,
            ];
        });

        const [res]: any = await conn.query(
            `INSERT INTO cost_center
                (NAME_CENTER, CENTER_REG, STATUS_CENTER, CODE_CENTER, DESCR_CENTER,FK_COD_EMP)
             VALUES ?`,
            [values]
        );

        let newId = res.insertId;

        for (const b of batch) {
            mapCenterCost[b.ID_CENTER] = newId;
            newId++;
        }

        console.log(` -> Batch migrado: ${batch.length} centro de costos`);
    }
    return mapCenterCost;
}
