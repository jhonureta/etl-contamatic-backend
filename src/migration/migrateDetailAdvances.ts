
export async function migrateAdvancesCustomers(
    legacyConn: any,
    conn: any,
    mapClients: any,
    mapAdvancesCustomers
): Promise<Record<string, number>> {

    console.log("Migrando anticipos de clientes...");
    // Obtiene centroCosto únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT *From detalle_anticipos inner join anticipos on detalle_anticipos.FK_COD_ANT = anticipos.ID_ANT WHERE anticipos.TIPO_ANT ='CLIENTES';`);

    const anticiposClientes = rows as any[];

    if (!anticiposClientes.length) {
        throw new Error(" -> No hay anticipos de clientes para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapAdvancesDetailCustomers: Record<string, number> = {};
    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {

            const idPersona = mapClients[c.FK_PERSONA] || null;
            return [
                c.FECH_REGANT,
                c.SALDO_ANT,
                idPersona
            ];
        });
        const [res]: any = await conn.query(
            `INSERT INTO detail_advances(FEC_DET_ANT, ABS_ANT, FORMAPAGO, IMPORTE_DET, SALDO_DET, SECUENCIA_DET, FK_IDANT, FK_ID_MOVI, BENEFICIARIA, ORIGEN_ANT, FECH_REG, FK_AUDITANT, CAUSA_ANT, FK_ID_ORDEN) VALUES  ?`,
            [values]
        );
        let newId = res.insertId;
        for (const b of batch) {
            mapAdvancesDetailCustomers[b.ID_ANT] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de clientes`);
    }
    return mapAdvancesDetailCustomers;
}


export async function migrateSupplierAdvances(
    legacyConn: any,
    conn: any,
    mapClients: any
): Promise<{ supplierAdvanceIdMap: Record<number, number> }> {
    console.log("Migrando totales de anticipos de proveedores...");
    const [rows] = await legacyConn.query(`
        SELECT
            ID_ANT,
            FEC_ANT AS FECH_REGANT,
            SALDO_ANT AS SALDO_ANT,
            FK_COD_CLI_ANT AS FK_PERSONA
        FROM
            detalle_anticipos
        INNER JOIN anticipos ON anticipos.ID_ANT = detalle_anticipos.FK_COD_ANT
        WHERE
            anticipos.TIPO_ANT = 'PROVEEDORES'
        GROUP BY
            detalle_anticipos.FK_COD_ANT;
    `);

    const anticiposClientes = rows as any[];

    if (!anticiposClientes.length) {
        throw new Error(" -> No hay anticipos de proveedores para migrar.");
    }
    const BATCH_SIZE = 1000;
    const supplierAdvanceIdMap: Record<number, number> = {};
    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {
            const idPersona = mapClients[c.FK_PERSONA] || null;
            return [
                c.FECH_REGANT,
                c.SALDO_ANT,
                idPersona
            ];
        });
        const [res]: any = await conn.query(
            `INSERT INTO advances(FECH_REGANT, SALDO_ANT, FK_PERSONA) VALUES  ?`,
            [values]
        );
        let newId = res.insertId;
        for (const b of batch) {
            supplierAdvanceIdMap[b.ID_ANT] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de proveedores`);
    }
    return { supplierAdvanceIdMap };
}

