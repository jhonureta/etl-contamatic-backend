
export async function migrateAdvancesCustomers(
    legacyConn: any,
    conn: any,
    mapClients: any
): Promise<Record<string, number>> {

    console.log("Migrando anticipos de clientes...");
    // Obtiene centroCosto únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT ID_ANT, FEC_ANT as FECH_REGANT,SALDO_ANT as SALDO_ANT,FK_COD_CLI_ANT as FK_PERSONA   from detalle_anticipos inner join anticipos on anticipos.ID_ANT = detalle_anticipos.FK_COD_ANT where anticipos.TIPO_ANT ='CLIENTES' GROUP BY detalle_anticipos.FK_COD_ANT;`);

    const anticiposClientes = rows as any[];

    if (!anticiposClientes.length) {
        throw new Error(" -> No hay anticipos de clientes para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapAdvancesCustomers: Record<string, number> = {};
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
            mapAdvancesCustomers[b.ID_ANT] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de clientes`);
    }
    return mapAdvancesCustomers;
}
