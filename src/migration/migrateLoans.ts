
export async function migrateLoansCustomers(
    legacyConn: any,
    conn: any,
    mapClients: any
): Promise<Record<string, number>> {

    console.log("Migrando prestamos de clientes...");
    const mapLoansCustomers: Record<string, number> = {};
    // Obtiene centroCosto únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT
    prestamo_id,
    CASE 
        WHEN tipo_generacion = 'MOD-ADELANTO' THEN 'ANTEMPL' 
        WHEN tipo_generacion = 'MOD-PRESTAMO' THEN 'PRESTEMPL' 
        ELSE 'OTRA' 
    END AS ORIGEN_ANT,
    prestamo_fecha_emision AS FEC_DET_ANT,
    prestamo_fecha_inicio AS ANTRRHH_STARTDATE,
    prestamo_descripcion AS ABS_ANT,
    prestamo_monto AS IMPORTE_DET,
    prestamo_saldo AS SALDO_DET,
    prestamo_plazo AS ANTRRHH_MONTH,
    ROUND(prestamo_monto / NULLIF(prestamo_plazo, 0), 2) AS ANTRRHH_VAL,
    fkid_empleado,
    prestamo_metodopago AS FORMAPAGO,
    NULL AS SECUENCIA_DET,
    NULL AS FK_IDANT,
    NULL AS FK_ID_MOVI,
    tbEmpleados.emp_nombre AS BENEFICIARIA,
    prestamo_fecha_emision AS FECH_REG,
    NULL ASFK_AUDITANT,
    'INGRESO' AS CAUSA_ANT,
    NULL AS FK_ID_ORDEN
FROM
    tbPrestamo INNER JOIN tbEmpleados on tbEmpleados.empleado_id = tbPrestamo.fkid_empleado
WHERE
    1;`);

    const anticiposClientes = rows as any[];

    if (!anticiposClientes.length) {
        return mapLoansCustomers;
    }
    const BATCH_SIZE = 1000;

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
            mapLoansCustomers[b.ID_ANT] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de clientes`);
    }
    return mapLoansCustomers;
}


export async function migrateSupplierLoans(
    legacyConn: any,
    conn: any,
    mapSuppliers: any
): Promise<{ supplierLoanIdMap: Record<number, number> }> {
    console.log("Migrando totales de anticipos de proveedores...");
    const supplierLoanIdMap: Record<number, number> = {};
    const [rows] = await legacyConn.query(`
        SELECT
            ID_ANT,
            FEC_ANT AS FECH_REGANT,
            SALDO_ANT AS SALDO_ANT,
            FK_COD_PROV_ANT AS FK_PERSONA
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
        return { supplierLoanIdMap };
    }
    const BATCH_SIZE = 1000;
    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {
            const idPersona = mapSuppliers[c.FK_PERSONA] || null;
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
            supplierLoanIdMap[b.ID_ANT] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de proveedores`);
    }
    return { supplierLoanIdMap };
}

