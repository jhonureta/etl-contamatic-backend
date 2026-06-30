import { upsertTotaledEntry } from "./migrationTools";

type PayrollSeatIdMap = Array<{
    key: 'FK_CODROL' | 'FK_CODPREST' | 'FK_CODMOV' | 'ID_ADELANTO';
    id: number;
    cod_asiento: number;
    idAudit: number;
}>;

function getPayrollLoanSeatIds(mapIdAsiento: PayrollSeatIdMap): number[] {
    return [...new Set(
        mapIdAsiento
            .filter(asiento =>
                (asiento.key === 'FK_CODPREST' || asiento.key === 'ID_ADELANTO')
                && asiento.cod_asiento != null
            )
            .map(asiento => asiento.cod_asiento)
    )];
}

export async function migrateLoansPayrollAdvances(
    legacyConn: any,
    humanResourcesDb: any,
    conn: any,
    newCompanyId: number,
    mapClients: any,
    idEmpresaRhh: number,
    mapIdAsiento: PayrollSeatIdMap,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapCloseCash: Record<number, number>,
): Promise<{ mapLoansCustomers: Record<string, number>; totalMigrated: number }> {
    const mapAdvancesDetailEmployes: Record<number, number> = {};
    const mapLoansCustomers: Record<string, number> = {};
    const payrollLoanSeatIds = getPayrollLoanSeatIds(mapIdAsiento);

    if (!payrollLoanSeatIds.length) {
        return { mapLoansCustomers, totalMigrated: 0 };
    }

    const { movementIdAdvancesMap, mapAuditAdvances } = await migrateMovementAdvancesLoans(legacyConn, conn, newCompanyId, mapIdAsiento, bankMap, boxMap, mapCloseCash);

    const { employeesAdvancesMap: mapAdvancesEmployes } = await migrateSupplierLoans(humanResourcesDb, conn, mapClients, idEmpresaRhh);

    console.log("Migrando prestamos de clientes...");
    const [rows] = await humanResourcesDb.query(`SELECT
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
    NULL AS FK_AUDITANT,
    'INGRESO' AS CAUSA_ANT,
    NULL AS FK_ID_ORDEN
FROM
    tbPrestamo INNER JOIN tbEmpleados on tbEmpleados.empleado_id = tbPrestamo.fkid_empleado
WHERE
    fkid_empresa = ? AND tbPrestamo.id_asiento IS NOT NULL AND tbPrestamo.id_asiento IN (?);`, [idEmpresaRhh, payrollLoanSeatIds]);

    const anticiposClientes = rows as any[];

    console.log(` -> ${anticiposClientes.length} préstamos encontrados en tbPrestamo`);

    if (!anticiposClientes.length) {
        return { mapLoansCustomers, totalMigrated: 0 };
    }
    const BATCH_SIZE = 1000;
    let totalMigrated = 0;

    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = [];
        const inserted: any[] = [];
        for (const a of batch) {
            const idMov = movementIdAdvancesMap[a.prestamo_id] ?? null;
            if (idMov === null) continue;

            const idAuditoria = mapAuditAdvances[a.prestamo_id] ?? null;
            const idAdvance = mapAdvancesEmployes[a.fkid_empleado];
            values.push([
                a.FEC_DET_ANT,
                a.ABS_ANT,
                a.FORMAPAGO,
                a.IMPORTE_DET,
                a.SALDO_DET,
                a.SECUENCIA_DET,
                idAdvance,
                idMov,
                a.BENEFICIARIA,
                a.ORIGEN_ANT,
                a.FECH_REG,
                idAuditoria,
                a.CAUSA_ANT,
                null
            ]);
            inserted.push(a);
        }

        if (values.length === 0) continue;
      
        const [res]: any = await conn.query(
            `INSERT INTO detail_advances(FEC_DET_ANT, ABS_ANT, FORMAPAGO, IMPORTE_DET, SALDO_DET, SECUENCIA_DET, FK_IDANT, FK_ID_MOVI, BENEFICIARIA, ORIGEN_ANT, FECH_REG, FK_AUDITANT, CAUSA_ANT, FK_ID_ORDEN) VALUES ?`,
            [values]
        );
        let newId = res.insertId;
        for (const b of inserted) {
            mapAdvancesDetailEmployes[b.ID_ANT] = newId++;
        }
        totalMigrated += inserted.length;
    }
    return { mapLoansCustomers, totalMigrated };
}


export async function migrateSupplierLoans(
    legacyConn: any,
    conn: any,
    mapSuppliers: any,
    idEmpresaRhh: number
): Promise<{ employeesAdvancesMap: Record<number, number> }> {
    console.log("Migrando totales de anticipos de proveedores...");
    const employeesAdvancesMap: Record<number, number> = {};
    const [rows] = await legacyConn.query(`
        SELECT prestamo_id, sum(prestamo_monto) AS SALDO_ANT, fkid_empleado as FK_PERSONA, now() as FECH_REGANT
        FROM tbPrestamo 
        INNER JOIN tbEmpleados on tbEmpleados.empleado_id = tbPrestamo.fkid_empleado 
        left join tbMovimientos on tbMovimientos.id_prestamo = tbPrestamo.prestamo_id
        WHERE  fkid_empresa = ? AND tbPrestamo.id_asiento IS NOT NULL GROUP by fkid_empleado;
    `, [idEmpresaRhh]);

    const anticiposEmpleados = rows as any[];

    if (!anticiposEmpleados.length) {
        return { employeesAdvancesMap };
    }
    const BATCH_SIZE = 1000;
    for (let i = 0; i < anticiposEmpleados.length; i += BATCH_SIZE) {
        const batch = anticiposEmpleados.slice(i, i + BATCH_SIZE);

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
            employeesAdvancesMap[b.FK_PERSONA] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de empleados`);
    }



    return { employeesAdvancesMap };
}

export async function migrateMovementAdvancesLoans(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapIdAsiento: PayrollSeatIdMap,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapCloseCash: Record<number, number>,
    
): Promise<{ movementIdAdvancesMap: Record<number, number>; mapAuditAdvances: Record<number, number> }> {

    const movementIdAdvancesMap: Record<number, number> = {};
    const mapAuditAdvances: Record<number, number> = {};

    const [movements]: any[] = await legacyConn.query(`
        SELECT
            m.ID_MOVI,
            m.FK_COD_CAJAS_MOVI,
            m.FK_COD_BANCO_MOVI,
            m.TIP_MOVI,
            m.periodo_caja,
            m.FECHA_MOVI,
            CASE
                WHEN m.ORIGEN_MOVI = 'MOD-ADELANTO' THEN 'ANTEMPL'
                WHEN m.ORIGEN_MOVI = 'PSTO-NOMINA'  THEN 'PRESTEMPL'
                ELSE 'ANTEMPL'
            END AS ORIGEN_MOVI,
            m.IMPOR_MOVI,
            m.TIPO_MOVI,
            m.REF_MOVI,
            m.CONCEP_MOVI AS CONCEP_MOVIMIG,
            CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0 END AS ESTADO_MOVI,
            m.PER_BENE_MOVI,
            'EGRESO' AS CAUSA_MOVI,
            CASE
                WHEN m.ORIGEN_MOVI = 'MOD-ADELANTO' THEN 'ANTEMPL'
                WHEN m.ORIGEN_MOVI = 'PSTO-NOMINA'  THEN 'PRESTEMPL'
                ELSE 'ANTEMPL'
            END AS MODULO,
            m.FECHA_MANUAL,
            m.CONCILIADO,
            m.SECU_MOVI,
            m.FK_CONCILIADO,
            m.FK_ANT_MOVI,
            NULL AS NUM_VOUCHER,
            NULL AS NUM_LOTE,
            m.CONCEP_MOVI AS OBS_MOVI,
            m.periodo_caja AS FK_ARQUEO,
            m.RECIBO_CAJA,
            m.NUM_UNIDAD,
            m.FK_COD_RH,
            m.FK_DET_PREST,
            NULL AS JSON_PAGOS
        FROM movimientos m
        WHERE m.ORIGEN_MOVI = 'PSTO-NOMINA' OR m.ORIGEN_MOVI = 'MOD-ADELANTO'
        ORDER BY m.ID_MOVI ASC
    `);

    if (movements.length === 0) {
        return { movementIdAdvancesMap, mapAuditAdvances };
    }

    console.log(`Movimientos de anticipos/préstamos encontrados: ${movements.length}`);



    // Índices rápidos por key para evitar find() en cada iteración
    const adelantoMap = new Map<number, number>();
    const prestamoMap = new Map<number, number>();
    const idAuditTrMap = new Map<number, number>();
    for (const a of mapIdAsiento) {
        if (a.key === 'ID_ADELANTO') adelantoMap.set(a.id, a.cod_asiento);
        else if (a.key === 'FK_CODPREST') prestamoMap.set(a.id, a.cod_asiento);
        if (a.idAudit != null) {
            idAuditTrMap.set(a.id, a.idAudit);
        }
    }

    const validMovements = movements.filter((m: any) => {
        if (m.ORIGEN_MOVI === 'PRESTEMPL') return prestamoMap.has(m.FK_DET_PREST);
        if (m.ORIGEN_MOVI === 'ANTEMPL') return adelantoMap.has(m.FK_COD_RH);
        return false;
    });

    console.log(` -> ${validMovements.length} de ${movements.length} movimientos con asiento contable`);

    if (validMovements.length === 0) {
        console.log(' -> No hay movimientos con asiento contable para migrar');
        return { movementIdAdvancesMap, mapAuditAdvances };
    }

    const [[{ nextAudit }]]: any = await conn.query(
        `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit FROM audit WHERE FK_COD_EMP = ?`,
        [newCompanyId]
    );
    let auditSeq = nextAudit;

    const [[defaultUserRow]]: any = await conn.query(
        `SELECT COD_USUEMP as ID_USER FROM users WHERE FK_COD_EMP = ? AND ROL_USUEMP != 'superadmin' ORDER BY COD_USUEMP ASC LIMIT 1`,
        [newCompanyId]
    );
    const defaultUserId = defaultUserRow?.ID_USER ?? null;



    const BATCH_SIZE = 1000;
    for (let i = 0; i < validMovements.length; i += BATCH_SIZE) {
        const batch = validMovements.slice(i, i + BATCH_SIZE);

      /*   const auditValues = batch.map(() => [auditSeq++, 'NOMINA', newCompanyId]);
        const [resAudit]: any = await conn.query(
            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
            [auditValues]
        );
        const firstAuditId = resAudit.insertId; */

        const movementValues = batch.map((m: any, index: number) => {
           /*  const currentAuditId = firstAuditId + index; */
            const keyId = m.ORIGEN_MOVI === 'ANTEMPL' ? m.FK_COD_RH : m.FK_DET_PREST;
            const idAsiento = m.ORIGEN_MOVI === 'ANTEMPL'
                ? adelantoMap.get(keyId) ?? null
                : prestamoMap.get(keyId) ?? null;
            const idAudit = idAuditTrMap.get(keyId) ?? null;

            mapAuditAdvances[keyId] = idAudit;

            return [
                bankMap[m.FK_COD_BANCO_MOVI] ?? null,
                null,
                m.FK_CONCILIADO,
                defaultUserId,
                m.FECHA_MOVI,
                m.FECHA_MANUAL,
                m.TIP_MOVI,
                m.ORIGEN_MOVI,
                m.TIPO_MOVI,
                m.REF_MOVI,
                m.CONCEP_MOVIMIG,
                null,
                null,
                m.CAUSA_MOVI,
                m.MODULO,
                m.SECU_MOVI,
                m.IMPOR_MOVI,
                m.ESTADO_MOVI,
                m.PER_BENE_MOVI,
                m.CONCILIADO,
                newCompanyId,
                boxMap[m.FK_COD_CAJAS_MOVI] ?? null,
                m.OBS_MOVI,
                null,
                idAsiento,
                idAudit,
                mapCloseCash[m.FK_ARQUEO] ?? null,
                null,
                m.RECIBO_CAJA,
                null,
                m.NUM_UNIDAD,
                m.JSON_PAGOS
            ];
        });

        const [resMov]: any = await conn.query(`
            INSERT INTO movements(
                FKBANCO, FK_COD_TRAN, FK_CONCILIADO, FK_USER,
                FECHA_MOVI, FECHA_MANUAL, TIP_MOVI, ORIGEN_MOVI,
                TIPO_MOVI, REF_MOVI, CONCEP_MOVI, NUM_VOUCHER,
                NUM_LOTE, CAUSA_MOVI, MODULO, SECU_MOVI,
                IMPOR_MOVI, ESTADO_MOVI, PER_BENE_MOVI, CONCILIADO,
                FK_COD_EMP, IDDET_BOX, OBS_MOVI, IMPOR_MOVITOTAL,
                FK_ASIENTO, FK_AUDITMV, FK_ARQUEO, ID_TARJETA,
                RECIBO_CAJA, FK_CTAM_PLAN, NUMERO_UNIDAD, JSON_PAGOS
            ) VALUES ?
        `, [movementValues]);

        let currentMovId = resMov.insertId;
        batch.forEach((m: any) => {
            const keyId = m.ORIGEN_MOVI === 'ANTEMPL' ? m.FK_COD_RH : m.FK_DET_PREST;
            movementIdAdvancesMap[keyId] = currentMovId++;
        });

        console.log(` -> Batch migrado: ${batch.length} movimientos de préstamos/anticipos`);
    }
    return { movementIdAdvancesMap, mapAuditAdvances };
}
