import { upsertTotaledEntry } from "./migrationTools";
import { findFirstDefaultUser } from "./purchaseHelpers";

type PayrollSeatIdMap = Array<{
    key: 'FK_CODROL' | 'FK_CODPREST' | 'FK_CODMOV' | 'ID_ADELANTO';
    id: number;
    cod_asiento: number;
}>;

function getPayrollLoanSeatIds(mapIdAsiento: PayrollSeatIdMap): number[] {
    return [...new Set(
        mapIdAsiento
            .filter(asiento => asiento.key === 'FK_CODPREST' && asiento.cod_asiento != null)
            .map(asiento => asiento.cod_asiento)
    )];
}

export async function migrateLoansPayrollAdvances(
    legacyConn: any,
    conn: any,
    mapClients: any,
    newCompanyId: number,
    idEmpresaRhh: number,
    mapIdAsiento: PayrollSeatIdMap,
    mapConciliation: Record<number, number>,
    userMap: Record<number, number>,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapCloseCash: Record<number, number>
): Promise<Record<string, number>> {
    const mapAdvancesDetailEmployes: Record<number, number> = {};
    const mapLoansCustomers: Record<string, number> = {};
    const payrollLoanSeatIds = getPayrollLoanSeatIds(mapIdAsiento);

    if (!payrollLoanSeatIds.length) {
        return mapLoansCustomers;
    }

    const { movementIdAdvancesMap, mapAuditAdvances } = await migrateMovementAdvancesLoans(legacyConn, conn, mapClients, idEmpresaRhh, newCompanyId, mapClients, mapIdAsiento, payrollLoanSeatIds, mapConciliation, userMap, bankMap, boxMap, mapCloseCash);

    const mapAdvancesEmployes = migrateSupplierLoans(legacyConn, conn, mapClients, idEmpresaRhh);

    console.log("Migrando prestamos de clientes...");
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
    inner join tbMovimientos on tbMovimientos.id_prestamo = tbPrestamo.prestamo_id
WHERE 
    fkid_empresa = ? AND id_prestamo IS NOT NULL AND tbMovimientos.id_asiento IN (?);`, [idEmpresaRhh, payrollLoanSeatIds]);

    const anticiposClientes = rows as any[];

    if (!anticiposClientes.length) {
        return mapLoansCustomers;
    }
    const BATCH_SIZE = 1000;

    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = [];
        for (const a of batch) {
            let idMov = movementIdAdvancesMap[a.prestamo_id] ?? null;
            let idAuditoria = mapAuditAdvances[a.prestamo_id] ?? null;
            a.FK_ID_ORDEN = null;

            const idAdvance = mapAdvancesEmployes[a.FK_IDANT];
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
                a.FK_ID_ORDEN
            ]);
        };
        const [res]: any = await conn.query(
            `INSERT INTO detail_advances(FEC_DET_ANT, ABS_ANT, FORMAPAGO, IMPORTE_DET, SALDO_DET, SECUENCIA_DET, FK_IDANT, FK_ID_MOVI, BENEFICIARIA, ORIGEN_ANT, FECH_REG, FK_AUDITANT, CAUSA_ANT, FK_ID_ORDEN) VALUES  ?`,
            [values]
        );
        let newId = res.insertId;
        for (const b of batch) {
            mapAdvancesDetailEmployes[b.ID_ANT] = newId;
            newId++;
        }
    }
    return mapLoansCustomers;
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
        inner join tbMovimientos on tbMovimientos.id_prestamo = tbPrestamo.prestamo_id
        WHERE  fkid_empresa = ? AND id_prestamo IS NOT NULL GROUP by fkid_empleado;
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
    mapSuppliers: any,
    idEmpresaRhh: number,
    newCompanyId: number,
    mapClients: any,
    mapIdAsiento: PayrollSeatIdMap,
    payrollLoanSeatIds: number[],
    mapConciliation: Record<number, number>,
    userMap: Record<number, number>,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapCloseCash: Record<number, number>
): Promise<{ movementIdAdvancesMap: Record<number, number>, mapAuditAdvances: Record<number, number> }> {

    const movementIdAdvancesMap: Record<number, number> = {};
    const mapAuditAdvances: Record<number, number> = {};

    const [movements] = await legacyConn.query(`SELECT
    prestamo_id,
    CASE WHEN tipo_generacion = 'MOD-ADELANTO' THEN 'ANTEMPL' WHEN tipo_generacion = 'MOD-PRESTAMO' THEN 'PRESTEMPL' ELSE 'OTRA'
END AS ORIGEN_ANT,
prestamo_fecha_emision AS FEC_DET_ANT,
prestamo_fecha_inicio AS ANTRRHH_STARTDATE,
prestamo_descripcion AS ABS_ANT,
prestamo_monto AS IMPORTE_DET,
prestamo_saldo AS SALDO_DET,
prestamo_plazo AS ANTRRHH_MONTH,
ROUND(
    prestamo_monto / NULLIF(prestamo_plazo, 0),
    2
) AS ANTRRHH_VAL,
fkid_empleado,
prestamo_metodopago AS FORMAPAGO,
NULL AS SECUENCIA_DET,
NULL AS FK_IDANT,
NULL AS FK_ID_MOVI,
tbEmpleados.emp_nombre AS BENEFICIARIA,
prestamo_fecha_emision AS FECH_REG,
NULL ASFK_AUDITANT,
'INGRESO' AS CAUSA_ANT,
NULL AS FK_ID_ORDEN,
fkid_empresa,


NULL AS ID_MOVI,
CASE WHEN tipo_movimiento='CAJAS' THEN JSON_UNQUOTE(JSON_EXTRACT(detalle_pago, '$[0].id')) ELSE NULL
END AS FK_COD_CAJAS_MOVI,
CASE WHEN tipo_movimiento='BANCOS' THEN JSON_UNQUOTE(JSON_EXTRACT(detalle_pago, '$[0].id')) ELSE NULL
END AS FK_COD_BANCO_MOVI,
tipo_movimiento AS TIP_MOVI,
NULL AS periodo_caja,
prestamo_fecha_emision AS FECHA_MOVI,
CASE WHEN tipo_generacion = 'MOD-ADELANTO' THEN 'ANTEMPL' WHEN tipo_generacion = 'MOD-PRESTAMO' THEN 'PRESTEMPL' ELSE 'OTRA'
END AS ORIGEN_MOVI,
prestamo_monto AS IMPOR_MOVI,
metodo_pago AS TIPO_MOVI,
referencia_pago AS REF_MOVI,
NULL AS CONCEP_MOVI,
1  AS ESTADO_MOVI,
tbEmpleados.emp_nombre AS PER_BENE_MOVI,'EGRESO' AS CAUSA_MOVI,
CASE WHEN tipo_generacion = 'MOD-ADELANTO' THEN 'ANTEMPL' WHEN tipo_generacion = 'MOD-PRESTAMO' THEN 'PRESTEMPL' ELSE 'OTRA'
END AS MODULO,
prestamo_fecha_emision AS FECHA_MANUAL,
CASE WHEN tipo_movimiento='BANCOS' THEN 'POR CONCILIAR' ELSE NULL
END AS CONCILIADO,
secuencia_movimiento AS SECU_MOVI,
NULL AS FK_CONCILIADO,
id_prestamo AS FK_ANT_MOVI,
NULL AS FK_USER,
NULL AS FK_COD_TRAN,
NULL AS NUMERO_UNIDAD,
NULL AS RECIBO_CAJA,
NULL AS NUM_VOUCHER,
NULL AS NUM_LOTE,
NULL AS OBS_MOVI,
ABS(importe_movimiento) AS IMPOR_MOVITOTAL,
NULL AS FK_AUDITMV,
NULL AS FK_ARQUEO,
NULL AS ID_TARJETA,
NULL AS FK_CTAM_PLAN,
NULL AS FDP_DET_ANT, NULL AS NUM_UNIDAD, '[]' AS JSON_PAGOS
FROM
    tbPrestamo
INNER JOIN tbEmpleados ON tbEmpleados.empleado_id = tbPrestamo.fkid_empleado
INNER JOIN tbMovimientos ON tbMovimientos.id_prestamo = tbPrestamo.prestamo_id
WHERE
    fkid_empresa = ? AND id_prestamo IS NOT NULL AND tbMovimientos.id_asiento IN (?);`, [idEmpresaRhh, payrollLoanSeatIds]);
    if (movements.length === 0) {
        return { movementIdAdvancesMap, mapAuditAdvances };
    }
    console.log(`Movimientos de anticipos... ${movements.length}`);

    const [[{ nextAudit }]]: any = await conn.query(
        `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit FROM audit WHERE FK_COD_EMP = ?`,
        [newCompanyId]
    );
    let auditSeq = nextAudit;
    //ORG_ORDEN

    const [cardData]: any[] = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`, [newCompanyId]);
    const cardId = cardData[0]?.ID_TARJETA ?? null;


    const [movementsData]: any[] = await conn.query(`SELECT ID_MOVI,FK_CONCILIADO,CONCILIADO,FK_COD_RH FROM movimientos WHERE ORIGEN_MOVI LIKE '%NOMINA%'; `);

    const [defaultUser] = await findFirstDefaultUser({ conn, companyId: newCompanyId });

    let defaultUserId = null;
    if (defaultUser) {
        defaultUserId = defaultUser.COD_USUEMP;
    }

    const BATCH_SIZE = 1500;
    //TIPO_MOVI
    for (let i = 0; i < movements.length; i += BATCH_SIZE) {
        const batchMovements = movements.slice(i, i + BATCH_SIZE);
        const auditValues = batchMovements.map(o => [auditSeq++, o.forma, newCompanyId]);
        const [resAudit]: any = await conn.query(
            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
            [auditValues]
        );
        const firstAuditId = resAudit.insertId;
        const movementValues = batchMovements.map((m, index) => {
            /* for (const m of batchMovements) { */
            const bankId = bankMap[m.FK_COD_BANCO_MOVI];
            const idBoxDetail = boxMap[m.FK_COD_CAJAS_MOVI];
            let transactionId = null;

            const prestamoId = m.prestamo_id; // tu valor de prestamo_id
            const movimiento = movementsData.find((m: any) => m.FK_COD_RH === prestamoId);
            const fkConciliado = movimiento ? movimiento.FK_CONCILIADO : null;


            const currentAuditId = firstAuditId + index;
            mapAuditAdvances[m.FK_ANT_MOVI] = currentAuditId;
            //const idFkConciliation = null;// mapConciliation[m.FK_CONCILIADO] ?? null;
            let idPlanCuenta = null;
            const idAsiento = mapIdAsiento.find(
                asiento => asiento.key === 'FK_CODPREST' && asiento.id === m.prestamo_id
            )?.cod_asiento ?? null;

            m.FK_ARQUEO = mapCloseCash[m.FK_ARQUEO] ?? null;
            return [
                bankId,
                transactionId,
                fkConciliado,
                defaultUserId,
                m.FECHA_MOVI,
                m.FECHA_MANUAL,
                m.TIP_MOVI,
                m.ORIGEN_MOVI,
                m.TIPO_MOVI,
                m.REF_MOVI,
                m.CONCEP_MOVIMIG,
                m.NUM_VOUCHER,
                m.NUM_LOTE,
                m.CAUSA_MOVI,
                m.MODULO,
                m.SECU_MOVI,
                m.IMPOR_MOVI,
                m.ESTADO_MOVI,
                m.PER_BENE_MOVI,
                m.CONCILIADO == 'POR CONCILIAR' ? (fkConciliado != null ? 'CONCILIADO' : null) : m.CONCILIADO,
                newCompanyId,
                idBoxDetail,
                m.OBS_MOVI,
                m.TOTPAG_TRAC,
                idAsiento,
                currentAuditId,
                m.FK_ARQUEO,
                m.TIPO_MOVI == 'TARJETA' ? cardId : null,
                m.RECIBO_CAJA,
                idPlanCuenta,
                m.NUM_UNIDAD,
                m.JSON_PAGOS
            ];

        });
        //FK_CONCILIADO
        const [resMov]: any = await conn.query(`
				INSERT INTO movements(
						FKBANCO,
						FK_COD_TRAN,
						FK_CONCILIADO,
						FK_USER,
						FECHA_MOVI,
						FECHA_MANUAL,
						TIP_MOVI,
						ORIGEN_MOVI,
						TIPO_MOVI,
						REF_MOVI,
						CONCEP_MOVI,
						NUM_VOUCHER,
						NUM_LOTE,
						CAUSA_MOVI,
						MODULO,
						SECU_MOVI,
						IMPOR_MOVI,
						ESTADO_MOVI,
						PER_BENE_MOVI,
						CONCILIADO,
						FK_COD_EMP,
						IDDET_BOX,
						OBS_MOVI,
						IMPOR_MOVITOTAL,
						FK_ASIENTO,
						FK_AUDITMV,
						FK_ARQUEO,
						ID_TARJETA,
						RECIBO_CAJA,
						FK_CTAM_PLAN,
						NUMERO_UNIDAD,
						JSON_PAGOS
				)
				VALUES ?
			`, [movementValues]);

        let currentMovId = resMov.insertId;
        batchMovements.forEach(o => {
            movementIdAdvancesMap[o.FK_ANT_MOVI] = currentMovId++;
        });
        console.log(` -> Batch migrado: ${batchMovements.length} anticipos clientes`);
    }

    return { movementIdAdvancesMap, mapAuditAdvances };
}
