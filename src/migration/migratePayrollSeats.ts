import { migratePayrollConfiguration, principalConfigNameByLegacyCode } from "./migrateEmployes";
import { migrateLoansPayrollAdvances } from "./migrateLoans";
import { upsertTotaledEntry } from "./migrationTools";
import { findNextAuditCode, insertAudit, toJSONArray, toNumber } from "./purchaseHelpers";

type PayrollSeatIdMap = Array<{
    key: 'FK_CODROL' | 'FK_CODPREST' | 'FK_CODMOV' | 'ID_ADELANTO';
    id: number;
    cod_asiento: number;
    idAudit: number;
}>;
    
async function getEmpresaRhhId(humanResourcesDb: any, codEmp: number | null): Promise<number | null> {
    if (!humanResourcesDb || codEmp == null) {
        return null;
    }

    const [empresaRows]: any[] = await humanResourcesDb.query(
        `SELECT * FROM tbEmpresa WHERE empresa_codigo = ? LIMIT 1`,
        [codEmp]
    );
    const empresa = empresaRows[0];
    return empresa?.id_empresa ?? empresa?.empresa_id ?? empresa?.empresa_codigo ?? null;
}

async function getAdvanceAuditMap(
    humanResourcesDb: any,
    codEmp: number | null,
    mapIdAsiento: PayrollSeatIdMap,
    mapAuditPayroll: Record<number, number>,
    idEmpresaRhh?: number | null
): Promise<{ advanceAuditMap: Record<number, number | null>; idEmpresaRhh: number | null }> {
    const advanceIds = [...new Set(
        mapIdAsiento
            .filter(asiento => asiento.key === 'ID_ADELANTO' && asiento.id != null)
            .map(asiento => asiento.id)
    )];

    const advanceAuditMap: Record<number, number | null> = {};
    if (!humanResourcesDb || !advanceIds.length) {
        return { advanceAuditMap, idEmpresaRhh: null };
    }


    const params: any[] = [advanceIds];
    let companyFilter = '';
    if (idEmpresaRhh != null) {
        companyFilter = ' AND tbPrestamo.fkid_empresa = ?';
        params.push(idEmpresaRhh);
    }

    const [rows]: any[] = await humanResourcesDb.query(`
        SELECT
            tbPrestamo.prestamo_id,
            tbPrestamo.fk_idAdelanto,
            tbAdelantos.id_asiento
        FROM tbPrestamo
        INNER JOIN tbAdelantos ON tbPrestamo.fk_idAdelanto = tbAdelantos.id_adelanto
        WHERE tbPrestamo.tipo_generacion = 'MOD-ADELANTO'
            AND tbPrestamo.fk_idAdelanto IN (?)
            ${companyFilter}
    `, params);

    for (const row of rows) {
        advanceAuditMap[row.fk_idAdelanto] = mapAuditPayroll[row.id_asiento] ?? null;
    }

    return { advanceAuditMap, idEmpresaRhh };
}
//    idEmpresaRhh,
export async function migratePayroll(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapConciliation,
    userMap,
    bankMap,
    boxMap,
    mapCloseCash,
    humanResourcesDb?: any,
    codEmp?: number
): Promise<{
    countEmployes: number;
    countSeats: number;
    countPayrolls: number;
    countObligations: number;
    countPaymentMovements: number;
    countPaymentDetails: number;
    countLoans: number;
}> {

    let idEmpresaRhh = null;

    idEmpresaRhh = await getEmpresaRhhId(humanResourcesDb, codEmp);


    const { mapDepartments, mapPositions, mapSalaries, mapBenefits, mapEmployes, mappingContracts } = await migratePayrollConfiguration(
        legacyConn,
        conn,
        newCompanyId,
        humanResourcesDb,
        idEmpresaRhh,
        mapAccounts
    );


    const {
        mapAuditPayroll,
        mapAuditPagoRoll,
        mapAuditAnticipoRoll,
        mapAuditMovimientosPagoRoll,
        mapIdAsiento,
        mapEntryAccount,
    } = await migratePayrollSeats(legacyConn,
        conn,
        newCompanyId,
        mapPeriodo,
        mapProject,
        mapCenterCost,
        mapAccounts);
    const mapClients = mapEmployes;

    const { payrollIdMap, payrollIdMapAudit } = await migratePayrollRecords(
        conn,
        newCompanyId,
        humanResourcesDb,
        idEmpresaRhh,
        mapEmployes,
        mapBenefits,
        mapAuditPagoRoll,
        mapAuditPayroll,
        mapIdAsiento
    );

   /*  const rolAsientoUpdates: Array<[number, number]> = [];
    for (const entry of mapIdAsiento) {
        if (entry.key !== 'FK_CODROL') continue;
        const newAsientoId = mapEntryAccount[entry.cod_asiento];
        const newPayrollId = payrollIdMap[entry.id];
        if (newAsientoId && newPayrollId) {
            rolAsientoUpdates.push([newAsientoId, newPayrollId]);
        }
    } */

   /*  if (rolAsientoUpdates.length) {
        const [existingCols]: any[] = await conn.query(`SHOW COLUMNS FROM accounting_movements LIKE 'FK_CODROL'`);
        if (!existingCols.length) {
            await conn.query(`ALTER TABLE accounting_movements ADD COLUMN FK_CODROL INT NULL`);
        }

        const caseWhen = rolAsientoUpdates.map(() => 'WHEN ? THEN ?').join(' ');
        const inIds = rolAsientoUpdates.map(([id]) => id);
        const params: any[] = rolAsientoUpdates.flatMap(([id, payrollId]) => [id, payrollId]);
        params.push(inIds);
        await conn.query(
            `UPDATE accounting_movements SET FK_CODROL = CASE COD_ASIENTO ${caseWhen} END WHERE COD_ASIENTO IN (?)`,
            params
        );
        console.log(` -> FK_CODROL actualizado en ${rolAsientoUpdates.length} asientos de nomina`);
    } */

    const { totalLoans } = await migratePayrollMovements(
        legacyConn,
        conn,
        newCompanyId,
        idEmpresaRhh,
        mapConciliation,
        userMap,
        bankMap,
        boxMap,
        mapCloseCash,
        mapAuditPayroll,
        mapAuditPagoRoll,
        mapAuditAnticipoRoll,
        mapAuditMovimientosPagoRoll,
        mapIdAsiento,
        mapClients,
        humanResourcesDb,
        codEmp ?? null,

    );

    const { mapObligations } = await migratePayrollObligations(
        conn,
        newCompanyId,
        humanResourcesDb,
        idEmpresaRhh,
        mapEmployes,
        payrollIdMap,
        payrollIdMapAudit
    );

    const { mapMovements: mapPaymentMovements, mapMovementsByFkCodRH } = await migratePayrollPaymentMovements(
        legacyConn,
        conn,
        newCompanyId,
        mapConciliation,
        userMap,
        bankMap,
        boxMap,
        mapCloseCash,
        mapEntryAccount,
        mapIdAsiento,
        mapAuditMovimientosPagoRoll
    );

    const { mapPaymentDetails } = await migratePayrollPaymentDetails(
        humanResourcesDb,
        conn,
        idEmpresaRhh,
        mapObligations,
        mapMovementsByFkCodRH
    );

    return {
        countEmployes: Object.keys(mapEmployes).length,
        countSeats: Object.keys(mapEntryAccount).length,
        countPayrolls: Object.keys(payrollIdMap).length,
        countObligations: Object.keys(mapObligations).length,
        countPaymentMovements: Object.keys(mapPaymentMovements).length,
        countPaymentDetails: Object.keys(mapPaymentDetails).length,
        countLoans: totalLoans,
    };
}

export async function migratePayrollObligations(
    conn: any,
    newCompanyId: number,
    humanResourcesDb: any,
    idEmpresaRhh: number | null,
    mapEmployes: Record<number, number>,
    payrollIdMap: Record<number, number>,
    payrollIdMapAudit: Record<number, number>,
): Promise<{ mapObligations: Record<number, number> }> {
    console.log("Migrando cuentas por pagar de nómina (CXPN)...");

    const mapObligations: Record<number, number> = {};

    if (!humanResourcesDb || !idEmpresaRhh) {
        console.warn("No se migran CxP de nómina: no hay conexión o empresa RRHH.");
        return { mapObligations };
    }

    const [rows]: any[] = await humanResourcesDb.query(`
        SELECT
            tbCuentasRol.id_cuenta AS ID_CUENTA,
            tbCuentasRol.id_empleado AS FK_PERSONA,
            tbRol.rol_fecha_registro AS FECH_EMISION,
            tbRol.rol_fecha_registro AS FECH_VENCIMIENTO,
            tbCuentasRol.total_cuenta AS TOTAL,
            tbCuentasRol.saldo_cuenta AS SALDO,
            tbCuentasRol.id_rol_perido AS FK_PAYROLL,
            CONCAT(tbRol.rol_periodo, '-', LPAD(tbRol.rol_mes, 2, '0')) AS REF_SECUENCIA,
            tbCuentasRol.registro_cuenta AS OBLG_FEC_REG
        FROM tbCuentasRol
        INNER JOIN tbRol ON tbCuentasRol.id_rol_perido = tbRol.rol_id
        WHERE tbRol.estado_rol = 'Terminado'
            AND tbCuentasRol.id_empresa = ?
    `, [idEmpresaRhh]);

    if (!rows.length) {
        console.warn("No hay cuentas por pagar de nómina para migrar.");
        return { mapObligations };
    }

    const BATCH_SIZE = 500;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const insertValues: any[] = [];
        const validRows: any[] = [];

        for (const o of batch) {
            const personaId = mapEmployes[o.FK_PERSONA] ?? null;
            const payrollId = payrollIdMap[o.FK_PAYROLL] ?? null;
            const auditId = payrollIdMapAudit[o.FK_PAYROLL] ?? null;
            if (!personaId || !payrollId) continue;

            insertValues.push([
                personaId,
                'CXPN',
                o.FECH_EMISION,
                o.FECH_VENCIMIENTO,
                10,
                1,
                o.SALDO,
                o.TOTAL,
                o.REF_SECUENCIA,
                null,
                payrollId,
                'MIGRADO',
                null,
                '1',
                auditId,
                o.OBLG_FEC_REG,
                newCompanyId
            ]);
            validRows.push(o);
        }

        if (!insertValues.length) continue;

        const [res]: any = await conn.query(`
            INSERT INTO cuentas_obl (
                FK_PERSONA, TIPO_OBL, FECH_EMISION, FECH_VENCIMIENTO, TIP_DOC,
                ESTADO, SALDO, TOTAL, REF_SECUENCIA, FK_COD_TRANS,
                FK_PAYROLL, TIPO_CUENTA, FK_ID_INFCON, TIPO_ESTADO_CUENTA,
                FK_AUDITOB, OBLG_FEC_REG, FK_COD_EMP
            ) VALUES ?
        `, [insertValues]);


        let newId = res.insertId;
        for (const o of validRows) {
            mapObligations[o.ID_CUENTA] = newId++;
        }

        console.log(` -> Batch migrado: ${insertValues.length} cuentas por pagar de nómina`);
    }
    console.log("✅ Migración de CxP de nómina completada correctamente");
    return { mapObligations };
}

async function getConfigurationDetailIdColumn(conn: any): Promise<string> {
    const [columns]: any[] = await conn.query(`SHOW COLUMNS FROM configuration_rrhh_detail`);
    const columnNames = columns.map((column: any) => column.Field);
    const idColumn = columnNames.find((name: string) =>
        name !== 'FK_CONFRRHH_ID' && /(^ID_|_ID$|ID$)/i.test(name)
    );

    if (!idColumn) {
        throw new Error('No se encontro columna ID en configuration_rrhh_detail.');
    }

    return idColumn;
}

async function getConfigurationDetailMap(conn: any, newCompanyId: number): Promise<Record<number, number>> {
    const detailIdColumn = await getConfigurationDetailIdColumn(conn);
    const [rows]: any[] = await conn.query(`
        SELECT
            ${detailIdColumn} AS DETAIL_ID,
            FK_CONFRRHH_ID
        FROM configuration_rrhh_detail
        WHERE FK_CODDET_EMP = ?
        ORDER BY ${detailIdColumn}
    `, [newCompanyId]);

    const detailMap: Record<number, number> = {};
    for (const row of rows) {
        if (!detailMap[row.FK_CONFRRHH_ID]) {
            detailMap[row.FK_CONFRRHH_ID] = row.DETAIL_ID;
        }
    }

    return detailMap;
}

function parseJsonArray(value: any): any[] {
    return toJSONArray(value);
}

function payrollItemAmount(item: any): number {
    return toNumber(item?.valor ?? item?.[item?.codigo] ?? 0);
}

function payrollItemMap(items: any[]): Record<string, any> {
    const map: Record<string, any> = {};
    for (const item of items) {
        const code = String(item?.codigo ?? '').trim().toUpperCase();
        if (code) map[code] = item;
    }
    return map;
}

function benefitTypeByCode(employeePayroll: any): Record<string, string | null> {
    const options = parseJsonArray(employeePayroll?.contrato_beneficiossociales);
    const map: Record<string, string | null> = {};

    for (const option of options) {
        const code = String(option?.beneficio_codigo ?? '').trim().toUpperCase();
        const type = option?.tipoBeneficio ? String(option.tipoBeneficio).trim().toUpperCase() : null;

        if (code === 'PI7') map.I7 = type;
        if (code === 'PI8') map.I8 = type;
        if (code === 'PI9') map.I9 = type;
    }

    return map;
}

function sumItems(items: any[], predicate: (item: any) => boolean): number {
    return items.reduce((total, item) => predicate(item) ? total + payrollItemAmount(item) : total, 0);
}

function payrollDetailType(item: any, annualTypes: Record<string, string | null>): string | null {
    const code = String(item?.codigo ?? '').trim().toUpperCase();
    if (annualTypes[code]) return annualTypes[code];

    const benefitType = String(item?.beneficio_tipo ?? '').trim().toUpperCase();
    if (benefitType === 'INGRESO') return 'INGRESO';
    if (benefitType === 'EGRESO') return 'EGRESO';
    if (benefitType === 'PROVISIONES') return 'PROVISION';
    return benefitType || null;
}

function resolvePayrollAuditId(
    rol: any,
    mapAuditPagoRoll: Record<number, number>,
    mapAuditPayroll: Record<number, number>
): number | null {
    return mapAuditPagoRoll[rol.rol_id]
        ?? mapAuditPayroll[rol.rolid_asiento]
        ?? (mapAuditPayroll as any)[String(rol.rolid_asiento ?? '')]
        ?? (mapAuditPayroll as any)[String(rol.rol_asiento ?? '').trim()]
        ?? null;
}

export async function migratePayrollRecords(
    conn: any,
    newCompanyId: number,
    humanResourcesDb: any,
    idEmpresaRhh: number,
    mapEmployes: Record<number, number>,
    mapBenefits: Record<number, number>,
    mapAuditPagoRoll: Record<number, number>,
    mapAuditPayroll: Record<number, number>,
    mapIdAsiento: PayrollSeatIdMap
    //): Promise<Record<number, number>> {
): Promise<{ payrollIdMap: Record<number, number>; payrollIdMapAudit: Record<number, number> }> {
    console.log("Migrando roles de nomina...");
    const payrollIdMap: Record<number, number> = {};
    const payrollIdMapAudit: Record<number, number> = {};

    if (!humanResourcesDb || !idEmpresaRhh) {
        console.warn("No se migran roles de nomina: no hay conexion o empresa RRHH.");
        return { payrollIdMap, payrollIdMapAudit };
    }


    /* throw new Error(`Error al migrar la configuración de la empresa=`); */
    const [rows]: any[] = await humanResourcesDb.query(`
        SELECT
            rol_id,
            rol_periodo,
            rol_mes,
            rol_fecha_registro,
            rol_totalIngreso,
            rol_totalEgreso,
            rol_totalPagar,
            detalle_rol,
            estado_rol,
            rolid_asiento,
            rol_asiento
        FROM tbRol
        WHERE fk_empresa_id = ?
        ORDER BY rol_id
    `, [idEmpresaRhh]);

    if (!rows.length) {
        console.warn(`No se encontraron roles para idEmpresaRhh=${idEmpresaRhh}.`);
        return { payrollIdMap, payrollIdMapAudit };
    }

    const configurationMaps = await getConfigurationDetailMap(conn, newCompanyId);
    const principalCodes = new Set(Object.keys(principalConfigNameByLegacyCode));
    const detailValues: any[] = [];
    const BATCH_SIZE = 500;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const payrollRows: any[] = [];
        const payrollSources: Array<{ rol: any; employeePayroll: any }> = [];

        for (const rol of batch) {
            const employees = parseJsonArray(rol.detalle_rol);

            for (const employeePayroll of employees) {
                const employeeId = mapEmployes[Number(employeePayroll.empleado_id)] ?? null;
                if (!employeeId) continue;

                const movements = Array.isArray(employeePayroll.movimiento) ? employeePayroll.movimiento : [];
                const movementMap = payrollItemMap(movements);
                const totalIngresos = payrollItemAmount(movementMap.TI);
                const totalEgresos = payrollItemAmount(movementMap.TE);
                const totalPagar = payrollItemAmount(movementMap.TP);
                const payrollStatus = String(rol.estado_rol ?? '').toUpperCase() === 'TERMINADO'
                    ? 'PROCESADO'
                    : String(rol.estado_rol ?? '').toUpperCase() || 'PENDIENTE';
                const idAudit = resolvePayrollAuditId(rol, mapAuditPagoRoll, mapAuditPayroll);
                payrollIdMapAudit[rol.rol_id] = idAudit ?? null;

                payrollRows.push([
                    employeeId,
                    payrollStatus,
                    rol.rol_periodo,
                    rol.rol_mes,
                    160,
                    payrollItemAmount(movementMap.I1),
                    payrollItemAmount(movementMap.I2),
                    payrollItemAmount(movementMap.I3),
                    payrollItemAmount(movementMap.I4),
                    payrollItemAmount(movementMap.I7),
                    payrollItemAmount(movementMap.I8),
                    payrollItemAmount(movementMap.I9),
                    '[]',
                    '[]',
                    0,
                    totalIngresos,
                    '[]',
                    '[]',
                    payrollItemAmount(movementMap.E1) + payrollItemAmount(movementMap.E2),
                    payrollItemAmount(movementMap.E4),
                    0,
                    0,
                    payrollItemAmount(movementMap.E7),
                    payrollItemAmount(movementMap.I6) || payrollItemAmount(movementMap.E9),
                    totalEgresos,
                    totalPagar,
                    rol.rol_fecha_registro,
                    newCompanyId,
                    idAudit
                ]);
                payrollSources.push({ rol, employeePayroll });
            }
        }

        if (!payrollRows.length) continue;

        const [res]: any = await conn.query(`
            INSERT INTO payrolls (
                FK_EMPLOYEE,
                PAYR_STATUS,
                PAYR_PERIOD,
                PAYR_MONTH,
                PAYR_HOUR_VAL_LV,
                PAYR_SALARY,
                PAYR_HOUR_VAL_100,
                PAYR_HOUR_VAL_50,
                PAYR_HOUR_VAL_25,
                PAYR_RES_FUND,
                PAYR_THIRTEENTH,
                PAYR_FOURTEENTH,
                PAYR_DET_ING_AP,
                PAYR_DET_ING_NOAP,
                PAYR_TOT_SUM_ING,
                PAYR_TOT_ING,
                PAYR_DET_EGR_AP,
                PAYR_DET_EGR_NOAP,
                PAYR_ADVANCE,
                PAYR_LOAN,
                PAYR_TOT_SUM_EGR,
                PAYR_IESS_NODEP,
                PAYR_IESS,
                PAYR_APO_PATR,
                PAYR_TOT_EGR,
                PAYR_TOTAL,
                PAYR_FEC_REG,
                FK_COD_EMP,
                FK_AUDITTR
            ) VALUES ?
        `, [payrollRows]);

        let payrollId = res.insertId;
        for (const source of payrollSources) {
            if (!(source.rol.rol_id in payrollIdMap)) {
                payrollIdMap[source.rol.rol_id] = payrollId;
            }
            const annualTypes = benefitTypeByCode(source.employeePayroll);
            const movements = Array.isArray(source.employeePayroll.movimiento) ? source.employeePayroll.movimiento : [];

            for (const item of movements) {
                const code = String(item?.codigo ?? '').trim().toUpperCase();
                const amount = payrollItemAmount(item);
                if (!code || principalCodes.has(code) || code.startsWith('T') || amount === 0) continue;

                const oldConfigurationId = Number(item?.id_configuracion);
                const configurationId = mapBenefits[oldConfigurationId] ?? null;
                const configurationDetailId = configurationId ? configurationMaps[configurationId] ?? null : null;
                if (!configurationDetailId) continue;

                detailValues.push([
                    amount,
                    payrollDetailType(item, annualTypes),
                    payrollId,
                    configurationDetailId,
                    null
                ]);
            }

            payrollId++;
        }

        console.log(` -> Batch migrado: ${payrollRows.length} roles de nomina`);
    }

    if (detailValues.length) {
        await conn.query(`
            INSERT INTO payroll_details (
                PAYDET_AMOUNT,
                PAYDET_TYPE,
                FK_PAYROLL,
                FK_CONF_RRHH_DET,
                FK_ID_DET_ANT
            ) VALUES ?
        `, [detailValues]);
    }

    return { payrollIdMap, payrollIdMapAudit };
}

export async function migratePayrollMovements(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    idEmpresaRhh: number,
    mapConciliation: Record<number, number>,
    userMap: Record<number, number>,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapCloseCash: Record<number, number>,
    mapAuditPayroll: Record<number, number>,
    mapAuditPagoRoll: Record<number, number>,
    mapAuditAnticipoRoll: Record<number, number>,
    mapAuditMovimientosPagoRoll: Record<number, number>,
    mapIdAsiento: PayrollSeatIdMap,
    mapClients: Record<number, number>,
    humanResourcesDb?: any,
    codEmp: number | null = null
): Promise<{ movementOrderIdMap: Record<number, number>; totalLoans: number }> {
    console.log("Migrando movimientos de nómina...");
    const movementOrderIdMap: Record<number, number> = {};
    try { //mapAuditPayroll
        const [movements]: any[] = await legacyConn.query(`
            SELECT
                m.ID_MOVI,
                m.FK_COD_CAJAS_MOVI,
                m.FK_COD_BANCO_MOVI,
                m.TIP_MOVI AS TIP_MOVI,
                m.periodo_caja,
                m.FECHA_MOVI AS FECHA_MOVI,
                m.ORIGEN_MOVI AS ORIGEN_MOVI,
                m.IMPOR_MOVI AS IMPOR_MOVI,
                m.TIPO_MOVI AS TIPO_MOVI,
                m.REF_MOVI,
                m.CONCEP_MOVI AS CONCEP_MOVIMIG,
                CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0 END AS ESTADO_MOVI,
                m.PER_BENE_MOVI AS PER_BENE_MOVI,
                'EGRESO' AS CAUSA_MOVI,
                m.MODULO AS MODULO,
                m.FECHA_MANUAL AS FECHA_MANUAL,
                m.CONCILIADO,
                m.FK_COD_CX,
                m.SECU_MOVI,
                m.FK_CONCILIADO,
                m.FK_ANT_MOVI,
                m.FK_USER_EMP_MOVI AS FK_USER_EMP_MOVI,
                m.FK_TRAC_MOVI AS FK_TRAC_MOVI,
                NULL AS NUM_VOUCHER,
                NULL AS NUM_LOTE,
                m.CONCEP_MOVI AS OBS_MOVI,
                NULL AS FK_ASIENTO,
                m.periodo_caja AS FK_ARQUEO,
                m.RECIBO_CAJA,
                m.NUM_UNIDAD,
                m.FK_COD_RH,
                m.FK_DET_PREST,
                NULL AS TOTPAG_TRAC,
                NULL AS JSON_PAGOS
            FROM movimientos m
            WHERE m.ORIGEN_MOVI = 'CXP-NOMINA'
            ORDER BY ID_MOVI ASC
        `);

        if (movements.length === 0) {
            return { movementOrderIdMap, totalLoans: 0 };
        }

        const [cardData]: any[] = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`, [newCompanyId]);
        const cardId = cardData[0]?.ID_TARJETA ?? null;

        const [[movementSeqRow]]: any[] = await conn.query(
            `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS SECU_MOVI FROM movements WHERE MODULO = 'NOMINA' AND FK_COD_EMP = ?`,
            [newCompanyId]
        );
        let movementSequence = movementSeqRow?.SECU_MOVI ?? 1;

        const BATCH_SIZE = 1500;
      
        for (let i = 0; i < movements.length; i += BATCH_SIZE) {
            const batchMovements = movements.slice(i, i + BATCH_SIZE);
            const movementValues: any[] = batchMovements.map((m) => {
                const bankId = bankMap[m.FK_COD_BANCO_MOVI] ?? null;
                const idBoxDetail = boxMap[m.FK_COD_CAJAS_MOVI] ?? null;
                const transactionId = null;//payrollIdMap[m.FK_TRAC_MOVI] ?? null;
                const userId = userMap[m.FK_USER_EMP_MOVI] ?? null;
                let transAuditId = null;// mapAuditNomina[m.FK_TRAC_MOVI] ?? null;
                const idFkConciliation = mapConciliation[m.FK_CONCILIADO] ?? null;
                m.FK_ARQUEO = mapCloseCash[m.FK_ARQUEO] ?? null;

                //CXP-NOMINA
                //PSTO-NOMINA
                if (m.ORIGEN_MOVI.includes('CXP-NOMINA')) {
                    transAuditId = mapAuditPagoRoll[m.FK_COD_RH] ?? null;
                }
           
                return [
                    bankId,
                    transactionId,
                    idFkConciliation,
                    userId,
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
                    movementSequence++,
                    m.IMPOR_MOVI,
                    m.ESTADO_MOVI,
                    m.PER_BENE_MOVI,
                    m.CONCILIADO,
                    newCompanyId,
                    idBoxDetail,
                    m.OBS_MOVI,
                    m.TOTPAG_TRAC,
                    m.FK_ASIENTO,
                    transAuditId,
                    m.FK_ARQUEO,
                    m.TIPO_MOVI === 'TARJETA' ? cardId : null,
                    m.RECIBO_CAJA,
                    null,
                    m.NUM_UNIDAD,
                    m.JSON_PAGOS
                ];
            });

            const [resMov]: any = await conn.query(`
                INSERT INTO movements (
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
                ) VALUES ?
            `, [movementValues]);

            let nextMovementId = resMov.insertId;
            batchMovements.forEach(({ ID_MOVI }) => {
                movementOrderIdMap[ID_MOVI] = nextMovementId++;
            });

            console.log(` -> Batch migrado: ${batchMovements.length} movimientos de nómina`);
        }

        const { totalMigrated: totalLoans } = await migrateLoansPayrollAdvances(
            legacyConn,
            humanResourcesDb,
            conn,
            newCompanyId,
            mapClients,
            idEmpresaRhh,
            mapIdAsiento,
            bankMap,
            boxMap,
            mapCloseCash,
        );
        console.log(` -> Total migrado: ${totalLoans} préstamos y anticipos de nómina`);

        console.log("✅ Migración de movimientos de nómina completada correctamente");
        return { movementOrderIdMap, totalLoans };
    } catch (error) {
        console.error("❌ Error al migrar movimientos de nómina:", error);
        throw error;
    }
}

export async function migratePayrollSeats(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>,
    mapDetailAsiento: Record<number, number>,
    mapAuditPayroll: Record<number, number>,
    mapAuditPagoRoll: Record<number, number>,
    mapAuditAnticipoRoll: Record<number, number>,
    mapAuditMovimientosPagoRoll: Record<number, number>,
    mapIdAsiento: PayrollSeatIdMap
}> {


    console.log("🚀 Migrando encabezado de asiento contables anticipo proveedores..........");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const mapAuditPayroll: Record<number, number> = {};
        const mapAuditPagoRoll: Record<number, number> = {};
        const mapAuditAnticipoRoll: Record<number, number> = {};
        const mapAuditMovimientosPagoRoll: Record<number, number> = {};
        const mapIdAsiento: PayrollSeatIdMap = [];
        const [rows]: any[] = await legacyConn.query(`SELECT
    cod_asiento,
    fecha_asiento AS FECHA_ASI,
    descripcion_asiento AS DESCRIP_ASI,
    numero_asiento AS NUM_ASI,
   origen_asiento AS ORG_ASI,
debe_asiento AS TDEBE_ASI,
haber_asiento AS THABER_ASI,
numero_asiento,
tipo_asiento AS TIP_ASI,
fk_cod_periodo AS FK_PERIODO,
fecha_registro_asiento AS FECHA_REG,
fecha_update_asiento AS FECHA_ACT,
json_asi AS JSON_ASI,
res_asiento AS RES_ASI,
ben_asiento AS BEN_ASI,
NULL AS FK_AUDIT,
NULL AS FK_COD_EMP,
contabilidad_asientos.FK_CODTRAC,
NULL AS COD_TRAC,
CAST(
    REGEXP_REPLACE(
        RIGHT(numero_asiento, 9),
        '[^0-9]',
        ''
    ) AS UNSIGNED
) AS SEC_ASI,
cod_origen,
FK_CODMOV,
FK_CODPREST,

contabilidad_asientos.FK_CODROL
FROM
    contabilidad_asientos
WHERE
    origen_asiento LIKE '%NOMINA%' OR origen_asiento = 'MOD-ADELANTO';` );

        if (!rows.length) {
            return { mapEntryAccount, mapDetailAsiento: {}, mapAuditPayroll, mapAuditPagoRoll, mapAuditAnticipoRoll, mapAuditMovimientosPagoRoll, mapIdAsiento };
        }

        const BATCH_SIZE = 1000;
        // Obtener el siguiente código de auditoría
        let nextAuditId = await findNextAuditCode({ conn, companyId: newCompanyId });


        //origen_asiento
        //rol=NOMINA
        //pagos = CXP-NOMINA
        //PRESTAMOS =PSTO-NOMINA
        //ACTA FINIQUITO = ACTAFINQ-NOMINA
        //LIQ-BEN-NOMINA
        //MOD-ADELANTO



        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            // Insert audits first to capture auto-increment IDs (ID_AUDIT)
            const auditValues = batch.map(() => [nextAuditId++, 'NOMINA', newCompanyId]);
            const [resAudit]: any = await conn.query(
                `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
                [auditValues]
            );
            const firstAuditId = resAudit.insertId;

            for (let j = 0; j < batch.length; j++) {
                const o = batch[j];
                const periodoId = mapPeriodo[o.FK_PERIODO];
                const idAuditTr = firstAuditId + j;

                mapAuditPayroll[o.cod_asiento] = idAuditTr;
                (mapAuditPayroll as any)[String(o.cod_asiento)] = idAuditTr;
                if (o.NUM_ASI) {
                    (mapAuditPayroll as any)[String(o.NUM_ASI).trim()] = idAuditTr;
                }

                if (o.FK_CODROL != null) {
                    mapAuditPagoRoll[o.FK_CODROL] = idAuditTr;
                    mapIdAsiento.push({
                        key: 'FK_CODROL',
                        id: o.FK_CODROL,
                        cod_asiento: o.cod_asiento,
                        idAudit: idAuditTr
                    });
                }
                if (o.FK_CODPREST != null && o.ORG_ASI === 'MOD-ADELANTO') {
                    mapAuditAnticipoRoll[o.FK_CODPREST] = idAuditTr;
                    mapIdAsiento.push({
                        key: 'ID_ADELANTO',
                        id: o.FK_CODPREST,
                        cod_asiento: o.cod_asiento,
                        idAudit: idAuditTr
                    });
                } else {
                    if (o.FK_CODPREST != null) {
                        mapAuditAnticipoRoll[o.FK_CODPREST] = idAuditTr;
                        mapIdAsiento.push({
                            key: 'FK_CODPREST',
                            id: o.FK_CODPREST,
                            cod_asiento: o.cod_asiento,
                            idAudit: idAuditTr
                        });
                    }
                }
                if (o.FK_CODMOV != null) {
                    mapAuditMovimientosPagoRoll[o.FK_CODMOV] = idAuditTr; //finiquito y liquidacion
                    mapIdAsiento.push({
                        key: 'FK_CODMOV',
                        id: o.FK_CODMOV,
                        cod_asiento: o.cod_asiento,
                        idAudit: idAuditTr
                    });
                }

                const idMovimiento = null;

                insertValues.push([
                    o.FECHA_ASI,
                    o.DESCRIP_ASI,
                    o.NUM_ASI,
                    o.ORG_ASI,
                    o.TDEBE_ASI,
                    o.THABER_ASI,
                    o.TIP_ASI,
                    periodoId,
                    o.FECHA_REG,
                    o.FECHA_ACT,
                    o.JSON_ASI,
                    o.RES_ASI,
                    o.BEN_ASI,
                    idAuditTr,
                    newCompanyId,
                    o.SEC_ASI,
                    null,
                    idMovimiento
                ]);
            }

            // Insertar asientos contables
            const [res]: any = await conn.query(`INSERT INTO accounting_movements(
                    FECHA_ASI,
                    DESCRIP_ASI,
                    NUM_ASI,
                    ORG_ASI,
                    TDEBE_ASI,
                    THABER_ASI,
                    TIP_ASI,
                    FK_PERIODO,
                    FECHA_REG,
                    FECHA_ACT,
                    JSON_ASI,
                    RES_ASI,
                    BEN_ASI,
                    FK_AUDIT,
                    FK_COD_EMP,
                    SEC_ASI,
                    FK_MOVTRAC,
                    FK_MOV) VALUES ?`, [insertValues]);

            let newId = res.insertId;
            for (const o of batch) {
                mapEntryAccount[o.cod_asiento] = newId++;
            }

            console.log(` -> Batch migrado: ${batch.length} migrar asientos anticipo proveedores`);
        }
        console.log("✅ Migración asiento contable anticipo proveedores completada correctamente");

        // Pasar el map de auditoría a los detalles si es necesario
        const mapDetailAsiento = await migratePayrollSeatDetails(
            legacyConn,
            conn,
            newCompanyId,
            mapProject,
            mapCenterCost,
            mapAccounts,
            mapEntryAccount
        );

        console.log("✅ Migración asientos manuales completada correctamente");
        return { mapEntryAccount, mapDetailAsiento, mapAuditPayroll, mapAuditPagoRoll, mapAuditAnticipoRoll, mapAuditMovimientosPagoRoll, mapIdAsiento };
    } catch (err) {
        console.error("❌ Error en migración de asiento contable:", err);
        throw err;
    }
}


export async function migratePayrollSeatDetails(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {
    console.log("🚀 Cuentas contables");
    console.log("🚀 Iniciando migración de detalles de asientos contables anticipo nomina..........");

    const mapAccountDetail: Record<number, number> = {};

    const [rows]: any[] = await legacyConn.query(`SELECT
            d.cod_detalle_asiento,
            contabilidad_asientos.fecha_asiento,
            contabilidad_asientos.cod_asiento AS FK_COD_ASIENTO,
            d.debe_detalle_asiento AS DEBE_DET,
            d.haber_detalle_asiento AS HABER_DET,
            d.fk_cod_plan AS FK_CTAC_PLAN,
            d.fkProyectoCosto AS FK_COD_PROJECT,
            d.fkCentroCosto AS FK_COD_COST
FROM

    contabilidad_asientos
INNER JOIN contabilidad_detalle_asiento d ON d.fk_cod_asiento = contabilidad_asientos.cod_asiento

WHERE
     contabilidad_asientos.origen_asiento LIKE '%NOMINA%' OR origen_asiento = 'MOD-ADELANTO';`);

    if (!rows.length) {
        console.log("⚠️ No hay registros para migrar");
        return { mapAccountDetail };
    } //console.log(rows);

    const BATCH_SIZE = 1000;
    console.log(`📦 Total registros a migrar: ${rows.length}`);
    let totalDebe = 0;
    let totalHaber = 0;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        console.log(`➡️ Procesando batch ${i / BATCH_SIZE + 1}`);

        try {
            const insertValues: any[] = [];
            const totalsMap = new Map<string, any>();

            for (const o of batch) {
                const idPlan = mapAccounts[o.FK_CTAC_PLAN];
                const idProyecto = mapProject[o.FK_COD_PROJECT] ?? null;
                const idCentroCosto = mapCenterCost[o.FK_COD_COST] ?? null;
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO] ?? null;

                if (!idPlan || !idCodAsiento) continue;

                const debe = Number(o.DEBE_DET) || 0;
                const haber = Number(o.HABER_DET) || 0;

                insertValues.push([
                    idCodAsiento,
                    debe,
                    haber,
                    idPlan,
                    idProyecto,
                    idCentroCosto
                ]);

                const key = `${newCompanyId}-${idPlan}-${o.fecha_asiento}`;
                if (!totalsMap.has(key)) {
                    totalsMap.set(key, {
                        id_plan: idPlan,
                        fecha: o.fecha_asiento,
                        debe: 0,
                        haber: 0,
                        total: 0,
                        operacion: "suma"
                    });
                }

                const acc = totalsMap.get(key);
                acc.debe += debe;
                acc.haber += haber;
                acc.total++;

                totalHaber += haber;
                totalDebe += debe;
            }

            if (!insertValues.length) {
                console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
                continue;
            }

            const [res]: any = await conn.query(`
                INSERT INTO accounting_movements_det (
                    FK_COD_ASIENTO,
                    DEBE_DET,
                    HABER_DET,
                    FK_CTAC_PLAN,
                    FK_COD_PROJECT,
                    FK_COD_COST
                ) VALUES ?
            `, [insertValues]);

            let newId = res.insertId;

            for (const o of batch) {
                const idPlan = mapAccounts[o.FK_CTAC_PLAN];
                console.log(`➡️ Procesando detalle de asiento anticipo nomina ${idPlan}`);
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

                if (!idPlan || !idCodAsiento) continue;

                mapAccountDetail[o.cod_detalle_asiento] = newId++;
            }

            for (const t of totalsMap.values()) {
                await upsertTotaledEntry(conn, t, newCompanyId);
            }

            console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado anticipo nomina con ${insertValues.length} registros. Total acumulado - DEBE: ${totalDebe}, HABER: ${totalHaber}`);

        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }

    console.log("🎉 Migración  detalles contables completada anticipo nomina");
    return { mapAccountDetail };
}

export async function migratePayrollPaymentMovements(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapConciliation: Record<number, number>,
    userMap: Record<number, number>,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapCloseCash: Record<number, number>,
    mapEntryAccount: Record<number, number>,
    mapIdAsiento: PayrollSeatIdMap,
    mapAuditMovimientosPagoRoll: Record<number, number>,
): Promise<{ mapMovements: Record<number, number>; mapAuditMovements: Record<number, number>; mapMovementsByFkCodRH: Record<number, number> }> {
    console.log("Migrando movimientos de pago de nómina (CXP-NOMINA)...");

    const mapMovements: Record<number, number> = {};
    const mapAuditMovements: Record<number, number> = {};
    // FK_COD_RH (legacy) = tbMovimientos.id_movimiento (RRHH) → nuevo movements PK
    const mapMovementsByFkCodRH: Record<number, number> = {};

    // FK_CODMOV (legacy) → nuevo COD_ASIENTO en accounting_movements
    const fkCodMovToNewAsientoId: Record<number, number> = {};
    for (const entry of mapIdAsiento) {
        if (entry.key === 'FK_CODMOV') {
            const newAsientoId = mapEntryAccount[entry.cod_asiento];
            if (newAsientoId) fkCodMovToNewAsientoId[entry.id] = newAsientoId;
        }
    }

    try {
        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit FROM audit WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const [[{ nextSecu }]]: any = await conn.query(
            `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS nextSecu FROM movements WHERE MODULO = 'CXPN' AND FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const [[{ idCard }]]: any = await conn.query(
            `SELECT ID_TARJETA AS idCard FROM cards WHERE FK_COD_EMP = ? LIMIT 1`,
            [newCompanyId]
        );

        let auditSeq = nextAudit;
        let secuenciaMovimiento = nextSecu;

        const [rows]: any[] = await legacyConn.query(`
            SELECT
                m.ID_MOVI,
                NULL AS COD_TRANS,
                m.FK_COD_CAJAS_MOVI,
                m.FK_COD_BANCO_MOVI,
                m.TIP_MOVI AS TIP_MOVI,
                m.periodo_caja,
                m.FECHA_MOVI AS FECHA_MOVI,
                'CXPN' AS ORIGEN_MOVI,
                IFNULL(m.IMPOR_MOVI, 0) AS IMPOR_MOVI,
                IFNULL(m.TIPO_MOVI, 0) AS TIPO_MOVI,
                m.REF_MOVI,
                m.CONCEP_MOVI AS CONCEP_MOVIMIG,
                CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0 END AS ESTADO_MOVI,
                IFNULL(m.PER_BENE_MOVI, 'MIG') AS PER_BENE_MOVI,
                'EGRESO' AS CAUSA_MOVI,
                'CXPN' AS MODULO,
                m.FECHA_MANUAL AS FECHA_MANUAL,
                m.CONCILIADO,
                m.FK_COD_CX,
                m.SECU_MOVI,
                m.FK_CONCILIADO,
                m.FK_ANT_MOVI,
                m.FK_USER_EMP_MOVI AS FK_USER_EMP_MOVI,
                NULL AS FK_TRAC_MOVI,
                NULL AS NUM_VOUCHER,
                NULL AS NUM_LOTE,
                m.CONCEP_MOVI AS OBS_MOVI,
                NULL AS FK_ASIENTO,
                m.periodo_caja AS FK_ARQUEO,
                m.RECIBO_CAJA,
                m.NUM_UNIDAD,
                m.FK_COD_RH
            FROM movimientos m
            WHERE m.\`ORIGEN_MOVI\` = 'CXP-NOMINA'
            ORDER BY m.ID_MOVI ASC
        `);

        if (!rows.length) {
            console.warn("No hay movimientos de pago de nómina para migrar.");
            return { mapMovements, mapAuditMovements, mapMovementsByFkCodRH };
        }

        const BATCH_SIZE = 1500;
        // Pares [newMovementId, newAsientoId] para UPDATE accounting_movements.FK_MOV
        const asientoMovPairs: Array<[number, number]> = [];

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);

            // Pre-calcular CODIGO_AUT por empresa antes del INSERT para mantener consistencia
            const batchAuditCodes = batch.map(() => auditSeq++);
            const auditValues = batchAuditCodes.map((code: number) => [code, 'CXPN', newCompanyId]);
            const [resAudit]: any = await conn.query(
                `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
                [auditValues]
            );
            const firstAuditId = resAudit.insertId;

            const movementValues = batch.map((m: any, index: number) => {
                const fkAudit = mapAuditMovimientosPagoRoll[m.FK_COD_RH] ?? (firstAuditId + index);
                mapAuditMovements[m.ID_MOVI] = fkAudit;

                return [
                    bankMap[m.FK_COD_BANCO_MOVI] ?? null,
                    null,
                    mapConciliation[m.FK_CONCILIADO] ?? null,
                    userMap[m.FK_USER_EMP_MOVI] ?? null,
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
                    secuenciaMovimiento++,
                    m.IMPOR_MOVI,
                    m.ESTADO_MOVI,
                    m.PER_BENE_MOVI,
                    m.CONCILIADO,
                    newCompanyId,
                    boxMap[m.FK_COD_CAJAS_MOVI] ?? null,
                    m.OBS_MOVI,
                    m.IMPOR_MOVI,
                    null,
                    fkAudit,
                    mapCloseCash[m.FK_ARQUEO] ?? null,
                    m.TIP_MOVI === 'TARJETA' ? idCard : null,
                    m.RECIBO_CAJA,
                    null,
                    m.NUM_UNIDAD,
                    null
                ];
            });

            const [resMov]: any = await conn.query(`
                INSERT INTO movements (
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
                const newMovId = currentMovId++;
                mapMovements[m.ID_MOVI] = newMovId;
                if (m.FK_COD_RH != null) mapMovementsByFkCodRH[m.FK_COD_RH] = newMovId;
                const newAsientoId = fkCodMovToNewAsientoId[m.FK_COD_RH];
                if (newAsientoId) asientoMovPairs.push([newMovId, newAsientoId]);
            });

            console.log(` -> Batch migrado: ${batch.length} movimientos de pago de nómina (CXPN)`);
        }

        // Asociar accounting_movements.FK_MOV con el movimiento correspondiente
        if (asientoMovPairs.length) {
            const UPDT_SIZE = 500;
            for (let i = 0; i < asientoMovPairs.length; i += UPDT_SIZE) {
                const chunk = asientoMovPairs.slice(i, i + UPDT_SIZE);
                const caseWhen = chunk.map(() => 'WHEN ? THEN ?').join(' ');
                const inIds = chunk.map(([, asientoId]) => asientoId);
                const params: any[] = chunk.flatMap(([movId, asientoId]) => [asientoId, movId]);
                params.push(inIds);
                await conn.query(
                    `UPDATE accounting_movements SET FK_MOV = CASE COD_ASIENTO ${caseWhen} END WHERE COD_ASIENTO IN (?)`,
                    params
                );
            }
            console.log(` -> accounting_movements.FK_MOV actualizado: ${asientoMovPairs.length} asientos vinculados`);
        }

        console.log("✅ Migración de movimientos de pago de nómina completada correctamente");
        return { mapMovements, mapAuditMovements, mapMovementsByFkCodRH };
    } catch (error) {
        console.error("❌ Error al migrar movimientos de pago de nómina:", error);
        throw error;
    }
}

export async function migratePayrollPaymentDetails(
    humanResourcesDb: any,
    conn: any,
    idEmpresaRhh: number | null,
    mapObligations: Record<number, number>,
    mapMovementsByFkCodRH: Record<number, number>,
): Promise<{ mapPaymentDetails: Record<number, number> }> {
    console.log("Migrando detalles de pago de nómina (account_detail CXPN)...");

    const mapPaymentDetails: Record<number, number> = {};

    if (!humanResourcesDb || !idEmpresaRhh) {
        console.warn("No se migran detalles de pago de nómina: no hay conexión o empresa RRHH.");
        return { mapPaymentDetails };
    }

    try {
        const [rows]: any[] = await humanResourcesDb.query(`
            SELECT
                tbMovimientos.id_movimiento   AS COD_DETCUENTA,
                tbMovimientos.idfk_cuenta_rol AS FK_COD_CUENTA,
                tbMovimientos.id_movimiento   AS FK_ID_MOVI,
                tbMovimientos.registro_movimiento AS FECHA_REG,
                tbMovimientos.importe_movimiento  AS IMPORTE,
                0 AS SALDO,
                0 AS NEW_SALDO
            FROM tbMovimientos
            INNER JOIN tbCuentasRol ON tbMovimientos.idfk_cuenta_rol = tbCuentasRol.id_cuenta
            WHERE tbMovimientos.idfk_empresa = ?
            ORDER BY tbMovimientos.id_movimiento ASC
        `, [idEmpresaRhh]);

        if (!rows.length) {
            console.warn("No hay detalles de pago de nómina para migrar.");
            return { mapPaymentDetails };
        }

        const BATCH_SIZE = 1500; console.log(mapObligations, mapMovementsByFkCodRH);

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];
            const validRows: any[] = [];


            for (const o of batch) {
                const newCuentaId = mapObligations[o.FK_COD_CUENTA] ?? null;
                const newMoviId = mapMovementsByFkCodRH[o.FK_ID_MOVI] ?? null;

                if (!newCuentaId || !newMoviId) continue;

                insertValues.push([
                    newCuentaId,
                    newMoviId,
                    o.FECHA_REG,
                    o.IMPORTE,
                    o.SALDO,
                    o.NEW_SALDO,
                ]);
                validRows.push(o);
            }

            if (!insertValues.length) continue;

            const [res]: any = await conn.query(`
                INSERT INTO account_detail (
                    FK_COD_CUENTA, FK_ID_MOVI, FECHA_REG, IMPORTE, SALDO, NEW_SALDO
                ) VALUES ?
            `, [insertValues]);

            let newId = res.insertId;
            for (const o of validRows) {
                mapPaymentDetails[o.COD_DETCUENTA] = newId++;
            }

            console.log(` -> Batch migrado: ${insertValues.length} detalles de pago de nómina`);
        }

        console.log("✅ Migración de detalles de pago de nómina completada correctamente");
        return { mapPaymentDetails };
    } catch (error) {
        console.error("❌ Error al migrar detalles de pago de nómina:", error);
        throw error;
    }
}
