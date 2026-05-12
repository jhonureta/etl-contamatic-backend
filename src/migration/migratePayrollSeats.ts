import { migratePayrollConfiguration } from "./migrateEmployes";
import { migrateLoansPayrollAdvances } from "./migrateLoans";
import { upsertTotaledEntry } from "./migrationTools";
import { findNextAuditCode, insertAudit } from "./purchaseHelpers";

type PayrollSeatIdMap = Array<{
    key: 'FK_CODROL' | 'FK_CODPREST' | 'FK_CODMOV' | 'ID_ADELANTO';
    id: number;
    cod_asiento: number;
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
): Promise<{ movementOrderIdMap: Record<number, number> }> {
    //mapAuditPayroll

    let idEmpresaRhh = null;

    idEmpresaRhh = await getEmpresaRhhId(humanResourcesDb, codEmp);


    const { mapDepartments, mapPositions, mapSalaries, mapEmployes, mappingContracts } = await migratePayrollConfiguration(
        legacyConn,
        conn,
        newCompanyId,
        humanResourcesDb,
        idEmpresaRhh
    );


    const {
        mapAuditPayroll,
        mapAuditPagoRoll,
        mapAuditAnticipoRoll,
        mapAuditMovimientosPagoRoll, mapIdAsiento } = await migratePayrollSeats(legacyConn,
            conn,
            newCompanyId,
            mapPeriodo,
            mapProject,
            mapCenterCost,
            mapAccounts);
    const mapClients = mapEmployes;
    await migratePayrollMovements(
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
        codEmp ?? null
    );

    return { movementOrderIdMap: {} };
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
): Promise<{ movementOrderIdMap: Record<number, number> }> {
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
            WHERE m.ORIGEN_MOVI LIKE '%NOMINA%' OR m.ORIGEN_MOVI = 'MOD-ADELANTO'
            ORDER BY ID_MOVI ASC
        `);

        if (movements.length === 0) {
            return { movementOrderIdMap };
        }

        const [cardData]: any[] = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`, [newCompanyId]);
        const cardId = cardData[0]?.ID_TARJETA ?? null;

        const [[movementSeqRow]]: any[] = await conn.query(
            `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS SECU_MOVI FROM movements WHERE MODULO = 'NOMINA' AND FK_COD_EMP = ?`,
            [newCompanyId]
        );
        let movementSequence = movementSeqRow?.SECU_MOVI ?? 1;

        const BATCH_SIZE = 1500;
        const hasPayrollLoanAdvances = movements.some((m) => m.ORIGEN_MOVI?.includes('PSTO-NOMINA'));
        const { advanceAuditMap } = await getAdvanceAuditMap(humanResourcesDb, codEmp, mapIdAsiento, mapAuditPayroll, idEmpresaRhh);

        if (hasPayrollLoanAdvances) {
            await migrateLoansPayrollAdvances(
                legacyConn,
                conn,
                mapClients,
                newCompanyId,
                idEmpresaRhh,
                mapIdAsiento,
                mapConciliation,
                userMap,
                bankMap,
                boxMap,
                mapCloseCash
            );
        }

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
                if (m.ORIGEN_MOVI.includes('PSTO-NOMINA')) {
                    transAuditId = mapAuditAnticipoRoll[m.FK_DET_PREST] ?? null;
                    m.FK_ASIENTO = mapIdAsiento.find(
                        asiento => asiento.key === 'FK_CODPREST' && asiento.id === m.FK_DET_PREST
                    )?.cod_asiento ?? null;
                }
                if (m.ORIGEN_MOVI === 'MOD-ADELANTO') {
                    m.FK_ASIENTO = advanceAuditMap[m.FK_DET_PREST] ?? null;
                    transAuditId = mapAuditPayroll[m.FK_ASIENTO] ?? null;
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

        console.log("✅ Migración de movimientos de nómina completada correctamente");
        return { movementOrderIdMap };
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
            const auditValues: any[] = [];

            for (const o of batch) {
                const periodoId = mapPeriodo[o.FK_PERIODO];
                // Generar y asignar auditoría para cada asiento
                const auditId = nextAuditId++;
                auditValues.push([auditId, 'NOMINA', newCompanyId]);
                mapAuditPayroll[o.cod_asiento] = auditId;

                if (o.FK_CODROL != null) {
                    mapAuditPagoRoll[o.FK_CODROL] = auditId;
                    mapIdAsiento.push({
                        key: 'FK_CODROL',
                        id: o.FK_CODROL,
                        cod_asiento: o.cod_asiento
                    });
                }
                if (o.FK_CODPREST != null && o.ORG_ASI === 'MOD-ADELANTO') {
                    mapAuditAnticipoRoll[o.FK_CODPREST] = auditId;
                    mapIdAsiento.push({
                        key: 'ID_ADELANTO',
                        id: o.FK_CODPREST,
                        cod_asiento: o.cod_asiento
                    });
                } else {
                    if (o.FK_CODPREST != null) {
                        mapAuditAnticipoRoll[o.FK_CODPREST] = auditId;
                        mapIdAsiento.push({
                            key: 'FK_CODPREST',
                            id: o.FK_CODPREST,
                            cod_asiento: o.cod_asiento
                        });
                    }
                }
                if (o.FK_CODMOV != null) {
                    mapAuditMovimientosPagoRoll[o.FK_CODMOV] = auditId; //finiquito y liquidacion
                    mapIdAsiento.push({
                        key: 'FK_CODMOV',
                        id: o.FK_CODMOV,
                        cod_asiento: o.cod_asiento
                    });
                }



                const idAuditTr = auditId;
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

            // Insertar auditorías
            if (auditValues.length) {
                await conn.query(`INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`, [auditValues]);
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
