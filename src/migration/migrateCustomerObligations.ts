import { fetchAccountingPeriod, upsertTotaledEntry } from "./migrationTools";

export async function migrateCustomerAccounting(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapAuditSales: Record<number, number | null>,
    mapClients: Record<number, number | null>,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    userMap: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapConciliation: Record<number, number | null>
) {

    console.log("🧩 Iniciando migración contable de clientes");

    /* ──────────────────────────────
     1️⃣ OBLIGACIONES
    ────────────────────────────── */
    const {
        mapObligationsCustomers,
        mapObligationsAudit,
        oblAsiToAuditId
    } = await migrateMovementsTransactions(
        legacyConn,
        conn,
        newCompanyId,
        mapSales,
        mapAuditSales,
        mapClients,
        userMap,
        mapAccounts
    );

    console.log(
        `✔ Obligaciones migradas: ${Object.keys(mapObligationsCustomers).length}`
    );

    console.log(
        `✔ Auditoria migradas: ${Object.keys(mapObligationsAudit).length}`
    );


    /*  MOVIMIENTOS QUE DENTRO DE MULTIPAGOS NO TENGAN CREDITO */
    const { mapMovements } = await migrateMovementsObligations(
        legacyConn,
        conn,
        newCompanyId,
        mapSales,
        mapAuditSales,
        mapObligationsAudit,
        bankMap,
        boxMap,
        userMap,
        mapConciliation
    );

    /*  DETALLE DE MOVIMIENTOS DE CREDITO POR MULTIPAGOS */


    console.log(`✔ Detalle de movimientos migrados: ${Object.keys(mapMovements).length}`);

    const { mapMovementsSales } = await migrateMovementsObligationsCredito(
        legacyConn,
        conn,
        newCompanyId,
        mapSales,
        mapAuditSales,
        mapObligationsAudit,
        bankMap,
        mapMovements,
        mapConciliation
    );
    console.log(`✔ Detalle de movimientos por credito multipagos migrado: ${Object.keys(mapMovementsSales).length}`);




    /*MIGRAR ENCABEZADO DE ASIENTO CONTABLE */


    const { mapEntryAccount } = await migrateSalesAccountingEntries(
        legacyConn,
        conn,
        newCompanyId,
        mapSales,
        mapAuditSales,
        mapMovements,
        mapPeriodo,
    )

    console.log(`✔ Encabezado de asientos contables: ${Object.keys(mapEntryAccount).length}`);


    const { mapAccountDetail } = await migrateSalesAccountingEntriesDetail(
        legacyConn,
        conn,
        newCompanyId,
        mapProject,
        mapCenterCost,
        mapAccounts,
        mapEntryAccount
    )
    console.log(`✔ Asientos contables detalle: ${Object.keys(mapAccountDetail).length}`);


    return { mapMovements, mapObligationsCustomers, mapObligationsAudit };
}



export async function migrateMovementsTransactions(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapAuditSales: Record<number, number | null>,
    mapClients: Record<number, number | null>,
    userMap: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
): Promise<{
    mapObligationsCustomers: Record<number, number>,
    mapObligationsAudit: Record<number, number>,
    oblAsiToAuditId: Record<number, number>
}> {

    console.log("🚀 Migrando obligaciones clientes");

    // Tipados locales
    type OblRow = {
        old_id: number;
        fk_persona_old: number;
        fk_cod_trans_old: number | null;
        obl_asi_group: number | null;
        tipo_obl: string;
        fech_emision: any;
        fech_vencimiento: any;
        tip_doc: string;
        estado: string;
        saldo: number;
        total: number;
        ref_secuencia: string;
        tipo_cuenta: string;
        fk_id_infcon: number | null;
        tipo_estado_cuenta: string;
        fk_cod_usu_cp: number | null;
    };

    const mapObligationsCustomers: Record<number, number> = {};
    const mapObligationsAudit: Record<number, number> = {};
    const oblAsiToAuditId: Record<number, number> = {};
    const oblAsiToAuditIdUser: Record<number, number | null> = {};
    const oblAsientoToAuditId: Record<number, number> = {};

    // Helpers internos
    async function fetchNextAuditSeq(conn: any, companyId: number): Promise<number> {
        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit
             FROM audit
             WHERE FK_COD_EMP = ?`,
            [companyId]
        );
        return Number(nextAudit ?? 1);
    }

    async function createAudit(conn: any, codigoAut: number, companyId: number): Promise<number> {
        const [resAudit]: any = await conn.query(
            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP)
             VALUES (?, 'VENTAS', ?)`,
            [codigoAut, companyId]
        );
        return resAudit.insertId;
    }

    function buildCuentasOblRow(o: OblRow, auditId: number) {
        return [
            mapClients[o.fk_persona_old],
            o.tipo_obl,
            o.fech_emision,
            o.fech_vencimiento,
            o.tip_doc,
            o.estado,
            o.saldo,
            o.total,
            o.ref_secuencia,
            mapSales[o.fk_cod_trans_old] ?? null,
            o.tipo_cuenta,
            o.fk_id_infcon,
            o.tipo_estado_cuenta,
            auditId,
            new Date(),
            newCompanyId
        ];
    }

    try {
        const BATCH_SIZE = 500;

        let auditSeq: number = await fetchNextAuditSeq(conn, newCompanyId);

        const [rows]: any[] = await legacyConn.query(`
            SELECT
                cod_cp AS old_id,
                fk_cod_cli_cp AS fk_persona_old,
                FK_TRAC_CUENTA AS fk_cod_trans_old,
                OBL_ASI AS obl_asi_group,
                Tipo_cxp AS tipo_obl,
                fecha_emision_cxp AS fech_emision,
                fecha_vence_cxp AS fech_vencimiento,
                tipo_documento AS tip_doc,
                estado_cxp AS estado,
                saldo_cxp AS saldo,
                valor_cxp AS total,
                referencia_cxp AS ref_secuencia,
                TIPO_CUENTA AS tipo_cuenta,
                FK_SERVICIO AS fk_id_infcon,
                TIPO_ESTADO_CUENTA AS tipo_estado_cuenta,
                fk_cod_usu_cp
            FROM cuentascp
            WHERE Tipo_cxp = 'CXC'
            ORDER BY cod_cp
        `);

        if (!rows.length) {
            return { mapObligationsCustomers, mapObligationsAudit, oblAsiToAuditId };
        }

        let importadoAuditId: number | null = null;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch: OblRow[] = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                let auditId: number | null = null;

                // 1) Si existe mapeo por transacción
                if (o.fk_cod_trans_old && mapAuditSales[o.fk_cod_trans_old]) {
                    auditId = mapAuditSales[o.fk_cod_trans_old]!;
                }

                // 2) Si pertenece a un grupo de obl_asi
                else if (o.obl_asi_group !== null) {
                    if (!oblAsiToAuditId[o.obl_asi_group]) {
                        const codigoAut = auditSeq++;
                        const newAuditId = await createAudit(conn, codigoAut, newCompanyId);
                        oblAsiToAuditId[o.obl_asi_group] = newAuditId;
                        oblAsiToAuditIdUser[newAuditId] = userMap[o.fk_cod_usu_cp];
                        oblAsientoToAuditId[newAuditId] = o.obl_asi_group;
                    }
                    auditId = oblAsiToAuditId[o.obl_asi_group];
                }

                // 3) Si es importado
                else if (o.tipo_cuenta === 'Importado') {
                    if (!importadoAuditId) {
                        const codigoAut = auditSeq++;
                        importadoAuditId = await createAudit(conn, codigoAut, newCompanyId);
                        oblAsiToAuditIdUser[importadoAuditId] = userMap[o.fk_cod_usu_cp];
                        oblAsientoToAuditId[importadoAuditId] = o.obl_asi_group as any;
                    }
                    auditId = importadoAuditId;
                }

                if (!auditId) {
                    throw new Error(`No se pudo resolver AUDIT para obligación ${o.old_id}`);
                }

                mapObligationsAudit[o.old_id] = auditId;

                const customerId = mapClients[o.fk_persona_old];
                if (!customerId) {
                    throw new Error(`Cliente no mapeado: ${o.fk_persona_old}`);
                }

                insertValues.push(buildCuentasOblRow(o, auditId));
            }

            const [res]: any = await conn.query(`
                INSERT INTO cuentas_obl (
                    FK_PERSONA, TIPO_OBL, FECH_EMISION, FECH_VENCIMIENTO, TIP_DOC,
                    ESTADO, SALDO, TOTAL, REF_SECUENCIA, FK_COD_TRANS,
                    TIPO_CUENTA, FK_ID_INFCON, TIPO_ESTADO_CUENTA, FK_AUDITOB,
                    OBLG_FEC_REG, FK_COD_EMP
                ) VALUES ?
            `, [insertValues]);

            let newId = res.insertId;
            for (const o of batch) {
                mapObligationsCustomers[o.old_id] = newId++;
            }
        }

        // MIGRAR OBLIGACIONES MIGRADAS (misma lógica que antes)
        const movimientosMigrados = await migrateMovementsObligationsMigrados(
            legacyConn,
            conn,
            newCompanyId,
            oblAsiToAuditIdUser,
            oblAsientoToAuditId,
            mapAccounts
        );

        console.log("✅ Migración completada correctamente");
        return { mapObligationsCustomers, mapObligationsAudit, oblAsiToAuditId };
    } catch (err) {
        console.error("❌ Error en migración de obligaciones:", err);
        throw err;
    }
}

export async function migrateMovementsObligationsMigrados(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    oblAsiToAuditIdUser: Record<number, number | null>,
    oblAsientoToAuditId: Record<number, number>,
    mapAccounts: Record<number, number | null>,
): Promise<{
    movimientosMigrados: Record<number, number>
}> {
    console.log("🚀 Migrando movimientos importados");
    const movimientosMigrados: Record<number, number> = {};
    const mapAsientos: Record<number, number> = {};
    const ArrayMovements: any[] = [];

    type AggRow = {
        IMPORTE: number;
        COD_CUENTA: number | null;
        FK_PERSONA: number | null;
        TIPO_OBL: string | null;
        FECH_EMISION: any;
        FECH_VENCIMIENTO: any;
        TIP_DOC: string | null;
        ESTADO: string | null;
        SALDO: number;
        FK_COD_TRANS: number | null;
        FK_PAYROLL: number | null;
        TIPO_CUENTA: string | null;
        TIPO_ESTADO_CUENTA: string | null;
        FK_COD_EMP: number | null;
        FK_AUDITOB: number;
        REF_SECUENCIA: string | null;
        FK_ID_INFCON: number | null;
        OBLG_FEC_REG: any;
    };

    const BATCH_SIZE = 500;

    try {
        const [rows]: any[] = await conn.query(`SELECT
                                                SUM(TOTAL) AS IMPORTE,
                                                MAX(COD_CUENTA) AS COD_CUENTA,
                                                MAX(FK_PERSONA) AS FK_PERSONA,
                                                MAX(TIPO_OBL) AS TIPO_OBL,
                                                MAX(FECH_EMISION) AS FECH_EMISION,
                                                MAX(FECH_VENCIMIENTO) AS FECH_VENCIMIENTO,
                                                MAX(TIP_DOC) AS TIP_DOC,
                                                MAX(ESTADO) AS ESTADO,
                                                SUM(SALDO) AS SALDO,
                                                MAX(FK_COD_TRANS) AS FK_COD_TRANS,
                                                MAX(FK_PAYROLL) AS FK_PAYROLL,
                                                MAX(TIPO_CUENTA) AS TIPO_CUENTA,
                                                MAX(TIPO_ESTADO_CUENTA) AS TIPO_ESTADO_CUENTA,
                                                MAX(FK_COD_EMP) AS FK_COD_EMP,
                                                FK_AUDITOB,
                                                MAX(REF_SECUENCIA) AS REF_SECUENCIA,
                                                MAX(FK_ID_INFCON) AS FK_ID_INFCON,
                                                MAX(OBLG_FEC_REG) AS OBLG_FEC_REG
                                            FROM cuentas_obl
                                            WHERE TIPO_OBL = 'CXC'
                                            AND TIPO_CUENTA = 'Importado'
                                            AND FK_COD_EMP = ?
                                            GROUP BY FK_AUDITOB;`,
            [newCompanyId]
        );

        if (!rows.length) {
            return { movimientosMigrados };
        }

        const [movSec]: any = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO= 'IMP-CXC' AND  FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let secuenciaMovimiento = movSec[0]?.SECU_MOVI ?? 1;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch: AggRow[] = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                const idUser = oblAsiToAuditIdUser[o.FK_AUDITOB];
                const idAuditTr = o.FK_AUDITOB;

                ArrayMovements.push({
                    secuencia: secuenciaMovimiento,
                    importe: o.IMPORTE,
                    auditoria: o.FK_AUDITOB,
                    emision: o.FECH_EMISION,
                    registro: o.OBLG_FEC_REG,
                });

                insertValues.push([
                    null,
                    null,
                    null,
                    idUser,
                    o.FECH_EMISION,
                    o.OBLG_FEC_REG,
                    'IMP-CXC',
                    'IMP-CXC',
                    'IMPORTACION',
                    'IMPORTACION DE OBLIGACIONES',
                    'Importación',
                    null,
                    null,
                    'INGRESO',
                    'IMP-CXC',
                    secuenciaMovimiento,
                    o.IMPORTE,
                    1,
                    'IMPORTACION A SISTEMA',
                    null,
                    newCompanyId,
                    null,
                    'Importación',
                    o.IMPORTE,
                    null,
                    idAuditTr,
                    null,
                    null,
                    null,
                    null,
                    null,
                    '[]'
                ]);

                secuenciaMovimiento++;
            }

            const [res]: any = await conn.query(`INSERT INTO movements(
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
                                                VALUES ?`, [insertValues]);

            let newId = res.insertId;
            for (const o of batch) {
                movimientosMigrados[o.FK_AUDITOB] = newId++;
            }
        }

        for (let i = 0; i < ArrayMovements.length; i += BATCH_SIZE) {
            const batch = ArrayMovements.slice(i, i + BATCH_SIZE);
            const insertValuesEntry: any[] = [];

            for (const o of batch) {
                const idMovimiento = movimientosMigrados[o.auditoria];
                const periodo = await fetchAccountingPeriod(conn, { fechaManual: o.emision, companyId: newCompanyId });
                const periodoId = periodo[0].COD_PERIODO;
                const secuenciaMovimiento = (o.secuencia).toString().padStart(9, '0');
                insertValuesEntry.push([
                    o.emision,
                    'N' + secuenciaMovimiento + '-Importación',
                    'ASICXC-' + secuenciaMovimiento,
                    'CXC',
                    o.importe,
                    o.importe,
                    'IMP-CXC',
                    periodoId,
                    o.registro,
                    o.registro,
                    '[]',
                    'US-MIG',
                    'IMPORTACION A SISTEMA',
                    o.auditoria,
                    newCompanyId,
                    o.secuencia,
                    null,
                    idMovimiento
                ]);
            }

            const [res]: any = await conn.query(`INSERT INTO accounting_movements(FECHA_ASI,
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
                     FK_MOV) VALUES ?`, [insertValuesEntry]);

            let newId = res.insertId;
            for (const o of batch) {
                mapAsientos[o.auditoria] = newId++;
            }
        }

        const [rowsAsientoDetail]: any[] = await legacyConn.query(`SELECT contabilidad_detalle_asiento.fk_cod_asiento,contabilidad_detalle_asiento.fk_cod_plan, contabilidad_detalle_asiento.debe_detalle_asiento, contabilidad_detalle_asiento.haber_detalle_asiento FROM contabilidad_asientos inner join contabilidad_detalle_asiento on contabilidad_asientos.cod_asiento= contabilidad_detalle_asiento.fk_cod_asiento where tipo_asiento = 'OBLIGACIONES' limit 2`, [newCompanyId]);

        if (!rowsAsientoDetail.length) {
            /* return { movimientosMigrados }; */
        }

        if (rowsAsientoDetail.length !== 2) {
            throw new Error("Asiento inválido: debe tener exactamente 2 detalles");
        }

        const debeRow = rowsAsientoDetail.find(r => Number(r?.debe_detalle_asiento) > 0);
        const haberRow = rowsAsientoDetail.find(r => Number(r?.haber_detalle_asiento) > 0);

        if (!debeRow || !haberRow) {
            throw new Error("Asiento inválido: no se encontró correctamente DEBE y HABER");
        }

        const cuentaPlanDebe = mapAccounts[debeRow.fk_cod_plan];
        const cuentaPlanHaber = mapAccounts[haberRow.fk_cod_plan];

        let totalDebe = 0;
        let totalHaber = 0;

        for (let i = 0; i < ArrayMovements.length; i += BATCH_SIZE) {
            const batch = ArrayMovements.slice(i, i + BATCH_SIZE);

            try {
                const insertValues: any[] = [];
                const totalsMap = new Map<string, any>();

                for (const o of batch) {
                    const idCodAsiento = mapAsientos[o.auditoria];
                    if (!idCodAsiento) continue;

                    const debe = Number(o.importe) || 0;
                    const haber = Number(o.importe) || 0;

                    insertValues.push([
                        idCodAsiento,
                        debe,
                        0,
                        cuentaPlanDebe,
                        null,
                        null
                    ]);

                    insertValues.push([
                        idCodAsiento,
                        0,
                        haber,
                        cuentaPlanHaber,
                        null,
                        null
                    ]);

                    const keyDebe = `${newCompanyId}-${cuentaPlanDebe}-${o.emision}`;
                    if (!totalsMap.has(keyDebe)) {
                        totalsMap.set(keyDebe, {
                            id_plan: cuentaPlanDebe,
                            fecha: o.emision,
                            debe: 0,
                            haber: 0,
                            total: 0,
                            operacion: "suma"
                        });
                    }

                    const accDebe = totalsMap.get(keyDebe);
                    accDebe.debe += debe;
                    accDebe.total++;

                    const keyHaber = `${newCompanyId}-${cuentaPlanHaber}-${o.emision}`;
                    if (!totalsMap.has(keyHaber)) {
                        totalsMap.set(keyHaber, {
                            id_plan: cuentaPlanHaber,
                            fecha: o.emision,
                            debe: 0,
                            haber: 0,
                            total: 0,
                            operacion: "suma"
                        });
                    }

                    const accHaber = totalsMap.get(keyHaber);
                    accHaber.haber += haber;
                    accHaber.total++;

                    totalDebe += debe;
                    totalHaber += haber;
                }

                if (!insertValues.length) {
                    console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
                    continue;
                }

                await conn.query(`
            INSERT INTO accounting_movements_det (
                FK_COD_ASIENTO,
                DEBE_DET,
                HABER_DET,
                FK_CTAC_PLAN,
                FK_COD_PROJECT,
                FK_COD_COST
            ) VALUES ?
        `, [insertValues]);

                for (const t of totalsMap.values()) {
                    await upsertTotaledEntry(conn, t, newCompanyId);
                }

                console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado`);

            } catch (err) {
                console.error("❌ Error en batch:", err);
                throw err;
            }
        }

        return { movimientosMigrados };
    } catch (err) {
        console.error("❌ Error en migración de obligaciones:", err);
        throw err;
    }
}

export async function migrateMovementsObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapAuditSales: Record<number, number | null>,
    mapObligationsAudit: Record<number, number | null>,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    userMap: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
): Promise<{
    mapMovements: Record<number, number>
}> {

    console.log("🚀 Migrando movimientos clientes");
    const mapMovements: Record<number, number> = {};
    try {//IMPORTE_GD

        const [rows]: any[] = await legacyConn.query(` SELECT
                                                    m.ID_MOVI,
                                                    t.COD_TRAC AS COD_TRANS,
                                                    m.FK_COD_CAJAS_MOVI,
                                                    m.FK_COD_BANCO_MOVI,
                                                    IFNULL(m.TIP_MOVI, t.METPAG_TRAC) AS TIP_MOVI,
                                                    m.periodo_caja,
                                                    IFNULL(m.FECHA_MOVI, t.fecha) AS FECHA_MOVI,
                                                    'VENTA' AS ORIGEN_MOVI,
                                                    IFNULL(m.IMPOR_MOVI, t.TOTPAG_TRAC) AS IMPOR_MOVI,
                                                    IFNULL(m.TIPO_MOVI, t.METPAG_TRAC) AS TIPO_MOVI,
                                                    m.REF_MOVI,
                                                    CASE 
                                                        WHEN t.TIP_TRAC = 'Fisica' THEN IFNULL(t.NUM_TRACFIC,'')
                                                        WHEN t.TIP_TRAC = 'Electronica' THEN IFNULL(t.NUM_TRAC,'')
                                                        ELSE IFNULL(t.NUM_TRACCI,'')
                                                    END AS CONCEP_MOVIMIG,
                                                    CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0 END AS ESTADO_MOVI,
                                                    IFNULL(m.PER_BENE_MOVI,IFNULL(mt.PER_BENE_MOVI, 'MIG')) AS PER_BENE_MOVI,
                                                    'INGRESO' AS CAUSA_MOVI,
                                                    'VENTAS' AS MODULO,
                                                    IFNULL(m.FECHA_MANUAL, t.FEC_TRAC) AS FECHA_MANUAL,
                                                    m.CONCILIADO,
                                                    m.FK_COD_CX,
                                                    m.SECU_MOVI,
                                                    m.FK_CONCILIADO,
                                                    m.FK_ANT_MOVI,
                                                    IFNULL(m.FK_USER_EMP_MOVI, t.FK_COD_USU) AS FK_USER_EMP_MOVI,
                                                    IFNULL(m.FK_TRAC_MOVI, t.COD_TRAC) AS FK_TRAC_MOVI, mt.NUM_VOUCHER as NUM_VOUCHER, mt.NUM_LOTE as NUM_LOTE,
                                                   m.CONCEP_MOVI as OBS_MOVI, t.TOTPAG_TRAC , NULL AS FK_ASIENTO, NULL AS FK_ARQUEO, m.RECIBO_CAJA, m.NUM_UNIDAD
                                                FROM transacciones t
                                                LEFT JOIN movimientos m 
                                                    ON m.FK_TRAC_MOVI = t.COD_TRAC
                                                LEFT JOIN movimientos_tarjeta mt on mt.FK_TRAC_MOVI = t.COD_TRAC    
                                                WHERE t.TIP_TRAC IN ('Electronica','Fisica','comprobante-ingreso')  ORDER BY ID_MOVI ASC;`
        );

        if (!rows.length) {
            return { mapMovements };
        }


        const [cardGeneric]: any = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        const idCard = cardGeneric[0]?.ID_TARJETA ?? null;


        const [movSec]: any = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO= 'VENTAS' AND  FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let secuenciaMovimiento = movSec[0]?.SECU_MOVI ?? 1;

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {

                let auditId: number | null = null;


                mapObligationsAudit[o.old_id] = auditId;
                o.JSON_PAGOS = '[]';
                const idBanco = bankMap[o.FK_COD_BANCO_MOVI];
                const idDetCaja = boxMap[o.FK_COD_CAJAS_MOVI]
                const idTrn = mapSales[o.FK_TRAC_MOVI]
                const idUser = userMap[o.FK_USER_EMP_MOVI]
                const idAuditTr = mapAuditSales[o.COD_TRANS];
                const idFkConciliation = mapConciliation[o.FK_CONCILIADO] ?? null;
                const idPlanCuenta = null;


                /* if (!idTrn) {

                } */

                //o.ID_MOVI,
                insertValues.push([
                    idBanco,//ok
                    idTrn,//ok
                    idFkConciliation,//ok
                    idUser,
                    o.FECHA_MOVI,
                    o.FECHA_MANUAL,
                    o.TIP_MOVI,
                    o.ORIGEN_MOVI,
                    o.TIPO_MOVI,
                    o.REF_MOVI,
                    o.CONCEP_MOVIMIG,
                    o.NUM_VOUCHER,
                    o.NUM_LOTE,
                    o.CAUSA_MOVI,
                    o.MODULO,
                    secuenciaMovimiento,
                    o.IMPOR_MOVI,
                    o.ESTADO_MOVI,
                    o.PER_BENE_MOVI,
                    o.CONCILIADO,
                    newCompanyId,
                    idDetCaja,
                    o.OBS_MOVI,
                    o.TOTPAG_TRAC,
                    o.FK_ASIENTO,
                    idAuditTr,
                    o.FK_ARQUEO,
                    o.TIPO_MOVI === 'TARJETA' ? idCard : null,
                    o.RECIBO_CAJA,
                    idPlanCuenta,
                    o.NUM_UNIDAD,
                    o.JSON_PAGOS
                ]);

                secuenciaMovimiento++;
            }

            const [res]: any = await conn.query(`INSERT INTO movements(
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
                                                VALUES ?`, [insertValues]);

            let newId = res.insertId;
            for (const o of batch) {
                mapMovements[o.COD_TRANS] = newId++;
            }
        }
        console.log("✅ Migración completada correctamente");
        return { mapMovements };
    } catch (err) {
        console.error("❌ Error en migración de obligaciones:", err);
        throw err;
    }
}

export async function migrateMovementsObligationsCredito(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapAuditSales: Record<number, number | null>,
    mapObligationsAudit: Record<number, number | null>,
    userMap: Record<number, number | null>,
    mapMovements: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
): Promise<{
    mapMovementsSales: Record<number, number>
}> {
    console.log("🚀 Migrando obligaciones clientes");
    try {//IMPORTE_GD

        const [rows]: any[] = await legacyConn.query(`SELECT
                                                    NULL AS ID_MOVI,
                                                    t.COD_TRAC AS COD_TRANS,
                                                    NULL AS FK_COD_CAJAS_MOVI,
                                                    NULL AS FK_COD_BANCO_MOVI,
                                                     t.METPAG_TRAC AS TIP_MOVI,
                                                   NULL AS periodo_caja,
                                                    t.fecha AS FECHA_MOVI,
                                                    'VENTA' AS ORIGEN_MOVI,
                                                    m.valor_cxp AS IMPOR_MOVI,
                                                    t.METPAG_TRAC AS TIPO_MOVI,
                                                    'CREDITO' AS REF_MOVI,
                                                    CASE 
                                                        WHEN t.TIP_TRAC = 'Fisica' THEN IFNULL(t.NUM_TRACFIC,'')
                                                        WHEN t.TIP_TRAC = 'Electronica' THEN IFNULL(t.NUM_TRAC,'')
                                                        ELSE IFNULL(t.NUM_TRACCI,'')
                                                    END AS CONCEP_MOVIMIG,
                                                    1 AS ESTADO_MOVI,
                                                    'MIG' AS  PER_BENE_MOVI,
                                                    'INGRESO' AS CAUSA_MOVI,
                                                    'VENTAS' AS MODULO,
                                                    t.FEC_TRAC AS FECHA_MANUAL,
                                                    NULL AS CONCILIADO,
                                                    NULL AS FK_COD_CX,
                                                    NULL AS SECU_MOVI,
                                                    NULL AS FK_CONCILIADO,
                                                    NULL AS FK_ANT_MOVI,
                                                    t.FK_COD_USU AS FK_USER_EMP_MOVI,
                                                    t.COD_TRAC AS FK_TRAC_MOVI,NULL AS  NUM_VOUCHER,NULL as NUM_LOTE,
                                                   t.OBS_TRAC as OBS_MOVI, t.TOTPAG_TRAC , NULL AS FK_ASIENTO, NULL AS FK_ARQUEO,NULL AS RECIBO_CAJA, NULL AS NUM_UNIDAD
                                                FROM transacciones t
                                                INNER JOIN cuentascp m 
                                                    ON m.FK_TRAC_CUENTA = t.COD_TRAC
                                                WHERE t.TIP_TRAC IN ('Electronica','Fisica','comprobante-ingreso') and t.METPAG_TRAC='MULTIPLES' ORDER BY t.COD_TRAC ASC;` );

        if (!rows.length) {
            return { mapMovementsSales: mapMovements };
        }

        const idCard = null;


        const [movSec]: any = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO= 'VENTAS' AND  FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let secuenciaMovimiento = movSec[0]?.SECU_MOVI ?? 1;

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {

                let auditId: number | null = null;


                mapObligationsAudit[o.old_id] = auditId;
                o.JSON_PAGOS = '[]';
                const idBanco = null;
                const idDetCaja = null
                const idTrn = mapSales[o.FK_TRAC_MOVI]
                const idUser = userMap[o.FK_USER_EMP_MOVI]
                const idAuditTr = mapAuditSales[o.COD_TRANS];
                const idFkConciliation = mapConciliation[o.FK_CONCILIADO] ?? null;
                const idPlanCuenta = null;
                //o.ID_MOVI,
                insertValues.push([
                    idBanco,//ok
                    idTrn,//ok
                    idFkConciliation,//ok
                    idUser,
                    o.FECHA_MOVI,
                    o.FECHA_MANUAL,
                    o.TIP_MOVI,
                    o.ORIGEN_MOVI,
                    o.TIPO_MOVI,
                    o.REF_MOVI,
                    o.CONCEP_MOVIMIG,
                    o.NUM_VOUCHER,
                    o.NUM_LOTE,
                    o.CAUSA_MOVI,
                    o.MODULO,
                    secuenciaMovimiento,
                    o.IMPOR_MOVI,
                    o.ESTADO_MOVI,
                    o.PER_BENE_MOVI,
                    o.CONCILIADO,
                    newCompanyId,
                    idDetCaja,
                    o.OBS_MOVI,
                    o.TOTPAG_TRAC,
                    o.FK_ASIENTO,
                    idAuditTr,
                    o.FK_ARQUEO,
                    idCard,
                    o.RECIBO_CAJA,
                    idPlanCuenta,
                    o.NUM_UNIDAD,
                    o.JSON_PAGOS
                ]);

                secuenciaMovimiento++;
            }

            const [res]: any = await conn.query(`INSERT INTO movements(
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
                                                VALUES ?`, [insertValues]);

            let newId = res.insertId;
            for (const o of batch) {
                mapMovements[o.COD_TRANS] = newId++;
            }
        }
        console.log("✅ Migración movimiento credito completada correctamente");
        return { mapMovementsSales: mapMovements };
    } catch (err) {
        console.error("❌ Error en migración de obligaciones:", err);
        throw err;
    }
}

export async function migrateSalesAccountingEntries(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapAuditSales: Record<number, number | null>,
    mapMovements: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {
    console.log("🚀 Migrando encabezado de asiento contables");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const [rows]: any[] = await legacyConn.query(`SELECT
                                                        cod_asiento,
                                                        fecha_asiento AS FECHA_ASI,
                                                        descripcion_asiento AS DESCRIP_ASI,
                                                        numero_asiento AS NUM_ASI,
                                                        origen_asiento AS ORG_ASI,
                                                        debe_asiento AS TDEBE_ASI,
                                                        haber_asiento AS THABER_ASI,
                                                        origen_asiento AS TIP_ASI,
                                                        fk_cod_periodo AS FK_PERIODO,
                                                        fecha_registro_asiento AS FECHA_REG,
                                                        fecha_update_asiento AS FECHA_ACT,
                                                        json_asi AS JSON_ASI,
                                                        res_asiento AS RES_ASI,
                                                        ben_asiento AS BEN_ASI,
                                                        NULL AS FK_AUDIT,
                                                        NULL AS FK_COD_EMP,
                                                        CAST(REGEXP_REPLACE(RIGHT(numero_asiento, 9), '[^0-9]', '') AS UNSIGNED)  AS SEC_ASI,
                                                        transacciones.COD_TRAC AS FK_MOVTRAC,
                                                        NULL AS FK_MOV
                                                    FROM
                                                        transacciones
                                                    LEFT JOIN contabilidad_asientos ON contabilidad_asientos.FK_CODTRAC = transacciones.COD_TRAC
                                                    WHERE
                                                        TIP_TRAC IN(
                                                            'Electronica',
                                                            'Fisica',
                                                            'comprobante-ingreso'
                                                        )  AND contabilidad_asientos.descripcion_asiento NOT LIKE '%(RETENCION%'
                                                    ORDER BY
                                                        transacciones.COD_TRAC;` );

        if (!rows.length) {
            return { mapEntryAccount };
        }


        const BATCH_SIZE = 1000;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                const idTrn = mapSales[o.FK_MOVTRAC]
                const periodoId = mapPeriodo[o.FK_PERIODO]
                const idAuditTr = mapAuditSales[o.FK_MOVTRAC];
                const idMovimiento = mapMovements[o.FK_MOVTRAC];

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
                    idTrn,
                    idMovimiento
                ]);


            }

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
        }
        console.log("✅ Migración asiento contable completada correctamente");
        return { mapEntryAccount };
    } catch (err) {
        console.error("❌ Error en migración de asiento contable:", err);
        throw err;
    }
}


export async function migrateSalesAccountingEntriesDetail(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {

    console.log("🚀 Iniciando migración de detalles de asientos contables");

    const mapAccountDetail: Record<number, number> = {};

    const [rows]: any[] = await legacyConn.query(`
        SELECT 
            d.cod_detalle_asiento,
            a.fecha_asiento,
            a.cod_asiento AS FK_COD_ASIENTO,
            d.debe_detalle_asiento AS DEBE_DET,
            d.haber_detalle_asiento AS HABER_DET,
            d.fk_cod_plan AS FK_CTAC_PLAN,
            d.fkProyectoCosto AS FK_COD_PROJECT,
            d.fkCentroCosto AS FK_COD_COST
        FROM transacciones t
        INNER JOIN contabilidad_asientos a ON a.FK_CODTRAC = t.COD_TRAC
        INNER JOIN contabilidad_detalle_asiento d ON d.fk_cod_asiento = a.cod_asiento
        WHERE t.TIP_TRAC IN ('Electronica','Fisica','comprobante-ingreso')
          AND a.descripcion_asiento NOT LIKE '%(RETENCION%'
        ORDER BY t.COD_TRAC
    `);

    if (!rows.length) {
        console.log("⚠️ No hay registros para migrar");
        return { mapAccountDetail };
    }

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
                console.log(`➡️ Procesando detalle de asiento  ${idPlan}`);
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
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

                if (!idPlan || !idCodAsiento) continue;

                mapAccountDetail[o.cod_detalle_asiento] = newId++;
            }

            for (const t of totalsMap.values()) {
                await upsertTotaledEntry(conn, t, newCompanyId);
            }

            console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado`);

        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }

    console.log("🎉 Migración de detalles completada");
    return { mapAccountDetail };
}


