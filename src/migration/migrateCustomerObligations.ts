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
        mapObligationsAudit
    } = await migrateCustomerObligations(
        legacyConn,
        conn,
        newCompanyId,
        mapSales,
        mapAuditSales,
        mapClients
    );

    console.log(
        `✔ Obligaciones migradas: ${Object.keys(mapObligationsCustomers).length}`
    );

    console.log(
        `✔ Auditoria migradas: ${Object.keys(mapObligationsAudit).length}`
    );


    /*  DETALLE DE MOVIMIENTOS EXENTAS DE CREDITO */
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



}



export async function migrateCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapAuditSales: Record<number, number | null>,
    mapClients: Record<number, number | null>
): Promise<{
    mapObligationsCustomers: Record<number, number>,
    mapObligationsAudit: Record<number, number>
}> {

    console.log("🚀 Migrando obligaciones clientes");

    const mapObligationsCustomers: Record<number, number> = {};
    const mapObligationsAudit: Record<number, number> = {};

    try {

        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit
             FROM audit
             WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let auditSeq: number = nextAudit;

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
                TIPO_ESTADO_CUENTA AS tipo_estado_cuenta
            FROM cuentascp
            WHERE Tipo_cxp = 'CXC'
            ORDER BY cod_cp
        `);

        if (!rows.length) {
            return { mapObligationsCustomers, mapObligationsAudit };
        }

        const oblAsiToAuditId: Record<number, number> = {};
        let importadoAuditId: number | null = null;

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {

                let auditId: number | null = null;


                if (o.fk_cod_trans_old && mapAuditSales[o.fk_cod_trans_old]) {
                    auditId = mapAuditSales[o.fk_cod_trans_old]!;
                }


                else if (o.obl_asi_group !== null) {
                    if (!oblAsiToAuditId[o.obl_asi_group]) {
                        const codigoAut = auditSeq++;

                        const [resAudit]: any = await conn.query(
                            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP)
                             VALUES (?, 'VENTAS', ?)`,
                            [codigoAut, newCompanyId]
                        );

                        oblAsiToAuditId[o.obl_asi_group] = resAudit.insertId;
                    }

                    auditId = oblAsiToAuditId[o.obl_asi_group];
                }

                else if (o.tipo_cuenta === 'Importado') {
                    if (!importadoAuditId) {
                        const codigoAut = auditSeq++;

                        const [resAudit]: any = await conn.query(
                            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP)
                             VALUES (?, 'VENTAS', ?)`,
                            [codigoAut, newCompanyId]
                        );

                        importadoAuditId = resAudit.insertId;
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

                insertValues.push([
                    customerId,
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
                ]);
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
        console.log("✅ Migración completada correctamente");
        return { mapObligationsCustomers, mapObligationsAudit };
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
                const idProyecto = mapProject[o.FK_COD_PROJECT] ?? null;
                const idCentroCosto = mapCenterCost[o.FK_COD_COST] ?? null;
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

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
                        total: 0
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
            console.log(totalHaber);
            console.log(totalDebe);


        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }

    console.log("🎉 Migración de detalles completada");
    return { mapAccountDetail };
}

async function upsertTotaledEntry(
    conn: any,
    data: {
        id_plan: number;
        fecha: string;
        debe: number;
        haber: number;
        total: number;
    },
    companyId: number
) {
    await conn.query(`
        INSERT INTO totaledentries (
            ID_FKPLAN, FECHA_ENTRY, TOTAL_DEBE, TOTAL_HABER, TOTAL_NUMASI, FK_COD_EMP
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            TOTAL_DEBE = TOTAL_DEBE + VALUES(TOTAL_DEBE),
            TOTAL_HABER = TOTAL_HABER + VALUES(TOTAL_HABER),
            TOTAL_NUMASI = TOTAL_NUMASI + VALUES(TOTAL_NUMASI)
    `, [
        data.id_plan,
        data.fecha,
        data.debe,
        data.haber,
        data.total,
        companyId
    ]);
}

