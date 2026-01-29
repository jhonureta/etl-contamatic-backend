import { upsertTotaledEntry } from "./migrationTools";

export async function migrateBankCashTransactions(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapConciliation: Record<number, number>,
    userMap: Record<number, number>,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapPeriodo: Record<number, number>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
): Promise<{ movementCashBanck: Record<number, number> }> {
    try {

        console.log(`Iniciando migracion de movimientos cajas-bancos...`);


        const movementCashBanck: Record<number, number> = {};
        const movementAuditCashBanck: Record<number, number> = {};

        const [movements] = await legacyConn.query(`SELECT
    CASE WHEN FK_COD_CAJAS_MOVI>0 THEN FK_COD_CAJAS_MOVI ELSE NULL END AS FK_COD_CAJAS_MOVI,
    CASE WHEN FK_COD_BANCO_MOVI>0 THEN FK_COD_BANCO_MOVI ELSE NULL END AS FK_COD_BANCO_MOVI,
    TIP_MOVI,
    periodo_caja,
    FECHA_MOVI AS FECHA_MOVI,
    CASE WHEN ORIGEN_MOVI='CAJA' THEN 'CAJAS'
    WHEN ORIGEN_MOVI='BANCO' THEN 'BANCOS' ELSE NULL END AS ORIGEN_MOVI,
    IMPOR_MOVI,
    TIPO_MOVI,
    REF_MOVI,
    REF_MOVI as CONCEP_MOVI,
    SALDO_CAJA_MOVI,
    SALDO_BANCO_MOVI,
    ESTADO_MOVI,
    PER_BENE_MOVI,
    CAUSA_MOVI,
    MODULO,
    FECHA_MANUAL AS FECHA_MANUAL,
    CONCILIADO,
    FK_COD_CX,
    SECU_MOVI,
    FK_CONCILIADO,
    FK_ANT_MOVI,
    FK_USER_EMP_MOVI AS FK_USER,
    FK_TRAC_MOVI AS FK_COD_TRAN,
    FK_COD_RH,
    FK_DET_PREST,
    COD_AUDIT,
    FK_COD_TARJETA,
    NUM_UNIDAD AS NUMERO_UNIDAD,
    RECIBO_CAJA,
    NULL AS FK_COD_EMP,
    NULL AS NUM_VOUCHER,
    NULL AS NUM_LOTE,
    IMPOR_MOVI AS IMPOR_MOVITOTAL,
    NULL AS IDDET_BOX,
    CONCEP_MOVI AS OBS_MOVI,
    NULL AS FK_ASIENTO,
    NULL AS FK_AUDITMV,
    NULL AS FK_ARQUEO,
    NULL AS ID_TARJETA,
    NULL AS FK_CTAM_PLAN,
    NULL AS JSON_PAGOS
FROM
    movimientos
WHERE
    MODULO IN(
        'MOVIMIENTO',
        'DEPOSITO',
        'TRANSFERENCIA'
    );`);
        /*  const [movements] = rows; */
        if (movements.length === 0) {
            return { movementCashBanck };
        } /* console.log(movements); */
        console.log(`Movimientos de cajas y bancos... ${movements.length}`);

        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit FROM audit WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );
        let auditSeq = nextAudit;
        const BATCH_SIZE = 1000;
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
                const userId = userMap[m.FK_USER];
                const currentAuditId = firstAuditId + index;
                movementAuditCashBanck[`${m.MODULO}-${m.CAUSA_MOVI}-${m.SECU_MOVI}`] = currentAuditId;
                const idFkConciliation = mapConciliation[m.FK_CONCILIADO] ?? null;
                let idPlanCuenta = null;

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
                    m.SECU_MOVI,
                    m.IMPOR_MOVI,
                    m.ESTADO_MOVI,
                    m.PER_BENE_MOVI,
                    m.CONCILIADO,
                    newCompanyId,
                    idBoxDetail,
                    m.OBS_MOVI,
                    m.TOTPAG_TRAC,
                    m.FK_ASIENTO,
                    currentAuditId,
                    m.FK_ARQUEO,
                    null,
                    m.RECIBO_CAJA,
                    idPlanCuenta,
                    m.NUM_UNIDAD,
                    m.JSON_PAGOS
                ];

            });

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
                movementCashBanck[`${o.MODULO}-${o.CAUSA_MOVI}-${o.SECU_MOVI}`] = currentMovId++;
            });

            console.log(` -> Batch migrado: ${batchMovements.length} cajas y bancos`);
        }


        console.log("✅ Migración de movimientos anticipos clientes");

        const mapEntryAccount = await migrateBankCashTransactionsAccountingEntries(
            legacyConn,
            conn,
            newCompanyId,
            movementCashBanck,
            mapPeriodo,
            movementAuditCashBanck
        );

        const mapEntryDetailAccount = await migrateDetailedAccountingEntries(
            legacyConn,
            conn,
            newCompanyId,
            mapProject,
            mapCenterCost,
            mapAccounts,
            mapEntryAccount.mapEntryAccount
        )

        return { movementCashBanck };
    } catch (error) {
        throw error;
    }
}


export async function migrateBankCashTransactionsAccountingEntries(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    movementCashBanck: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    movementAuditCashBanck: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {


    console.log("🚀 Migrando encabezado de asiento contables cajas y bancos..........");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const [rows]: any[] = await legacyConn.query(`SELECT
   cod_asiento,
    fecha_asiento AS FECHA_ASI,
    descripcion_asiento AS DESCRIP_ASI,
    numero_asiento AS NUM_ASI,

 CASE WHEN contabilidad_asientos.origen_asiento='CAJA' THEN 'CAJAS'
    WHEN contabilidad_asientos.origen_asiento='BANCO' THEN 'BANCOS' ELSE NULL END AS ORG_ASI,


    debe_asiento AS TDEBE_ASI,
    haber_asiento AS THABER_ASI,
    numero_asiento,
    contabilidad_asientos.tipo_asiento AS TIP_ASI_VER,
    REGEXP_REPLACE(
    contabilidad_asientos.tipo_asiento,
    '-(INGRESO|EGRESO)$',
    ''
    ) AS TIP_ASI,

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
    NULL AS FK_MOV, contabilidad_asientos.cod_origen AS FK_ANTDET 
FROM
    contabilidad_asientos
WHERE
    tipo_asiento IN(
        'DEPOSITO-INGRESO',
        'DEPOSITO-EGRESO',
        'MOVIMIENTO-INGRESO',
        'MOVIMIENTO-EGRESO',
        'TRANSFERENCIA-INGRESO',
        'TRANSFERENCIA-EGRESO'
    )
ORDER BY
    cod_asiento
DESC;`);

        if (!rows.length) {
            return { mapEntryAccount };
        }


        const BATCH_SIZE = 1000;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                const periodoId = mapPeriodo[o.FK_PERIODO]
                const idAuditTr = movementAuditCashBanck[`${o.TIP_ASI_VER}-${o.FK_ANTDET}`];
                const idMovimiento = movementCashBanck[`${o.TIP_ASI_VER}-${o.FK_ANTDET}`];

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
             console.log(` -> Batch migrado: ${batch.length} caja-bancos`);
        }
        console.log("✅ Migración asiento contable gestion caja-bancos correctamente");
        return { mapEntryAccount };
    } catch (err) {
        console.error("❌ Error en migración de asiento contable:", err);
        throw err;
    }
}

export async function migrateDetailedAccountingEntries(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {
    console.log("🚀 Cuentas contables");
    console.log("🚀 Iniciando migración de detalles de asientos contables cajas y bancos..........");

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
                tipo_asiento IN(
        'DEPOSITO-INGRESO',
        'DEPOSITO-EGRESO',
        'MOVIMIENTO-INGRESO',
        'MOVIMIENTO-EGRESO',
        'TRANSFERENCIA-INGRESO',
        'TRANSFERENCIA-EGRESO')
            ORDER BY
                cod_asiento
            DESC;
        `);

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
                console.log(`➡️ Procesando detalle de asiento caja y bancos ${idPlan}`);
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];
                if (!idPlan || !idCodAsiento) continue;
                mapAccountDetail[o.cod_detalle_asiento] = newId++;
            }
            for (const t of totalsMap.values()) {
                await upsertTotaledEntry(conn, t, newCompanyId);
            }
            console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado caja y bancos`);
        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }
    console.log("🎉 Migración  detalles contables completada caja y bancos");
    return { mapAccountDetail };
}