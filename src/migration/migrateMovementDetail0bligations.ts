import { upsertTotaledEntry } from "./migrationTools";

export async function migrateMovementDetail0bligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    userMap: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
    mapObligationsCustomers: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
): Promise<{
    mapMovements: Record<number, number>,
    mapAuditMovements: Record<number, number>
}> {
    console.log("🚀 Migrando movimientos y COBRI obligaciones");

    const mapMovements: Record<number, number> = {};
    const mapAuditMovements: Record<number, number> = {};
    const mapAcountsObligations: Record<number, number> = {};

    try {
        // 1. Obtener secuencias iniciales
        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit FROM audit WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const [[{ nextSecu }]]: any = await conn.query(
            `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS nextSecu FROM movements WHERE MODULO = 'CXC' AND FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const [[{ idCard }]]: any = await conn.query(
            `SELECT ID_TARJETA AS idCard FROM cards WHERE FK_COD_EMP = ? LIMIT 1`,
            [newCompanyId]
        );

        let auditSeq = nextAudit;
        let secuenciaMovimiento = nextSecu;

        // 2. Pre-cargar Account Plan para evitar consultas en el loop (Cache)
        const [accountRows]: any = await conn.query(
            `SELECT ID_PLAN, CODIGO_PLAN FROM account_plan WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const accountMap = new Map(accountRows.map((a: any) => [a.CODIGO_PLAN, a.ID_PLAN]));

        // 3. Traer datos legados (Usando la query optimizada anteriormente)
        const [rows]: any[] = await legacyConn.query(`SELECT
                                                    detalles_cuentas.fk_cod_cuenta,
                                                    detalles_cuentas.FK_COD_GD,
                                                    detalles_cuentas.fk_cod_cli_c,
                                                    detalles_cuentas.Tipo_cp,
                                                    detalles_cuentas.fecha,
                                                    detalles_cuentas.importe,
                                                    detalles_cuentas.saldo,
                                                    IFNULL(movimientos.IMPOR_MOVI,detalles_cuentas.importe) AS IMPOR_MOVI,
                                                    grupo_detalles_t.IMPORTE_GD AS  IMPOR_MOVITOTAL,
                                                    detalles_cuentas.saldo,
                                                    CASE detalles_cuentas.forma_pago_cp 
                                                    WHEN 1  THEN 'EFECTIVO'
                                                    WHEN 2  THEN 'CHEQUE'
                                                    WHEN 3  THEN 'TRANSFERENCIA'
                                                    WHEN 5  THEN 'TARJETA'
                                                    WHEN 7  THEN 'ANTICIPO'
                                                    WHEN 8  THEN 'CCTACONT'
                                                    WHEN 16 THEN 'RET-VENTA'
                                                    WHEN 17 THEN 'NOTA DE CREDITO'
                                                    ELSE CAST(detalles_cuentas.forma_pago_cp AS CHAR)
                                                END AS forma,
                                                IFNULL(movimientos.PER_BENE_MOVI, 'MIG') AS PER_BENE_MOVI,
                                                detalles_cuentas.fk_cod_cajas,
                                                detalles_cuentas.fk_cod_banco,
                                                detalles_cuentas.fk_cod_Vemp,
                                                detalles_cuentas.NUM_VOUCHER,
                                                detalles_cuentas.NUM_LOTE,
                                                movimientos.ORIGEN_MOVI,
                                                IFNULL(movimientos.FECHA_MOVI, detalles_cuentas.FECH_REG) AS FECHA_MOVI,
                                                IFNULL(movimientos.REF_MOVI, detalles_cuentas.documento_cp) AS REF_MOVI,
                                                IFNULL(movimientos.FECHA_MANUAL,detalles_cuentas.fecha) AS FECHA_MANUAL,
                                                movimientos.FK_CONCILIADO,
                                                movimientos.CONCILIADO,
                                                movimientos.ID_MOVI,
                                                CASE WHEN  movimientos.ESTADO_MOVI='ACTIVO' THEN 1 ELSE 0 END AS ESTADO_MOVI, 
                                                IFNULL(movimientos.CAUSA_MOVI,'INGRESO') AS CAUSA_MOVI,
                                                movimientos.TIP_MOVI,
                                                movimientos.TIPO_MOVI,
                                                NULL AS FK_ASIENTO,
                                                NULL AS FK_ARQUEO,
                                                NULL AS RECIBO_CAJA,
                                                NULL AS NUM_UNIDAD,
                                                NULL AS JSON_PAGOS,
                                                'CXC' as MODULO,
                                                grupo_detalles_t.FECH_REG,
                                                IFNULL(
                                                    movimientos.CONCEP_MOVI,
                                                    detalles_cuentas.observacion_cp
                                                ) AS OBS_MOVI,
                                                IFNULL(
                                                    movimientos.CONCEP_MOVI,
                                                    detalles_cuentas.observacion_cp
                                                ) AS CONCEP_MOVI, grupo_detalles_t.SECU_CXC AS SECU_MOVI, movimientos.FK_COD_CAJAS_MOVI, movimientos.FK_COD_BANCO_MOVI
                                                FROM
                                                    cuentascp
                                                INNER JOIN detalles_cuentas ON cuentascp.cod_cp = detalles_cuentas.fk_cod_cuenta
                                                INNER JOIN grupo_detalles_t ON detalles_cuentas.FK_COD_GD = grupo_detalles_t.ID_GD
                                                LEFT JOIN movimientos ON movimientos.FK_COD_CX = grupo_detalles_t.ID_GD
                                                WHERE
                                                    cuentascp.Tipo_cxp = 'CXC' AND forma_pago_cp NOT IN ('17','16')
                                                GROUP BY
                                                    detalles_cuentas.FK_COD_GD
                                                ORDER BY
                                                    cod_detalle
                                                DESC;`);
        if (!rows.length) return { mapMovements, mapAuditMovements };

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);

            // --- PASO A: INSERTAR AUDITORÍA EN BATCH ---
            // Solo creamos auditoría si hay datos válidos
            const auditValues = batch.map(o => [auditSeq++, o.forma, newCompanyId]);
            const [resAudit]: any = await conn.query(
                `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
                [auditValues]
            );
            const firstAuditId = resAudit.insertId;

            // --- PASO B: PREPARAR MOVIMIENTOS ---
            const movementValues = batch.map((o, index) => {
                const currentAuditId = firstAuditId + index;
                mapAuditMovements[o.FK_COD_GD] = currentAuditId; // Guardar mapeo

                // Lógica de Negocio
                let modulo = 'CXC';
                let origen = 'CXC';
                let causa = 'INGRESO';
                let tipMovi = o.TIP_MOVI;
                let tipoMovi = o.TIPO_MOVI;
                let idPlanCuenta = null;

                if (o.forma === 'CCTACONT') {
                    const codigoPlan = o.REF_MOVI?.split('--')[0].trim();
                    idPlanCuenta = accountMap.get(codigoPlan) || null;
                    tipMovi = tipoMovi = 'CCTACONT';
                } else if (o.forma === 'RET-VENTA') {
                    tipMovi = tipoMovi = origen = 'RET-VENTA';
                    modulo = 'RETENCION-VENTA';
                } else if (o.forma === 'NOTA DE CREDITO') {
                    tipMovi = tipoMovi = 'CREDITO';
                    modulo = 'NCVENTA';
                    origen = 'NOTA CREDITO VENTA';
                }

                return [
                    bankMap[o.FK_COD_BANCO_MOVI] ?? null,
                    mapSales[o.FK_TRAC_MOVI] ?? null,
                    mapConciliation[o.FK_CONCILIADO] ?? null,
                    userMap[o.fk_cod_Vemp] ?? null,
                    o.FECHA_MOVI,
                    o.FECHA_MANUAL,
                    tipMovi,
                    origen,
                    tipoMovi,
                    o.REF_MOVI,
                    o.CONCEP_MOVI,
                    o.NUM_VOUCHER,
                    o.NUM_LOTE,
                    causa,
                    modulo,
                    secuenciaMovimiento++,
                    o.IMPOR_MOVI,
                    o.ESTADO_MOVI,
                    o.PER_BENE_MOVI,
                    o.CONCILIATED ?? null,
                    newCompanyId,
                    boxMap[o.FK_COD_CAJAS_MOVI] ?? null,
                    o.OBS_MOVI,
                    o.IMPOR_MOVITOTAL,
                    null, // FK_ASIENTO
                    currentAuditId,
                    null, // FK_ARQUEO
                    (o.forma === 'TARJETA') ? idCard : null,
                    null, // RECIBO_CAJA
                    idPlanCuenta,
                    null, // NUM_UNIDAD
                    '[]'  // JSON_PAGOS
                ];
            });

            // --- PASO C: INSERTAR MOVIMIENTOS EN BATCH ---
            const [resMov]: any = await conn.query(
                `INSERT INTO movements (
                    FKBANCO, FK_COD_TRAN, FK_CONCILIADO, FK_USER, FECHA_MOVI, FECHA_MANUAL,
                    TIP_MOVI, ORIGEN_MOVI, TIPO_MOVI, REF_MOVI, CONCEP_MOVI, NUM_VOUCHER,
                    NUM_LOTE, CAUSA_MOVI, MODULO, SECU_MOVI, IMPOR_MOVI, ESTADO_MOVI,
                    PER_BENE_MOVI, CONCILIADO, FK_COD_EMP, IDDET_BOX, OBS_MOVI,
                    IMPOR_MOVITOTAL, FK_ASIENTO, FK_AUDITMV, FK_ARQUEO, ID_TARJETA,
                    RECIBO_CAJA, FK_CTAM_PLAN, NUMERO_UNIDAD, JSON_PAGOS
                ) VALUES ?`,
                [movementValues]
            );

            // Actualizar mapeo de movimientos
            let currentMovId = resMov.insertId;
            batch.forEach(o => {
                mapMovements[o.FK_COD_GD] = currentMovId++;
            });
        }
        const mapDetailObligationsAplicate = await migratePaymentDetails(
            legacyConn,
            conn,
            mapObligationsCustomers,
            mapMovements
        );
        console.log("Movimientos de cobros realizados:", Object.keys(mapMovements).length);
        console.log("Detalle migrado:", Object.keys(mapDetailObligationsAplicate).length);
        console.log("✅ Migración de cobros completadas");



        const mapEntryAccount = await migrateAccountingEntriesCustomerObligations(
            legacyConn,
            conn,
            newCompanyId,
            mapMovements,
            mapPeriodo,
            mapAuditMovements,
        );

        console.log("Encabezado de asiento contable migrados:", Object.keys(mapEntryAccount).length);


        const mapDetailAsiento = await migrateDetailedAccountingEntriesCustomerObligations(
            legacyConn,
            conn,
            newCompanyId,
            mapProject,
            mapCenterCost,
            mapAccounts,
            mapEntryAccount.mapEntryAccount
        )

        console.log("Detalle de asientos contables migrados:", Object.keys(mapDetailAsiento).length);

        return { mapMovements, mapAuditMovements };

    } catch (err) {
        console.error("❌ Error:", err);
        throw err;
    }
}


export async function migratePaymentDetails(
    legacyConn: any,
    conn: any,
    mapObligationsCustomers: Record<number, number | null>,
    mapMovements: Record<number, number | null>,
): Promise<{

    mapDetailObligationsAplicate: Record<number, number>
}> {
    console.log("🚀 Migrando movimientos y COBRI obligaciones");


    const mapDetailObligationsAplicate: Record<number, number> = {};

    try {

        const [rows]: any[] = await legacyConn.query(`SELECT
                                                            detalles_cuentas.fk_cod_cuenta,
                                                            detalles_cuentas.FK_COD_GD,
                                                            detalles_cuentas.fecha,
                                                            detalles_cuentas.importe,
                                                            detalles_cuentas.saldo,
                                                            detalles_cuentas.nuevo_saldo
                                                        FROM
                                                            cuentascp
                                                        INNER JOIN detalles_cuentas ON cuentascp.cod_cp = detalles_cuentas.fk_cod_cuenta
                                                        INNER JOIN grupo_detalles_t ON detalles_cuentas.FK_COD_GD = grupo_detalles_t.ID_GD
                                                        LEFT JOIN movimientos ON movimientos.FK_COD_CX = grupo_detalles_t.ID_GD
                                                        WHERE
                                                            cuentascp.Tipo_cxp = 'CXC' AND detalles_cuentas.forma_pago_cp NOT IN ('17','16')
                                                        ORDER BY
                                                            cod_detalle
                                                        DESC;`);


        if (!rows.length) return { mapDetailObligationsAplicate };

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const movementValues = batch.map((o, index) => {
                const idCuenta = mapObligationsCustomers[o.fk_cod_cuenta];
                const idMovimiento = mapMovements[o.FK_COD_GD];

                return [
                    idCuenta,
                    idMovimiento,
                    o.fecha,
                    o.importe,
                    o.saldo,
                    o.nuevo_saldo
                ];
            });

            const [resMov]: any = await conn.query(
                `INSERT INTO account_detail (
                    FK_COD_CUENTA, FK_ID_MOVI, FECHA_REG, IMPORTE, SALDO, NEW_SALDO
                ) VALUES ?`,
                [movementValues]
            );

            let currentMovId = resMov.insertId;
            batch.forEach(o => {
                mapDetailObligationsAplicate[o.fk_cod_cuenta] = currentMovId++;
            });
        }
        console.log("✅ Migración completada");
        return { mapDetailObligationsAplicate };

    } catch (err) {
        console.error("❌ Error:", err);
        throw err;
    }
}

//MIGRAR ASIENTOS DE COBROS DE OBLIGACIONES
export async function migrateAccountingEntriesCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapMovements: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapAuditMovements: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {
    console.log("🚀 Migrando encabezado de asiento contables cobros obligaciones..........");
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
                                                        cod_origen,
                                                        NULL AS FK_MOV
                                                    FROM contabilidad_asientos 
                                                    WHERE
                                                        origen_asiento = 'CXC' AND tipo_asiento <> 'OBLIGACIONES';` );

        if (!rows.length) {
            return { mapEntryAccount };
        }


        const BATCH_SIZE = 1000;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                /*  const idTrn = mapSales[o.FK_MOVTRAC] */
                const periodoId = mapPeriodo[o.FK_PERIODO]
                const idAuditTr = mapAuditMovements[o.cod_origen];
                const idMovimiento = mapMovements[o.cod_origen];

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
        }
        console.log("✅ Migración asiento contable completada correctamente");
        return { mapEntryAccount };
    } catch (err) {
        console.error("❌ Error en migración de asiento contable:", err);
        throw err;
    }
}
//MIGRAR ASIENTOS CONTABLES 
export async function migrateDetailedAccountingEntriesCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {

    console.log("🚀 Iniciando migración de detalles de asientos contables cobros..........");

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
        FROM  contabilidad_asientos a 
        INNER JOIN contabilidad_detalle_asiento d ON d.fk_cod_asiento = a.cod_asiento
        WHERE  origen_asiento = 'CXC' AND tipo_asiento <> 'OBLIGACIONES';
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
                console.log(`➡️ Procesando detalle de asiento  ${idPlan}`);
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
