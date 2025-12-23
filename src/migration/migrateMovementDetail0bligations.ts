export async function migrateMovementsObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapObligationsCustomers: Record<number, number | null>,
    mapObligationsAudit: Record<number, number | null>,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    userMap: Record<number, number | null>
): Promise<{
    mapObligationsCustomers: Record<number, number>,
    mapObligationsAudit: Record<number, number>
}> {

    console.log("🚀 Migrando obligaciones clientes");

    const mapMovements: Record<number, number> = {};
    const mapAuditMovements: Record<number, number> = {};

    try {//IMPORTE_GD

        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit
             FROM audit
             WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let auditSeq: number = nextAudit;

        const [rows]: any[] = await legacyConn.query(`
                                                    SELECT
                                                    detalles_cuentas.fk_cod_cuenta,
                                                    detalles_cuentas.FK_COD_GD,
                                                    detalles_cuentas.fk_cod_cli_c,
                                                    detalles_cuentas.Tipo_cp,
                                                    detalles_cuentas.fecha,
                                                    detalles_cuentas.importe,
                                                    detalles_cuentas.saldo,
                                                    grupo_detalles_t.IMPORTE_GD AS IMPOR_MOVI,
                                                    detalles_cuentas.saldo,
                                                    CASE WHEN detalles_cuentas.forma_pago_cp = 16 THEN 'RET-VENTA' 
                                                    WHEN detalles_cuentas.forma_pago_cp = 17 THEN 'NOTA DE CREDITO' 
                                                    WHEN detalles_cuentas.forma_pago_cp = 5 THEN 'TARJETA' 
                                                    WHEN detalles_cuentas.forma_pago_cp = 7 THEN 'ANTICIPO' 
                                                    WHEN detalles_cuentas.forma_pago_cp = 3 THEN 'TRANSFERENCIA' 
                                                    WHEN detalles_cuentas.forma_pago_cp = 1 THEN 'EFECTIVO' 
                                                    WHEN detalles_cuentas.forma_pago_cp = 2 THEN 'CHEQUE' 
                                                    WHEN detalles_cuentas.forma_pago_cp = 8 THEN 'CCTACONT' ELSE detalles_cuentas.forma_pago_cp
                                                END AS forma,
                                                movimientos.PER_BENE_MOVI,
                                                detalles_cuentas.fk_cod_cajas,
                                                detalles_cuentas.fk_cod_banco,
                                                detalles_cuentas.fk_cod_Vemp,
                                                detalles_cuentas.NUM_VOUCHER,
                                                detalles_cuentas.NUM_LOTE,
                                                movimientos.ORIGEN_MOVI,
                                                IFNULL(movimientos.REF_MOVI, detalles_cuentas.documento_cp) AS REF_MOVI,
                                                movimientos.FECHA_MANUAL,
                                                movimientos.FK_CONCILIADO,
                                                movimientos.CONCILIADO,
                                                movimientos.ID_MOVI,
                                                movimientos.CAUSA_MOVI,
                                                movimientos.TIP_MOVI,
                                                movimientos.TIPO_MOVI,
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
                                                    cuentascp.Tipo_cxp = 'CXC'
                                                GROUP BY
                                                    detalles_cuentas.FK_COD_GD
                                                ORDER BY
                                                    cod_detalle
                                                DESC ;
        `);

        if (!rows.length) {
            return { mapObligationsCustomers, mapObligationsAudit };
        }


        const [cardGeneric]: any = await conn.query(
            `SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        const [movSec]: any = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO= 'VENTAS' AND  FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let secuenciaMovimiento = movSec[0]?.SECU_MOVI ?? 1;

        const idCard = cardGeneric[0]?.ID_TARJETA ?? null;
        const oblAsiToAuditId: Record<number, number> = {};
        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {

                let auditId: number | null = null;


                /*  mapObligationsAudit[o.old_id] = auditId; */


                if (o.obl_asi_group !== null) {
                    if (!oblAsiToAuditId[o.FK_COD_GD]) {
                        const codigoAut = auditSeq++;

                        const [resAudit]: any = await conn.query(
                            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP)
                             VALUES (?, 'VENTAS', ?)`,
                            [codigoAut, newCompanyId]
                        );
                        oblAsiToAuditId[o.FK_COD_GD] = resAudit.insertId;
                    }
                    auditId = oblAsiToAuditId[o.FK_COD_GD];
                }
            

                o.MODULO = 'CXC';
                o.ORIGEN_MOVI = 'CXC';
                o.CAUSA_MOVI = 'INGRESO';
                o.ID_TARJETA = null;
                let idPlanCuenta = null;
                if (o.forma == 'CCTACONT') {
                    o.TIP_MOVI = 'CCTACONT';
                    o.TIPO_MOVI = 'CCTACONT';
                    const codigoPlan = o.REF_MOVI.split('--')[0].trim();
                    const queryUsuser = "SELECT *FROM account_plan  WHERE account_plan.CODIGO_PLAN=? AND account_plan.FK_COD_EMP =? limit 1";
                    const [account_plan] = await conn.query(queryUsuser, [codigoPlan, newCompanyId]);
                    if (!account_plan.length) {
                        idPlanCuenta = null;
                    } else {
                        idPlanCuenta = account_plan[0].ID_PLAN;
                    }
                }

                if (o.forma == 'RET-VENTA') {
                    o.TIP_MOVI = 'RET-VENTA';
                    o.TIPO_MOVI = 'RET-VENTA';
                    o.MODULO = 'RETENCION-VENTA';
                    o.ORIGEN_MOVI = 'RET-VENTA';

                }

                if (o.forma == 'NOTA DE CREDITO') {
                    o.TIP_MOVI = 'CREDITO';
                    o.TIPO_MOVI = 'CREDITO';
                    o.MODULO = 'NCVENTA';
                    o.ORIGEN_MOVI = 'NOTA CREDITO VENTA';
                }
                o.ID_TARJETA = null;
                if (o.forma == 'TARJETA') {
                    o.ID_TARJETA = idCard;
                }

                o.JSON_PAGOS = '[]';


                mapObligationsAudit[o.old_id] = auditId;
                o.JSON_PAGOS = '[]';
                const idBanco = bankMap[o.FK_COD_BANCO_MOVI];
                const idDetCaja = boxMap[o.FK_COD_CAJAS_MOVI]
                const idTrn = mapSales[o.FK_TRAC_MOVI]
                const idUser = userMap[o.fk_cod_Vemp]
            
                //o.ID_MOVI,
                insertValues.push([
                    idBanco,//ok
                    idTrn,//ok
                    o.FK_CONCILIADO,//ok
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
                    auditId,
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
        return { mapObligationsCustomers, mapObligationsAudit };
    } catch (err) {
        console.error("❌ Error en migración de obligaciones:", err);
        throw err;
    }
}