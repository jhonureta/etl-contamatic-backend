export async function migrateMovementsObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapObligationsCustomers: Record<number, number | null>,
    mapObligationsAudit: Record<number, number | null>,
    bankMap,
    boxMap
): Promise<{
    mapObligationsCustomers: Record<number, number>,
    mapObligationsAudit: Record<number, number>
}> {

    console.log("🚀 Migrando obligaciones clientes");

    const mapMovements: Record<number, number> = {};
    const mapAuditMovements: Record<number, number> = {};

    try {//IMPORTE_GD

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
movimientos.REF_MOVI,
movimientos.FECHA_MANUAL,
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
DESC
    ;
        `);

        if (!rows.length) {
            return { mapObligationsCustomers, mapObligationsAudit };
        }


        const [cardGeneric]: any = await conn.query(
            `SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        const idCard = cardGeneric[0]?.ID_TARJETA ?? null;

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {

                let auditId: number | null = null;


                mapObligationsAudit[o.old_id] = auditId;

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
                const idBanco = bankMap[o.FK_COD_BANCO_MOVI];
                const idDetCaja = boxMap[o.FK_COD_CAJAS_MOVI]
                //o.ID_MOVI,
                insertValues.push([
                    idBanco, //ok
                    o.FK_COD_TRAN, //OK
                    o.FK_CONCILIADO,  //OK
                    o.FK_USER, //OK
                    o.FECHA_MOVI, //OK
                    o.FECHA_MANUAL, //OK
                    o.TIP_MOVI,//OK
                    o.ORIGEN_MOVI,//ok
                    o.TIPO_MOVI,//OK
                    o.REF_MOVI,//ok
                    o.CONCEP_MOVI,//ok
                    o.NUM_VOUCHER,//OK
                    o.NUM_LOTE,//OK
                    o.CAUSA_MOVI,//OK
                    o.MODULO,//OK
                    o.SECU_MOVI,//OK
                    o.IMPOR_MOVI, //OK
                    o.ESTADO_MOVI,
                    o.PER_BENE_MOVI,
                    o.CONCILIADO,  //OK
                    newCompanyId,  //OK
                    idDetCaja,
                    o.OBS_MOVI, //OK
                    o.IMPOR_MOVI, //OK
                    o.FK_ASIENTO,//OK
                    o.FK_AUDITMV,
                    o.FK_ARQUEO,//OK
                    o.ID_TARJETA,//OK
                    o.RECIBO_CAJA,//OK
                    idPlanCuenta,//ok
                    o.NUMERO_UNIDAD,//OK
                    o.JSON_PAGOS//OK
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