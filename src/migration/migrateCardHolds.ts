import { upsertTotaledEntry } from "./migrationTools";
import { RetentionCodeValue } from "./purchaseHelpers";

export async function migrateDataMovementsRetentionsHolds(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapConciliation: Record<number, number>,
    userMap: Record<number, number>,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapPeriodo: Record<number, number>,
    mapProject: Record<number, number>,
    mapCenterCost: Record<number, number>,
    mapAccounts: Record<number, number>,
    mapRetentions: Record<number, number>,
    mapWithholdingBanks: Record<string, number>,
    oldRetentionCodeMap: Map<string, RetentionCodeValue>,
    mapCloseCash: Record<number, number | null>,
): Promise<{ movementIdRetentionsMap: Record<number, number> }> {
    try {
        console.log(`Migrando movimientos de tarjetas`);

        const movementIdRetentionsMap: Record<number, number> = {};
        const mapAuditRetentions: Record<number, number> = {};

        const [movements] = await legacyConn.query(`SELECT 
    retenciones_tarjeta.id as ID_MOVI,
    NULL AS FK_COD_CAJAS_MOVI,
    NULL AS FK_COD_BANCO_MOVI,
    'TARJETA' AS TIP_MOVI,
    NULL AS periodo_caja,
   retenciones_tarjeta.fecha_retencion AS FECHA_MOVI,
    'RET-TAR' AS ORIGEN_MOVI,
    total_retencion AS IMPOR_MOVI,
    'TARJETA' AS TIPO_MOVI,
    'Retención de tarjeta' AS REF_MOVI,
    numero_documento AS CONCEP_MOVI,
    NULL AS SALDO_CAJA_MOVI,
    NULL AS SALDO_BANCO_MOVI,
    1 AS ESTADO_MOVI,
    'RETENCION TARJETA' AS PER_BENE_MOVI,
    'INGRESO' AS CAUSA_MOVI,
    'TARJETA' AS MODULO,
    'RET-TAR' AS ORG_ORDEN,
    retenciones_tarjeta.fecha_retencion AS FECHA_MANUAL,
    'POR CONCILIAR' AS CONCILIADO,
    NULL AS FK_COD_CX,
    NULL AS SECU_MOVI,
    NULL AS FK_CONCILIADO,
    NULL AS FK_ANT_MOVI,
    NULL AS FK_USER,
    NULL AS FK_COD_TRAN,
    NULL AS FK_COD_RH,
    NULL AS FK_DET_PREST,
    NULL AS COD_AUDIT,
    ID_BAN_RET AS FK_COD_TARJETA,
    fk_banco_retenedor AS BANCO_RETENEDOR,
    NULL AS NUMERO_UNIDAD,
    NULL AS RECIBO_CAJA,
    NULL AS COD_EMP,
    NULL AS NUM_VOUCHER,
    NULL AS NUM_LOTE,
    numero_documento AS OBS_MOVI,
    ABS(total_retencion) AS IMPOR_MOVITOTAL,
    NULL AS FK_AUDITMV,
    NULL AS FK_ARQUEO,
    ID_BAN_RET AS ID_TARJETA,
    NULL AS RECIBO_CAJA,
    NULL AS FK_CTAM_PLAN,
    NULL AS NUMERO_UNIDAD,
    NULL ASFDP_DET_ANT,
    subtotal_0 as SUB0_TRAC , 
    subtotal_12 as SUB12_TRAC , 
    iva as IVA_TRAC, 
    total_retencion as TOTALRET_TRAC, 
    neto_pagar as TOTALNETO_TARJETA, 
    detalle_retencion AS TARJETA_DETAIL,
    clave_acceso AS CLAVE_REL_TRANS, 
    numero_documento AS NUM_REL_DOC,
    bancos_emisores_retencion.RUC as BANCO_RETENEDOR_RUC
FROM
    retenciones_tarjeta
INNER JOIN bancos_emisores_retencion ON bancos_emisores_retencion.ID_BAN_RET = retenciones_tarjeta.fk_banco_retenedor
INNER JOIN contabilidad_asientos ON retenciones_tarjeta.id = contabilidad_asientos.cod_origen
WHERE
    tipo_asiento = 'RET-TAR';`);
        /*  const [movements] = rows; */
        if (movements.length === 0) {
            return { movementIdRetentionsMap };
        }
        console.log(`Movimientos de tarjetas... ${movements.length}`);


        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit FROM audit WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );
        let auditSeq = nextAudit;


        const [cardData]: any[] = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`, [newCompanyId]);
        const cardId = cardData[0]?.ID_TARJETA ?? null;

        const movementSequenceQuery = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO = 'RETENCION-TARJETA' AND  FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const [movementData] = movementSequenceQuery;
        let movementSequence = movementData[0]?.SECU_MOVI ?? 1;


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
                mapAuditRetentions[m.ID_MOVI] = currentAuditId;
                const idFkConciliation = mapConciliation[m.FK_CONCILIADO] ?? null;
                let idPlanCuenta = null;
                m.FK_ARQUEO = mapCloseCash[m.FK_ARQUEO] ?? null;
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
                    movementSequence,
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
                    cardId,
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
                movementIdRetentionsMap[o.ID_MOVI] = currentMovId++;
            });


            //agregar card holds
            const valCards = batchMovements.map(o => {
                const currentAuditId = mapAuditRetentions[o.ID_MOVI];

                const retencionVentaNueva = adaptarEstructuraMultiple(oldRetentionCodeMap, o.TARJETA_DETAIL);
                const idMov = movementIdRetentionsMap[o.ID_MOVI];
                const idBanRet = mapWithholdingBanks[o.BANCO_RETENEDOR_RUC] ?? null;

                return [
                    o.FECHA_MOVI,
                    o.SUB12_TRAC,
                    o.SUB0_TRAC,
                    o.IVA_TRAC,
                    o.TOTALRET_TRAC,
                    o.TOTALNETO_TARJETA,
                    JSON.stringify(retencionVentaNueva),
                    cardId,
                    idBanRet,
                    null,
                    currentAuditId,
                    idMov,
                    newCompanyId,
                    o.NUM_REL_DOC,
                    o.CLAVE_REL_TRANS
                ];
            });

            const [resCardsHolds]: any = await conn.query(
                `INSERT INTO card_holds(FECREG_RETENCION, SUBDIFERENTE_TARJETA, SUBCERO_TARJETA, IVA_TARJETA, TOTAL_TARJETA, TOTALNETO_TARJETA, TARJETA_DETAIL, 
                FK_TARJETA, FK_BANKRET, FK_CODTRAN, FK_AUDITMV, FK_MOV, FK_COD_EMP, NUMERO_TARJETA, 
                CLAVE_TARJETA) VALUES ?`,
                [valCards]
            );

            console.log(` -> Batch migrado: ${batchMovements.length} retencion de tarjetas`);
        }
        console.log("✅ Migración  movimientos de tarjetas completada correctamente");
        const mapEntryAccount = await migrateAccountingEntriesRetentions(
            legacyConn,
            conn,
            newCompanyId,
            movementIdRetentionsMap,
            mapPeriodo,
            mapAuditRetentions
        );
        await migrateDetailedAccountingEntriesRetention(
            legacyConn,
            conn,
            newCompanyId,
            mapProject,
            mapCenterCost,
            mapAccounts,
            mapEntryAccount.mapEntryAccount
        )


        return { movementIdRetentionsMap };
    } catch (error) {
        throw error;
    }
}




function formatDecimal(value: any, decimals: number = 2): string {
    const num = typeof value === 'number' ? value : parseFloat(value || 0);
    return isNaN(num) ? '0.00' : num.toFixed(decimals);
};

function parseInteger(value: any): number | null {
    const num = parseInt(value, 10);
    return isNaN(num) ? null : num;
};

function adaptarEstructuraMultiple(oldRetentionCodeMap, estructuraAntigua: any): any[] {
    try {
        // Parsear si es string JSON
        let datos = estructuraAntigua;
        if (typeof estructuraAntigua === 'string') {
            try {
                datos = JSON.parse(estructuraAntigua);
            } catch (parseError) {
                console.error('Error al parsear JSON de retencion:', parseError);
                return [];
            }
        }//retencionRentaCero

        // Normalizar: si viene como array, tomar el primer elemento; si es objeto, usarlo directamente
        const datosOriginales = Array.isArray(datos) ? datos[0] : datos;

        if (!datosOriginales || typeof datosOriginales !== 'object') {
            console.warn('Estructura de retención vacía o inválida');
            return [];
        }

        // Mapear retenciones por renta (Retención en la Fuente)
        const nuevasRetencionesRenta = (datosOriginales.retencionesRenta || [])
            .filter((ret: any) => ret && ret.selectCodigoTributario) // Filtrar nulos/undefined y renta vacío
            .map((ret: any) => ({
                codigoRenta: ret.selectCodigoTributario || null,
                idRetRenta: oldRetentionCodeMap.get(`${ret.selectCodigoTributario}:RENTA`)?.id || '',
                nombreRenta: oldRetentionCodeMap.get(`${ret.selectCodigoTributario}:RENTA`)?.name || '',
                porcentajeRenta: formatDecimal(ret.retencionRentaPorcentaje || '0'),
                subtotalBase0: formatDecimal(ret.retencionRentaCero) + Number(ret.retencionRentaNoGraba),
                subtotalDiferente: formatDecimal(ret.retencionRentaDoce || '0.00'),
                valorRetenido: formatDecimal(ret.retencionRentaValor || '0.00')
            }));



        // Mapear retenciones por IVA
        const nuevasRetencionesIva = (datosOriginales.retencionesIva || [])
            .filter((ret: any) => ret && ret.selectIva)
            .map((ret: any) => {
                return {
                    codigoIva: ret.selectIva || null,
                    idRetIva: oldRetentionCodeMap.get(`${ret.selectIva}:IVA`)?.id || '',
                    nombreIva: oldRetentionCodeMap.get(`${ret.selectIva}:IVA`)?.id || '',
                    porcentajeIva: formatDecimal(ret.retencionIvaPorcentaje),
                    subtotalDiferenteIva: formatDecimal(ret.retencionIvaDoce),
                    valorRetenido: formatDecimal(ret.retencionIvaValor),
                    impuestos: [{
                        codigo: 2,
                        tarifa: 12,
                        total: formatDecimal(ret.retencionIvaDoce) || '0.00'
                    }]
                };
            });


        return [
            {
                listadoRetenciones: nuevasRetencionesRenta,
                listadoRetencionesIva: nuevasRetencionesIva
            }
        ];
    } catch (error) {
        console.error('Error en adaptarEstructuraMultiple:', error);
        return [];
    }
};




export async function migrateAccountingEntriesRetentions(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    movementIdRetentionsMap: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapAuditRetentions: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {


    console.log("🚀 Migrando encabezado de asiento contables tarjetas..........");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const [rows]: any[] = await legacyConn.query(`SELECT 
 cod_asiento,
    fecha_asiento AS FECHA_ASI,
    descripcion_asiento AS DESCRIP_ASI,
    numero_asiento AS NUM_ASI,
   'RET-TAR' as  ORG_ASI,
debe_asiento AS TDEBE_ASI,
haber_asiento AS THABER_ASI,
numero_asiento,
'TARJETA' AS TIP_ASI,
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
NULL AS FK_MOV,
contabilidad_asientos.cod_origen AS FK_ANTDET
FROM
    retenciones_tarjeta
INNER JOIN bancos_emisores_retencion ON bancos_emisores_retencion.ID_BAN_RET = retenciones_tarjeta.fk_banco_retenedor
INNER JOIN contabilidad_asientos ON retenciones_tarjeta.id = contabilidad_asientos.cod_origen
WHERE
    tipo_asiento = 'RET-TAR';` );

        if (!rows.length) {
            return { mapEntryAccount };
        }


        const BATCH_SIZE = 1000;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                const periodoId = mapPeriodo[o.FK_PERIODO]
                const idAuditTr = mapAuditRetentions[o.FK_ANTDET];
                const idMovimiento = movementIdRetentionsMap[o.FK_ANTDET];

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
        console.log("✅ Migración asiento contable tarjetas completada correctamente");
        return { mapEntryAccount };
    } catch (err) {
        console.error("❌ Error en migración de asiento contable:", err);
        throw err;
    }
}
export async function migrateDetailedAccountingEntriesRetention(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {
    console.log("🚀 Cuentas contables");
    console.log("🚀 Iniciando migración de detalles de asientos contables tarjetas..........");

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
    retenciones_tarjeta
INNER JOIN bancos_emisores_retencion ON bancos_emisores_retencion.ID_BAN_RET = retenciones_tarjeta.fk_banco_retenedor
INNER JOIN contabilidad_asientos ON retenciones_tarjeta.id = contabilidad_asientos.cod_origen
INNER JOIN contabilidad_detalle_asiento d ON d.fk_cod_asiento = contabilidad_asientos.cod_asiento
WHERE
    tipo_asiento = 'RET-TAR';`);

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
                console.log(`➡️ Procesando detalle de asiento tarjetas ${idPlan}`);
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

                if (!idPlan || !idCodAsiento) continue;

                mapAccountDetail[o.cod_detalle_asiento] = newId++;
            }

            for (const t of totalsMap.values()) {
                await upsertTotaledEntry(conn, t, newCompanyId);
            }

            console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado tarjetas`);

        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }

    console.log("🎉 Migración  detalles contables completada tarjetas");
    return { mapAccountDetail };
}