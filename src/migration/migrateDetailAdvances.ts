import { upsertTotaledEntry } from "./migrationTools";

export async function migrateAdvancesCustomers(
    legacyConn: any,
    conn: any,
    mapClients: any,
    mapAdvancesCustomers
): Promise<Record<string, number>> {

    console.log("Migrando anticipos de clientes...");
    // Obtiene centroCosto únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT
    ID_DET_ANT AS ID_DET_ANT,
    FEC_DET_ANT,
    OBS_DET_ANT AS ABS_ANT,
    CASE WHEN FDP_DET_ANT = 'CUENTA CONTABLE' THEN 'CCTACONT' WHEN FDP_DET_ANT = 'DEVOLUCION A CAJA' THEN 'DEVCAJA' WHEN FDP_DET_ANT = 'DEVOLUCION' THEN 'DEVBANCO' ELSE FDP_DET_ANT
END AS FORMAPAGO,
ABS(IMPOR_DET_ANT) AS IMPORTE_DET,
SALDO_DET_ANT AS SALDO_DET,
SECU_DET_ANT AS SECUENCIA_DET,
FK_COD_ANT AS FK_IDANT,
PER_DET_ANT AS BENEFICIARIA,
CASE WHEN ref_cuentas > 0 THEN 'CXC' WHEN ref_cuentas > 0 THEN 'CXC' WHEN FDP_DET_ANT = 'NOTA DE CREDITO' THEN 'nota' WHEN FDP_DET_ANT = 'RETENCION EN VENTA' THEN 'RETENCION-VENTA' WHEN CONCAT(
    SUBSTRING_INDEX(FK_ORDEN, '-', 1),
    '-'
) = 'OE-' THEN 'ANT-ORDEN' WHEN CONCAT(
    SUBSTRING_INDEX(FK_ORDEN, '-', 1),
    '-'
) = 'OV-' THEN 'ANT-ORDEN' ELSE 'ANTCLI'
END AS ORIGEN_ANT,
ref_cuentas,
CASE WHEN CONCAT(
    SUBSTRING_INDEX(FK_ORDEN, '-', 1),
    '-'
) = 'OE-' THEN FK_ORDEN WHEN CONCAT(
    SUBSTRING_INDEX(FK_ORDEN, '-', 1),
    '-'
) = 'OV-' THEN FK_ORDEN ELSE NULL
END AS FK_ID_ORDEN,
CASE WHEN FK_COD_TRAC > 0 THEN FK_COD_TRAC ELSE NULL
END AS FK_COD_TRAC,
CASE WHEN FECH_REG = '0000-00-00 00:00:00' THEN FEC_DET_ANT ELSE FECH_REG
END AS FECH_REG,
CASE WHEN IMPOR_DET_ANT > 0 THEN 'INGRESO' ELSE 'EGRESO'
END AS CAUSA_ANT
FROM
    detalle_anticipos
INNER JOIN anticipos ON detalle_anticipos.FK_COD_ANT = anticipos.ID_ANT
WHERE
    anticipos.TIPO_ANT = 'CLIENTES';;`);

    const anticiposClientes = rows as any[];

    if (!anticiposClientes.length) {
        throw new Error(" -> No hay anticipos de clientes para migrar.");
    }


    const BATCH_SIZE = 1000;
    const mapAdvancesDetailCustomers: Record<string, number> = {};
    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {

            const idPersona = mapClients[c.FK_PERSONA] || null;
            return [
                c.FECH_REGANT,
                c.SALDO_ANT,
                idPersona
            ];
        });
        const [res]: any = await conn.query(
            `INSERT INTO detail_advances(FEC_DET_ANT, ABS_ANT, FORMAPAGO, IMPORTE_DET, SALDO_DET, SECUENCIA_DET, FK_IDANT, FK_ID_MOVI, BENEFICIARIA, ORIGEN_ANT, FECH_REG, FK_AUDITANT, CAUSA_ANT, FK_ID_ORDEN) VALUES  ?`,
            [values]
        );
        let newId = res.insertId;
        for (const b of batch) {
            mapAdvancesDetailCustomers[b.ID_ANT] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de clientes`);
    }
    return mapAdvancesDetailCustomers;
}


export async function migrateSupplierAdvances(
    legacyConn: any,
    conn: any,
    mapClients: any
): Promise<{ supplierAdvanceIdMap: Record<number, number> }> {
    console.log("Migrando totales de anticipos de proveedores...");
    const [rows] = await legacyConn.query(`
        SELECT
    ID_DET_ANT AS ID_DET_ANT,
    FEC_DET_ANT,
    OBS_DET_ANT AS ABS_ANT,

    CASE WHEN  FDP_DET_ANT ='CUENTA CONTABLE' THEN 'CCTACONT' 
   WHEN  FDP_DET_ANT = 'DEVOLUCION A CAJA' THEN 'DEVCAJA'
    WHEN  FDP_DET_ANT = 'DEVOLUCION' THEN 'DEVBANCO'
    ELSE FDP_DET_ANT
    END AS FORMAPAGO,
    
    
    IMPOR_DET_ANT AS IMPORTE_DET,
    SALDO_DET_ANT AS SALDO_DET,
    SECU_DET_ANT AS SECUENCIA_DET,
    FK_COD_ANT AS FK_IDANT,
    PER_DET_ANT AS BENEFICIARIA,
     CASE WHEN  ref_cuentas >0 THEN 'CXC' 
     WHEN  ref_cuentas >0 THEN 'CXC' 
      WHEN  FDP_DET_ANT ='NOTA DE CREDITO' THEN 'nota' 
    WHEN  FDP_DET_ANT = 'RETENCION EN VENTA'  THEN 'RETENCION-VENTA'
    WHEN  CONCAT(SUBSTRING_INDEX(FK_ORDEN, '-', 1), '-')='OE' THEN 'ANT-ORDEN'
     WHEN  CONCAT(SUBSTRING_INDEX(FK_ORDEN, '-', 1), '-')='OV' THEN 'ANT-ORDEN' ELSE
    'ANTCLI' END AS ORIGEN_ANT,
    
    ref_cuentas,
    FK_ORDEN AS FK_ID_ORDEN,
  CASE WHEN  FK_COD_TRAC>0 THEN FK_COD_TRAC ELSE NULL END AS FK_COD_TRAC ,
   CASE WHEN FECH_REG= '0000-00-00 00:00:00' THEN FEC_DET_ANT ELSE FECH_REG END AS FECH_REG,
     CASE WHEN IMPOR_DET_ANT>0 THEN 'INGRESO' ELSE 'EGRESO' END AS CAUSA_ANT
FROM
    detalle_anticipos
WHERE
    1;
    `);

    const anticiposClientes = rows as any[];

    if (!anticiposClientes.length) {
        throw new Error(" -> No hay anticipos de proveedores para migrar.");
    }
    const BATCH_SIZE = 1000;
    const supplierAdvanceIdMap: Record<number, number> = {};
    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {
            const idPersona = mapClients[c.FK_PERSONA] || null;
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
            supplierAdvanceIdMap[b.ID_ANT] = newId;
            newId++;
        }
        console.log(` -> Batch migrado: ${batch.length} anticipos de proveedores`);
    }
    return { supplierAdvanceIdMap };
}

async function migrateDataMovements({
    legacyConn,
    conn,
    newCompanyId,
    purchaseLiquidationIdMap,
    purchaseLiquidationAuditIdMap,
    mapConciliation,
    userMap,
    bankMap,
    boxMap,
    accountMap
}: MigrateDataMovementsParams): Promise<{
    movementIdMap: Record<number, number>
}> {
    try {
        const movementIdMap: Record<number, number> = {};

        const movementsQuery: ResultSet = await legacyConn.query(`
			SELECT
    ID_MOVI AS ID_MOVI,
    CASE WHEN FK_COD_CAJAS_MOVI > 0 THEN FK_COD_CAJAS_MOVI ELSE NULL
END AS FK_COD_CAJAS_MOVI,
CASE WHEN FK_COD_BANCO_MOVI > 0 THEN FK_COD_BANCO_MOVI ELSE NULL
END AS FK_COD_BANCO_MOVI,
CASE WHEN TIPO_MOVI = 'CUENTA CONTABLE' THEN 'CCTACONT' 
WHEN TIPO_MOVI = 'ANTICIPO' THEN 'ANTICIPO' ELSE TIP_MOVI
END AS TIP_MOVI,
periodo_caja,
FECHA_MOVI AS FECHA_MOVI,
CASE WHEN ORIGEN_MOVI = 'ANTICIPOS-CLIENTES' THEN 'ANTCLI' ELSE ORIGEN_MOVI
END AS ORIGEN_MOVI,
abs(detalle_anticipos.IMPOR_DET_ANT) AS IMPOR_MOVI,


CASE 
    WHEN TIPO_MOVI = 'DEVOLUCION' AND TIP_MOVI = 'CAJAS' THEN 'DEVCAJA'
    WHEN TIPO_MOVI = 'DEVOLUCION' AND TIP_MOVI <> 'CAJAS' THEN 'DEVBANCO'
    WHEN TIPO_MOVI='CUENTA CONTABLE' THEN 'CCTACONT'
    ELSE TIPO_MOVI
END AS TIPO_MOVI,


REF_MOVI,
CONCEP_MOVI,
SALDO_CAJA_MOVI,
SALDO_BANCO_MOVI,

CASE WHEN ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0
			END AS ESTADO_MOVI,

CASE WHEN PER_BENE_MOVI = '' THEN  clientes.NOM_CLI ELSE PER_BENE_MOVI
			END AS PER_BENE_MOVI,


CAUSA_MOVI,

CASE WHEN ORIGEN_MOVI = 'ANTICIPOS-CLIENTES' THEN 'ANTCLI' ELSE ORIGEN_MOVI
END AS MODULO,

FECHA_MANUAL AS FECHA_MANUAL,
CONCILIADO,
FK_COD_CX,
SECU_MOVI,
FK_CONCILIADO AS FK_CONCILIADO,
FK_ANT_MOVI,
FK_USER_EMP_MOVI AS FK_USER,
FK_TRAC_MOVI AS FK_COD_TRAN,
FK_COD_RH,
FK_DET_PREST,
COD_AUDIT,
FK_COD_TARJETA,
NUM_UNIDAD AS NUMERO_UNIDAD,
RECIBO_CAJA,
COD_EMP,
NULL AS NUM_VOUCHER,
NULL AS NUM_LOTE,
CONCEP_MOVI AS OBS_MOVI,
abs(IMPOR_MOVI) AS IMPOR_MOVITOTAL,
NULL AS FK_AUDITMV,
NULL AS FK_ARQUEO,
NULL AS ID_TARJETA,
NULL AS RECIBO_CAJA,
NULL AS FK_CTAM_PLAN,
NULL AS NUMERO_UNIDAD
FROM
    movimientos
INNER JOIN detalle_anticipos ON movimientos.FK_ANT_MOVI = detalle_anticipos.ID_DET_ANT
INNER JOIN anticipos ON detalle_anticipos.FK_COD_ANT = anticipos.ID_ANT
INNER JOIN clientes ON clientes.COD_CLI= anticipos.FK_COD_CLI_ANT
WHERE
    anticipos.TIPO_ANT = 'CLIENTES';;
		`);
        const [movements]: any[] = movementsQuery as Array<any>;
        if (movements.length === 0) {
            return { movementIdMap };
        }

        const cardQuery: ResultSet = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`, [newCompanyId]);
        const [cardData]: any[] = cardQuery as Array<any>;
        const cardId = cardData[0].ID_TARJETA ?? null;

        const movementSequenceQuery = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO = 'COMPRAS' AND  FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const [movementData] = movementSequenceQuery as Array<any>;
        let movementSequence = movementData[0]?.SECU_MOVI ?? 1;

        const BATCH_SIZE = 500;

        for (let i = 0; i < movements.length; i += BATCH_SIZE) {
            const batchMovements = movements.slice(i, i + BATCH_SIZE);
            const movementValues: any[] = [];

            for (const m of batchMovements) {
                const bankId = bankMap[m.FK_COD_BANCO_MOVI];
                const idBoxDetail = boxMap[m.FK_COD_CAJAS_MOVI];
                const transactionId = purchaseLiquidationIdMap[m.FK_TRAC_MOVI];
                const userId = userMap[m.FK_USER_EMP_MOVI];
                const transAuditId = purchaseLiquidationAuditIdMap[m.COD_TRANS];
                const idFkConciliation = mapConciliation[m.FK_CONCILIADO] ?? null;
                const idPlanAccount = null;
                let idPlanCuenta = null;
                if (m.TIPO_MOVI === 'CCTACONT') {
                    const codigoPlan = m.REF_MOVI?.split('--')[0].trim();
                    idPlanCuenta = accountMap.get(codigoPlan) || null;

                }

                movementValues.push([
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
                    transAuditId,
                    m.FK_ARQUEO,
                    cardId,
                    m.RECIBO_CAJA,
                    idPlanAccount,
                    m.NUM_UNIDAD,
                    m.JSON_PAGOS
                ]);
                movementSequence++;
            }

            const resultCreateMovement = await conn.query(`
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
            let nextMovementId = (resultCreateMovement[0] as ResultSetHeader).insertId;
            batchMovements.forEach(({ COD_TRANS }) => {
                movementIdMap[COD_TRANS] = nextMovementId++;
            });
        }
        console.log("✅ Migración de movimientos compra y liquidacion completada correctamente");
        return { movementIdMap };
    } catch (error) {
        throw error;
    }
}

export async function migrateAccountingEntriesCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapNoteMovementsFull: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapAuditCreditNote: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {

    /* legacyConn,
      conn,
      newCompanyId,
      mapNoteMovementsFull,
      mapPeriodo,
      mapAuditCreditNote */

    console.log("🚀 Migrando encabezado de asiento contables retenciones..........");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const [rows]: any[] = await legacyConn.query(`SELECT
    cod_asiento,
    fecha_asiento AS FECHA_ASI,
    descripcion_asiento AS DESCRIP_ASI,
    numero_asiento AS NUM_ASI,
    'NOTA CREDITO DEVOLUCION VENTA' AS ORG_ASI,
    debe_asiento AS TDEBE_ASI,
    haber_asiento AS THABER_ASI,
   
    numero_asiento,
  
  case WHEN transacciones.tipo_nota='descuento' then 'NC-VEN-DEV' ELSE 'NC-VEN-DESC' END
    AS TIP_ASI,
    fk_cod_periodo AS FK_PERIODO,
    fecha_registro_asiento AS FECHA_REG,
    fecha_update_asiento AS FECHA_ACT,
    json_asi AS JSON_ASI,
    res_asiento AS RES_ASI,
    ben_asiento AS BEN_ASI,
    NULL AS FK_AUDIT,
    NULL AS FK_COD_EMP,
    contabilidad_asientos.FK_CODTRAC,
    transacciones.COD_TRAC,
    CAST(
        REGEXP_REPLACE(
            RIGHT(numero_asiento, 9),
            '[^0-9]',
            ''
        ) AS UNSIGNED
    ) AS SEC_ASI,
    cod_origen,
    NULL AS FK_MOV
FROM
    contabilidad_asientos
INNER JOIN transacciones ON transacciones.COD_TRAC = contabilidad_asientos.FK_CODTRAC
WHERE
    TIP_TRAC IN('nota');` );

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
                const idAuditTr = mapAuditCreditNote[o.COD_TRAC];
                const idMovimiento = mapNoteMovementsFull[o.COD_TRAC];

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
        console.log("✅ Migración asiento contable retencion completada correctamente");
        return { mapEntryAccount };
    } catch (err) {
        console.error("❌ Error en migración de asiento contable:", err);
        throw err;
    }
}

export async function migrateDetailedAccountingEntriesCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {
    console.log("🚀 Cuentas contables");
    console.log("🚀 Iniciando migración de detalles de asientos contables retenciones..........");

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
    
INNER JOIN transacciones ON transacciones.COD_TRAC = contabilidad_asientos.FK_CODTRAC
INNER JOIN contabilidad_detalle_asiento d ON d.fk_cod_asiento = contabilidad_asientos.cod_asiento
WHERE
     TIP_TRAC IN('nota');`);

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

    console.log("🎉 Migración de detalles contables completada");
    return { mapAccountDetail };
}