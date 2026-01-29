import { upsertTotaledEntry } from "./migrationTools";


export async function migrateDataMovements(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    purchaseLiquidationIdMap: Record<number, number>,
    purchaseLiquidationAuditIdMap: Record<number, number>,
    mapConciliation: Record<number, number>,
    userMap: Record<number, number>,
    bankMap: Record<number, number>,
    boxMap: Record<number, number>,
    mapAdvancesCustomers: Record<number, number>,
    mapCreditNote: Record<number, number>,
    mapAuditCreditNote: Record<number, number>,
    mapRetMovements: Record<number, number>,
    mapRetAuditSales: Record<number, number>,
    movAudit: any[],
    mapMovements: Record<number, number>,
    mapAuditMovements: Record<number, number>,
    mapPeriodo: Record<number, number>,
    mapProject: Record<number, number>,
    mapCenterCost: Record<number, number>,
    mapAccounts: Record<number, number>,
    workOrderIdMap: Record<number, number>,
    workOrderAuditIdMap: Record<number, number>,
    workOrderSecuencieMap: Record<number, number>
): Promise<{ movementIdAdvancesMap: Record<number, number> }> {
    try {
        const movementIdAdvancesMap: Record<number, number> = {};
        const mapAuditAdvances: Record<number, number> = {};

        const [movements] = await legacyConn.query(`
                                            SELECT
    ID_MOVI AS ID_MOVI,
    CASE WHEN FK_COD_CAJAS_MOVI > 0 THEN FK_COD_CAJAS_MOVI ELSE NULL
END AS FK_COD_CAJAS_MOVI,
CASE WHEN FK_COD_BANCO_MOVI > 0 THEN FK_COD_BANCO_MOVI ELSE NULL
END AS FK_COD_BANCO_MOVI,
CASE WHEN TIPO_MOVI = 'CUENTA CONTABLE' THEN 'CCTACONT' WHEN TIPO_MOVI = 'ANTICIPO' THEN 'ANTICIPO' ELSE TIP_MOVI
END AS TIP_MOVI,
periodo_caja,
FECHA_MOVI AS FECHA_MOVI,
CASE 
WHEN SUBSTRING_INDEX(detalle_anticipos.FK_ORDEN, '-', 1) IN('OE', 'OV') THEN 'ANT-ORDEN' ELSE 'ANTCLI'
END AS ORIGEN_MOVI,
ABS(
    detalle_anticipos.IMPOR_DET_ANT
) AS IMPOR_MOVI,
CASE WHEN TIPO_MOVI = 'DEVOLUCION' AND TIP_MOVI = 'CAJAS' THEN 'DEVCAJA' WHEN TIPO_MOVI = 'DEVOLUCION' AND TIP_MOVI <> 'CAJAS' THEN 'DEVBANCO' WHEN TIPO_MOVI = 'CUENTA CONTABLE' THEN 'CCTACONT' ELSE TIPO_MOVI
END AS TIPO_MOVI,
REF_MOVI,

CASE 
#WHEN ORIGEN_MOVI = 'ANTICIPOS-CLIENTES' THEN 'ANTCLI' 
WHEN SUBSTRING_INDEX(detalle_anticipos.FK_ORDEN, '-', 1) IN('OE', 'OV') THEN detalle_anticipos.FK_ORDEN ELSE CONCEP_MOVI
END AS CONCEP_MOVI,

SALDO_CAJA_MOVI,
SALDO_BANCO_MOVI,
CASE WHEN ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0
END AS ESTADO_MOVI,
CASE WHEN PER_BENE_MOVI = '' THEN clientes.NOM_CLI ELSE PER_BENE_MOVI
END AS PER_BENE_MOVI,
CAUSA_MOVI,
#CASE WHEN ORIGEN_MOVI = 'ANTICIPOS-CLIENTES' THEN 'ANTCLI' ELSE ORIGEN_MOVI
case
WHEN SUBSTRING_INDEX(detalle_anticipos.FK_ORDEN, '-', 1) IN('OE', 'OV') THEN 'ANT-ORDEN' ELSE 'ANTCLI'
END AS MODULO,
CASE
WHEN SUBSTRING_INDEX(detalle_anticipos.FK_ORDEN, '-', 1) IN('OE') THEN 'ANT-ORDEN-TRABAJO' 
WHEN SUBSTRING_INDEX(detalle_anticipos.FK_ORDEN, '-', 1) IN('OV') THEN 'ANT-ORDEN-VENTA' ELSE 'ANTCLI'
END AS ORG_ORDEN,
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
ABS(IMPOR_MOVI) AS IMPOR_MOVITOTAL,
NULL AS FK_AUDITMV,
NULL AS FK_ARQUEO,
NULL AS ID_TARJETA,
NULL AS RECIBO_CAJA,
NULL AS FK_CTAM_PLAN,
NULL AS NUMERO_UNIDAD,
FDP_DET_ANT
FROM
    detalle_anticipos
INNER JOIN anticipos ON anticipos.ID_ANT = detalle_anticipos.FK_COD_ANT
INNER JOIN movimientos ON movimientos.FK_ANT_MOVI = detalle_anticipos.ID_DET_ANT
INNER JOIN clientes ON clientes.COD_CLI = anticipos.FK_COD_CLI_ANT
WHERE
    anticipos.TIPO_ANT = 'CLIENTES' AND ORIGEN_MOVI <> 'CxC';`);
        /*  const [movements] = rows; */
        if (movements.length === 0) {
            return { movementIdAdvancesMap };
        } /* console.log(movements); */
        console.log(`Movimientos de anticipos... ${movements.length}`);


        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit FROM audit WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );
        let auditSeq = nextAudit;


        const [cardData]: any[] = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`, [newCompanyId]);
        const cardId = cardData[0]?.ID_TARJETA ?? null;

        const movementSequenceQuery = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO = 'ANTCLI' AND  FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const [movementData] = movementSequenceQuery;
        let movementSequence = movementData[0]?.SECU_MOVI ?? 1;


        const [accountRows]: any = await conn.query(
            `SELECT ID_PLAN, CODIGO_PLAN FROM account_plan WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );
        const accountMap = new Map(accountRows.map((a: any) => [a.CODIGO_PLAN, a.ID_PLAN]));

        const BATCH_SIZE = 1500;
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
                mapAuditAdvances[m.FK_ANT_MOVI] = currentAuditId;
                const idFkConciliation = mapConciliation[m.FK_CONCILIADO] ?? null;
                let idPlanCuenta = null;
                if (m.TIPO_MOVI === 'CCTACONT') {
                    const codigoPlan = m.REF_MOVI?.split('--')[0].trim();
                    idPlanCuenta = accountMap.get(codigoPlan) || null;
                }
                if (m.MODULO === 'ANT-ORDEN') {

                    if (m.ORG_ORDEN === 'ANT-ORDEN-VENTA') {
                        transactionId = null;
                    }
                    if (m.ORG_ORDEN === 'ANT-ORDEN-TRABAJO') {
                        transactionId = workOrderSecuencieMap[m.CONCEP_MOVI] || null;
                        /*  console.log(`transactionId: ${transactionId}`); */
                    }
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
                    m.TIPO_MOVI == 'TARJETA' ? cardId : null,
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
                movementIdAdvancesMap[o.FK_ANT_MOVI] = currentMovId++;
            });
            console.log(` -> Batch migrado: ${batchMovements.length} anticipos clientes`);

        }
        console.log("✅ Migración de movimientos anticipos clientes");
        const { mapNoteMovementsFull } = await migrateAdvancesCustomers(
            legacyConn,
            conn,
            mapAdvancesCustomers,
            mapAuditCreditNote,
            mapRetAuditSales,
            mapMovements,
            mapAuditMovements,
            movementIdAdvancesMap,
            mapAuditAdvances,
            workOrderSecuencieMap
        );



        const mapEntryAccount = await migrateAccountingEntriesCustomerObligations(
            legacyConn,
            conn,
            newCompanyId,
            movementIdAdvancesMap,
            mapPeriodo,
            mapAuditAdvances
        );

        const mapEntryDetailAccount = await migrateDetailedAccountingEntriesCustomerObligations(
            legacyConn,
            conn,
            newCompanyId,
            mapProject,
            mapCenterCost,
            mapAccounts,
            mapEntryAccount.mapEntryAccount
        )



        return { movementIdAdvancesMap };
    } catch (error) {
        throw error;
    }
}


export async function migrateAdvancesCustomers(
    legacyConn: any,
    conn: any,
    mapAdvancesCustomers: Record<number, number>,
    mapAuditCreditNote: Record<number, number>,
    mapRetAuditSales: Record<number, number>,
    mapMovements: Record<number, number>,
    mapAuditMovements: Record<number, number>,
    movementIdAdvancesMap: Record<number, number>,
    mapAuditAdvances: Record<number, number>,
    workOrderSecuencieMap: Record<number, number>
): Promise<Record<string, number>> {


    console.log("Migrando detalle de anticipos de clientes...");
    // Obtiene centroCosto únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT
    da.ID_DET_ANT,
    da.FEC_DET_ANT,
    da.OBS_DET_ANT AS ABS_ANT,

    CASE da.FDP_DET_ANT
        WHEN 'CUENTA CONTABLE'     THEN 'CCTACONT'
        WHEN 'DEVOLUCION A CAJA'   THEN 'DEVCAJA'
        WHEN 'DEVOLUCION'          THEN 'DEVBANCO'
        ELSE da.FDP_DET_ANT
    END AS FORMAPAGO,

    ABS(da.IMPOR_DET_ANT) AS IMPORTE_DET,
    da.SALDO_DET_ANT AS SALDO_DET,
    da.SECU_DET_ANT AS SECUENCIA_DET,
    da.FK_COD_ANT AS FK_IDANT,
    da.PER_DET_ANT AS BENEFICIARIA,

    CASE
        WHEN da.ref_cuentas > 0 THEN 'CXC'
        WHEN da.FDP_DET_ANT = 'NOTA DE CREDITO' THEN 'nota'
        WHEN da.FDP_DET_ANT = 'RETENCION EN VENTA' THEN 'RETENCION-VENTA'
        WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN ('OE', 'OV') THEN 'ANT-ORDEN'
        ELSE 'ANTCLI'
    END AS ORIGEN_ANT,

CASE
WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN('OE') THEN 'ANT-ORDEN-TRABAJO' 
WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN('OV') THEN 'ANT-ORDEN-VENTA' ELSE 'ANTCLI'
END AS ORG_ORDEN,

    CASE
        WHEN da.ref_cuentas > 0 THEN da.ref_cuentas else NULL
    END AS ref_cuentas,
    CASE
        WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN ('OE', 'OV')
        THEN da.FK_ORDEN
        ELSE NULL
    END AS FK_ID_ORDEN,

    CASE
        WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN ('OE', 'OV')
        THEN da.FK_ORDEN
        ELSE NULL
    END AS  SECUENCIA,

    CASE
        WHEN da.FK_COD_TRAC > 0 THEN da.FK_COD_TRAC
        ELSE NULL
    END AS FK_COD_TRAC,

    CASE
        WHEN da.FECH_REG = '0000-00-00 00:00:00'
        THEN da.FEC_DET_ANT
        ELSE da.FECH_REG
    END AS FECH_REG,

    CASE
        WHEN da.IMPOR_DET_ANT > 0 THEN 'INGRESO'
        ELSE 'EGRESO'
    END AS CAUSA_ANT

FROM detalle_anticipos da
INNER JOIN anticipos a 
    ON a.ID_ANT = da.FK_COD_ANT
INNER JOIN clientes c 
    ON c.COD_CLI = a.FK_COD_CLI_ANT
LEFT JOIN movimientos m 
    ON m.FK_ANT_MOVI = da.ID_DET_ANT  and date(da.FEC_DET_ANT) = date(m.FECHA_MANUAL) and m.IMPOR_MOVI = da.IMPOR_DET_ANT  and da.PER_DET_ANT = m.PER_BENE_MOVI 

WHERE a.TIPO_ANT = 'CLIENTES';`);

    /*
    SELECT
    da.ID_DET_ANT,
    da.FEC_DET_ANT,
    da.OBS_DET_ANT AS ABS_ANT,

    CASE da.FDP_DET_ANT
        WHEN 'CUENTA CONTABLE'     THEN 'CCTACONT'
        WHEN 'DEVOLUCION A CAJA'   THEN 'DEVCAJA'
        WHEN 'DEVOLUCION'          THEN 'DEVBANCO'
        ELSE da.FDP_DET_ANT
    END AS FORMAPAGO,

    ABS(da.IMPOR_DET_ANT) AS IMPORTE_DET,
    da.SALDO_DET_ANT AS SALDO_DET,
    da.SECU_DET_ANT AS SECUENCIA_DET,
    da.FK_COD_ANT AS FK_IDANT,
    da.PER_DET_ANT AS BENEFICIARIA,

    CASE
        WHEN da.ref_cuentas > 0 THEN 'CXC'
        WHEN da.FDP_DET_ANT = 'NOTA DE CREDITO' THEN 'nota'
        WHEN da.FDP_DET_ANT = 'RETENCION EN VENTA' THEN 'RETENCION-VENTA'
        WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN ('OE', 'OV') THEN 'ANT-ORDEN'
        ELSE 'ANTCLI'
    END AS ORIGEN_ANT,

CASE
WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN('OE') THEN 'ANT-ORDEN-TRABAJO' 
WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN('OV') THEN 'ANT-ORDEN-VENTA' ELSE 'ANTCLI'
END AS ORG_ORDEN,

    CASE
        WHEN da.ref_cuentas > 0 THEN da.ref_cuentas else NULL
    END AS ref_cuentas,
    CASE
        WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN ('OE', 'OV')
        THEN da.FK_ORDEN
        ELSE NULL
    END AS FK_ID_ORDEN,

    CASE
        WHEN SUBSTRING_INDEX(da.FK_ORDEN, '-', 1) IN ('OE', 'OV')
        THEN da.FK_ORDEN
        ELSE NULL
    END AS  SECUENCIA,

    CASE
        WHEN da.FK_COD_TRAC > 0 THEN da.FK_COD_TRAC
        ELSE NULL
    END AS FK_COD_TRAC,

    CASE
        WHEN da.FECH_REG = '0000-00-00 00:00:00'
        THEN da.FEC_DET_ANT
        ELSE da.FECH_REG
    END AS FECH_REG,

    CASE
        WHEN da.IMPOR_DET_ANT > 0 THEN 'INGRESO'
        ELSE 'EGRESO'
    END AS CAUSA_ANT

FROM detalle_anticipos da
INNER JOIN anticipos a 
    ON a.ID_ANT = da.FK_COD_ANT
INNER JOIN clientes c 
    ON c.COD_CLI = a.FK_COD_CLI_ANT
LEFT JOIN movimientos m 
    ON m.FK_ANT_MOVI = da.ID_DET_ANT

WHERE a.TIPO_ANT = 'CLIENTES';
    */

    const anticiposClientes = rows;
    const mapAdvancesDetailCustomers: Record<string, number> = {};
    if (!anticiposClientes.length) {
        return mapAdvancesDetailCustomers;
    }
    const BATCH_SIZE = 1000;

    for (let i = 0; i < anticiposClientes.length; i += BATCH_SIZE) {
        const batch = anticiposClientes.slice(i, i + BATCH_SIZE);

        const values = [];
        for (const a of batch) {
            let idMov = null;
            let idAuditoria = null;
            a.FK_ID_ORDEN = null;
            if (a.FORMAPAGO == 'NOTA DE CREDITO') {
                idAuditoria = mapAuditCreditNote[a.FK_COD_TRAC];
                const [accountRows]: any = await conn.query(
                    `SELECT * FROM movements WHERE FK_AUDITMV = ?`,
                    [idAuditoria]
                );
                idMov = accountRows[0]?.ID_MOVI || null;
            }
            if (a.FORMAPAGO == 'RETENCION EN VENTA') {
                idAuditoria = mapRetAuditSales[a.FK_COD_TRAC];
                const [accountRows]: any = await conn.query(
                    `SELECT * FROM movements WHERE FK_AUDITMV = ?`,
                    [idAuditoria]
                );
                idMov = accountRows[0]?.ID_MOVI || null;
            }
            //CUENTAS POR COBRAR
            if (a.ref_cuentas != null) {

                idAuditoria = mapAuditMovements[a.ref_cuentas];
                idMov = mapMovements[a.ref_cuentas] || null;

            }
            if (a.ORIGEN_ANT == 'ANTCLI') {
                idAuditoria = mapAuditAdvances[a.ID_DET_ANT];
                idMov = movementIdAdvancesMap[a.ID_DET_ANT] || null;
            }

            if (a.ORIGEN_ANT == 'ANT-ORDEN') {
                idAuditoria = mapAuditAdvances[a.ID_DET_ANT];
                idMov = movementIdAdvancesMap[a.ID_DET_ANT] || null;

            }

            if (a.ORIGEN_ANT === 'ANT-ORDEN') {


                if (a.ORG_ORDEN === 'ANT-ORDEN-VENTA') {
                    a.FK_ID_ORDEN = null;
                }
                if (a.ORG_ORDEN === 'ANT-ORDEN-TRABAJO') {
                    a.FK_ID_ORDEN = workOrderSecuencieMap[a.SECUENCIA] || null;
                }
            }





            const idAdvance = mapAdvancesCustomers[a.FK_IDANT];
            /* a.FK_ID_ORDEN = null; */
            values.push([
                a.FEC_DET_ANT,
                a.ABS_ANT,
                a.FORMAPAGO,
                a.IMPORTE_DET,
                a.SALDO_DET,
                a.SECUENCIA_DET,
                idAdvance,
                idMov,
                a.BENEFICIARIA,
                a.ORIGEN_ANT,
                a.FECH_REG,
                idAuditoria,
                a.CAUSA_ANT,
                a.FK_ID_ORDEN
            ]);
        };
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

export async function migrateAccountingEntriesCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    movementIdAdvancesMap: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapAuditAdvances: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {


    console.log("🚀 Migrando encabezado de asiento contables anticipo clientes..........");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const [rows]: any[] = await legacyConn.query(`SELECT
    cod_asiento,
    fecha_asiento AS FECHA_ASI,
    descripcion_asiento AS DESCRIP_ASI,
    numero_asiento AS NUM_ASI,
    #'ANTCLI' AS ORG_ASI,

CASE 
WHEN SUBSTRING_INDEX(detalle_anticipos.FK_ORDEN, '-', 1) IN('OE', 'OV') THEN 'ANT-ORDEN' ELSE 'ANTCLI'
END AS ORG_ASI,

    debe_asiento AS TDEBE_ASI,
    haber_asiento AS THABER_ASI,
    numero_asiento,
    #'ANTCLI' AS TIP_ASI,

CASE 
WHEN SUBSTRING_INDEX(detalle_anticipos.FK_ORDEN, '-', 1) IN('OE', 'OV') THEN 'ANT-ORDEN' ELSE 'ANTCLI'
END AS TIP_ASI,

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
INNER JOIN detalle_anticipos ON detalle_anticipos.ID_DET_ANT = contabilidad_asientos.cod_origen
INNER JOIN anticipos ON anticipos.ID_ANT = detalle_anticipos.FK_COD_ANT
INNER JOIN movimientos ON movimientos.FK_ANT_MOVI = detalle_anticipos.ID_DET_ANT #and date(detalle_anticipos.FEC_DET_ANT) = date(movimientos.FECHA_MANUAL) and movimientos.IMPOR_MOVI = detalle_anticipos.IMPOR_DET_ANT  and detalle_anticipos.PER_DET_ANT = movimientos.PER_BENE_MOVI 
INNER JOIN clientes ON clientes.COD_CLI = anticipos.FK_COD_CLI_ANT
WHERE
    origen_asiento = 'ANTTICIPOS CLI';` );

        if (!rows.length) {
            return { mapEntryAccount };
        }


        const BATCH_SIZE = 1000;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                const periodoId = mapPeriodo[o.FK_PERIODO]
                const idAuditTr = mapAuditAdvances[o.FK_ANTDET];
                const idMovimiento = movementIdAdvancesMap[o.FK_ANTDET];

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
             console.log(` -> Batch migrado: ${batch.length} asiento anticipo clientes`);
        }
        console.log("✅ Migración asiento contable anticipo clientes completada correctamente");
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
    console.log("🚀 Iniciando migración de detalles de asientos contables anticipo clientes..........");

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
INNER JOIN detalle_anticipos ON detalle_anticipos.ID_DET_ANT = contabilidad_asientos.cod_origen
INNER JOIN contabilidad_detalle_asiento d ON d.fk_cod_asiento = contabilidad_asientos.cod_asiento
INNER JOIN anticipos ON anticipos.ID_ANT = detalle_anticipos.FK_COD_ANT
INNER JOIN movimientos ON movimientos.FK_ANT_MOVI = detalle_anticipos.ID_DET_ANT
INNER JOIN clientes ON clientes.COD_CLI = anticipos.FK_COD_CLI_ANT
WHERE
    origen_asiento = 'ANTTICIPOS CLI' ;`);

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
                console.log(`➡️ Procesando detalle de asiento anticipo clientes ${idPlan}`);
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

                if (!idPlan || !idCodAsiento) continue;

                mapAccountDetail[o.cod_detalle_asiento] = newId++;
            }

            for (const t of totalsMap.values()) {
                await upsertTotaledEntry(conn, t, newCompanyId);
            }

            console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado anticipo clientes`);

        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }

    console.log("🎉 Migración  detalles contables completada anticipo clientes");
    return { mapAccountDetail };
}