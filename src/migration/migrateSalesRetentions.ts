import { upsertTotaledEntry } from "./migrationTools";

export async function migrateSalesRetentions(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    userMap: any,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
    mapSales: Record<number, number | null>,
    mapObligationsCustomers: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapRetentions: Record<number, number | null>,
    /* mapEntryAccount: Record<number, number | null> */
): Promise<{ mapRetMovements: Record<number, number>; mapRetAuditSales: Record<number, number>; movAudit: any[] }> {
    console.log("Migrando retenciones en ventas...");



    const mapRetMovements: Record<number, number> = {};
    const mapRetAuditMovements: Record<number, number> = {};

    const [rows] = await legacyConn.query(`SELECT
    COD_TRAC,
    COD_TRAC AS FK_COD_TRAN,
    m.FK_COD_BANCO_MOVI AS FKBANCO,
    m.FK_CONCILIADO AS FK_CONCILIADO,
    IFNULL(
        m.FK_USER_EMP_MOVI,
        transacciones.FK_COD_USU
    ) AS FK_USER,
    IFNULL(
        m.FECHA_MOVI,
        transacciones.fecha
    ) AS FECHA_MOVI,
    IFNULL(
        FECHA_MANUAL,
        transacciones.FEC_TRAC
    ) AS FECHA_MANUAL,
    CASE WHEN COALESCE(ID_DET_ANT, '') <> '' THEN 'ANTICIPO' WHEN m.TIP_MOVI IS NULL THEN 'RET-VENTA' ELSE m.TIP_MOVI
END AS TIP_MOVI,
'RET-VENTA' AS ORIGEN_MOVI,
CASE WHEN COALESCE(ID_DET_ANT, '') <> '' THEN 'ANTICIPO' WHEN m.TIP_MOVI IS NULL THEN 'RET-VENTA' WHEN m.TIP_MOVI = 'BANCOS' THEN 'DEVBANCO' WHEN m.TIP_MOVI = 'CAJAS' THEN 'DEVCAJA' ELSE m.TIP_MOVI
END AS TIPO_MOVI,
IFNULL(CONCEP_MOVI, OBS_DET_ANT) AS REF_MOVI,
IFNULL(CONCEP_MOVI, OBS_DET_ANT) AS CONCEP_MOVI,
NULL AS NUM_VOUCHER,
NULL AS NUM_LOTE,
IFNULL(CAUSA_MOVI, 'INGRESO') AS CAUSA_MOVI,

'RETENCION-VENTA' as MODULO,NULL AS SECU_MOVI, 

IFNULL( m.IMPOR_MOVI, detalle_anticipos.IMPOR_DET_ANT) AS IMPOR_MOVI,

 CASE WHEN  m.ESTADO_MOVI='ACTIVO' THEN 1 ELSE 0 END AS ESTADO_MOVI, 
   IFNULL(m.PER_BENE_MOVI,  clientes.NOM_CLI) AS PER_BENE_MOVI,
NULL AS FK_COD_EMP,
 m.FK_CONCILIADO, m.CONCILIADO,
FK_COD_CAJAS_MOVI AS IDCAJA,

IFNULL( m.CONCEP_MOVI, detalle_anticipos.OBS_DET_ANT) AS OBS_MOVI,
IFNULL( m.IMPOR_MOVI, detalle_anticipos.IMPOR_DET_ANT) AS IMPOR_MOVITOTAL,

NULL AS FK_ASIENTO,
NULL AS FK_AUDITMV,NULL AS FK_ARQUEO,NULL AS ID_TARJETA, NULL AS RECIBO_CAJA,NULL AS FK_CTAM_PLAN,NULL AS NUMERO_UNIDAD,NULL AS JSON_PAGOS,

SUBSTRING(secuencialFacturaRet, 9, 9) AS SECUENCIA_REL_DOC,
transacciones.claveFacturaRet AS CLAVE_REL_TRANS,
YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
MONTH(FEC_TRAC) AS FEC_MES_TRAC,
FEC_TRAC AS FEC_TRAC,
fechaFacturaRet AS FEC_REL_TRAC,
FEC_MERC_TRAC AS FEC_MERC_TRAC,
METPAG_TRAC,
cabecera_compra AS DOCUMENT_REL_DETAIL,
OBS_TRAC,
FK_USU_CAJA AS FK_USER_VEND,
FK_COD_CLI AS FK_PERSON,
estado_compra AS ESTADO_REL,
SUB_TRAC AS SUB_TRAC,
IVA_TRAC AS IVA_TRAC,
SUB0_TRAC,
SUB12_TRAC,
TOTRET_TRAC AS TOT_RET_TRAC,
TOTPAG_TRAC AS TOT_PAG_TRAC,
NULL AS FECHA_AUTORIZACION_REL,
CASE WHEN secuencialFacturaRet <> '' THEN 'retencion-venta' ELSE NULL
END AS TIP_DOC_REL,
NULL AS FK_AUDITTR,
fecha AS FECHA_REG,
NULL AS PUNTO_EMISION_REC,
fechaAnulacion AS FECHA_ANULACION,
NULL AS FK_AUDIT_REL,
secuencialFacturaRet AS NUM_REL_DOC,
detalle_anticipos.ID_DET_ANT,
SECU_DET_ANT,
FDP_DET_ANT,
OBS_DET_ANT,
IMPOR_DET_ANT,
m.ID_MOVI
FROM
    transacciones
LEFT JOIN detalle_anticipos ON detalle_anticipos.FK_COD_TRAC = transacciones.COD_TRAC
LEFT JOIN clientes on clientes.COD_CLI = transacciones.FK_COD_CLI
LEFT JOIN movimientos AS m
ON
    m.FK_TRAC_MOVI = transacciones.COD_TRAC AND m.CONCEP_MOVI LIKE '%RETENCION%'
WHERE
    transacciones.TIP_TRAC IN(
        'Electronica',
        'Fisica',
        'comprobante-ingreso'
    ) AND secuencialFacturaRet <> '' AND (
                (detalle_anticipos.ID_DET_ANT IS NOT NULL AND detalle_anticipos.ID_DET_ANT <> '')
             OR (m.ID_MOVI    IS NOT NULL AND m.ID_MOVI    <> '')
          )
ORDER BY
    COD_TRAC
DESC;`);

    // AND (detalle_anticipos.ID_DET_ANT IS  NULL or detalle_anticipos.ID_DET_ANT = '') and  (m.ID_MOVI    IS  NULL or m.ID_MOVI = '')

    const ventas = rows as any[];
    const mapRetAuditSales: Record<number, number> = {};
    let movAudit = [];


    if (!ventas.length) {

        return { mapRetMovements, mapRetAuditSales, movAudit };
    }

    //Ultima secuencia de auditoria
    const secuencia_query = `SELECT IFNULL(MAX(CODIGO_AUT+0)+1,1) as codigoAuditoria FROM audit WHERE FK_COD_EMP=?;`;
    const [dataSecuencia] = await conn.query(secuencia_query, [newCompanyId]);
    let codigoAuditoria = dataSecuencia[0]['codigoAuditoria'];


    const BATCH_SIZE = 1000;
    /*  const mapSales: Record<number, number> = {}; */




    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {
            // Preparar inserción en lote para la tabla `audit`
            const auditValues: any[] = [];
            for (let j = 0; j < batch.length; j++) {
                auditValues.push([codigoAuditoria, 'RETENCION-VENTA', newCompanyId]);
                codigoAuditoria++;
            }
            const [auditRes]: any = await conn.query(`INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`, [auditValues]);

            // El insertId corresponde al primer id insertado; mapearlos por orden
            let firstAuditInsertId = auditRes.insertId;
            for (let j = 0; j < batch.length; j++) {
                const codTrans = batch[j].COD_TRAC;
                const auditId = firstAuditInsertId + j;
                mapRetAuditSales[codTrans] = auditId;
            } //console.log(batch);
            // Preparar valores para INSERT en batch
            const values = batch.map((t, index) => {
                console.log(`transformando y normalizando ${t.NUM_TRANS}`);
                const idDocumento = mapSales[t.COD_TRAC];
                const auditId = mapRetAuditSales[t.COD_TRAC];
                /*  return [
                     auditId,
                     idDocumento
                 ]; */
                return { auditId, idDocumento };
            });
            // Insertar clientes en batch

            await Promise.all(values.map(v =>
                conn.query(
                    `UPDATE transactions SET FK_AUDIT_REL = ? WHERE COD_TRANS = ?`,
                    [v.auditId, v.idDocumento]
                )
            ));

            /*  const [res]: any = await conn.query(`UPDATE transactions SET FK_AUDIT_REL = ?  WHERE COD_TRANS = ?`, [values]); */
            console.log(` -> Batch migrado: ${batch.length} retenciones de ventas.`);
        } catch (err) {
            throw err;
        }
    }

    const [[{ nextSecu }]]: any = await conn.query(
        `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS nextSecu FROM movements WHERE MODULO = 'RETENCION-VENTA' AND FK_COD_EMP = ?`,
        [newCompanyId]
    );
    let secuenciaMovimiento = nextSecu;

    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {


            // Preparar valores para INSERT en batch
            const values = batch.map((o, index) => {
                console.log(`transformando y normalizando movmientos ${o.NUM_REL_DOC}`);
                // Lógica de Negocio
                let idPlanCuenta = null;
                const currentAuditId = mapRetAuditSales[o.COD_TRAC];

                movAudit.push({
                    idAuditoria: currentAuditId,
                    idDocumento: mapSales[o.COD_TRAC],
                    idMovimiento: null
                });

                return [
                    bankMap[o.FKBANCO] ?? null,
                    mapSales[o.COD_TRAC] ?? null,
                    mapConciliation[o.FK_CONCILIADO] ?? null,
                    userMap[o.FK_USER] ?? null,
                    o.FECHA_MOVI,
                    o.FECHA_MANUAL,
                    o.TIP_MOVI,
                    o.ORIGEN_MOVI,
                    o.TIPO_MOVI,
                    o.REF_MOVI,
                    o.CONCEP_MOVI,
                    o.NUM_VOUCHER,
                    o.NUM_LOTE,
                    o.CAUSA_MOVI,
                    o.MODULO,
                    secuenciaMovimiento++,
                    o.IMPOR_MOVI,
                    o.ESTADO_MOVI,
                    o.PER_BENE_MOVI,
                    o.CONCILIATED ?? null,
                    newCompanyId,
                    boxMap[o.IDCAJA] ?? null,
                    o.OBS_MOVI,
                    o.IMPOR_MOVITOTAL,
                    null, // FK_ASIENTO
                    currentAuditId,
                    null, // FK_ARQUEO
                    null,
                    null, // RECIBO_CAJA
                    idPlanCuenta,
                    null, // NUM_UNIDAD
                    '[]'  // JSON_PAGOS
                ];

            });
            // Insertar clientes en batch
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
                [values]
            );

            // Actualizar mapeo de movimientos
            let currentMovId = resMov.insertId;
            batch.forEach(o => {
                let idMov = currentMovId++;
                mapRetMovements[o.COD_TRAC] = idMov;
                //Actualizar id de movimiento en el arreglo de auditoria
                const movAud = movAudit.find(ma => ma.idDocumento === mapSales[o.COD_TRAC]);
                if (movAud) {
                    movAud.idMovimiento = idMov;
                }
            });



            console.log(` -> Batch migrado: ${batch.length} retenciones de ventas.`);


        } catch (err) {
            throw err;
        }
    }

    const recalcular = true;
    await detRetSale(
        conn,
        ventas,
        mapRetAuditSales,
        mapRetentions,
        mapRetMovements,
        mapSales,
        newCompanyId,
        recalcular
    )


    const mapDetailCredit = await migrateRetentionsCredit(
        legacyConn,
        conn,
        newCompanyId,
        userMap,
        bankMap,
        boxMap,
        mapConciliation,
        mapSales,
        mapRetMovements,
        mapRetAuditSales,
        mapRetentions
    );


    const mapMigrateDetail = await migratePaymentDetails(
        legacyConn,
        conn,
        mapObligationsCustomers,
        mapDetailCredit.mapRetMovements
    );

    const mapEntryAccount = await migrateAccountingEntriesCustomerObligations(
        legacyConn,
        conn,
        newCompanyId,
        mapDetailCredit.mapRetMovements,
        mapPeriodo,
        mapRetAuditSales
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
    //mapRetAuditSales
    return { mapRetMovements, mapRetAuditSales, movAudit };
}


export async function migrateRetentionsCredit(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    userMap: any,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
    mapSales: Record<number, number | null>,
    mapRetMovements: Record<number, number | null>,
    mapRetAuditSales: Record<number, number | null>,
    mapRetentions: Record<number, number | null>,
): Promise<{ mapRetMovements: Record<number, number>; mapRetAuditSales: Record<number, number> }> {
    console.log("Migrando retenciones en ventas por credito...");
    /*  const mapRetMovements: Record<number, number> = {}; */
    const [rows] = await legacyConn.query(`SELECT
    COD_TRAC,
    COD_TRAC AS FK_COD_TRAN,
    m.FK_COD_BANCO_MOVI AS FKBANCO,
    m.FK_CONCILIADO AS FK_CONCILIADO,
    IFNULL(
        m.FK_USER_EMP_MOVI,
        transacciones.FK_COD_USU
    ) AS FK_USER,
    IFNULL(
        m.FECHA_MOVI,
        transacciones.fecha
    ) AS FECHA_MOVI,
    IFNULL(
        FECHA_MANUAL,
        transacciones.FEC_TRAC
    ) FECHA_MANUAL,
    CASE WHEN COALESCE(ID_DET_ANT, '') <> '' THEN 'ANTICIPO' WHEN m.TIP_MOVI IS NULL THEN 'RET-VENTA' ELSE m.TIP_MOVI
END AS TIP_MOVI,
'RET-VENTA' AS ORIGEN_MOVI,
CASE WHEN COALESCE(ID_DET_ANT, '') <> '' THEN 'ANTICIPO' WHEN m.TIP_MOVI IS NULL THEN 'RET-VENTA' WHEN m.TIP_MOVI = 'BANCOS' THEN 'DEVBANCO' WHEN m.TIP_MOVI = 'CAJAS' THEN 'DEVCAJA' ELSE m.TIP_MOVI
END AS TIPO_MOVI,
CONCAT(
    'RETENCION DE VENTA N°: ',
    IFNULL(NULLIF(secuencialFacturaRet, ''), 'S/N')
) AS REF_MOVI
,
IFNULL(CONCEP_MOVI, secuencialFacturaRet) AS CONCEP_MOVI,
NULL AS NUM_VOUCHER,
NULL AS NUM_LOTE,
IFNULL(CAUSA_MOVI, 'INGRESO') AS CAUSA_MOVI,
'RETENCION-VENTA' as MODULO,NULL AS SECU_MOVI, 
transacciones.TOTPAG_TRAC AS IMPOR_MOVI,
 1 as ESTADO_MOVI, 
   IFNULL(m.PER_BENE_MOVI,  clientes.NOM_CLI) AS PER_BENE_MOVI,
NULL AS FK_COD_EMP,
 m.FK_CONCILIADO, m.CONCILIADO,
FK_COD_CAJAS_MOVI AS IDCAJA,
cabecera_compra AS DOCUMENT_REL_DETAIL,
CONCAT(
    'RETENCION DE VENTA N°: ',
    IFNULL(NULLIF(secuencialFacturaRet, ''), 'S/N')
) AS OBS_MOVI,

transacciones.TOTPAG_TRAC AS IMPOR_MOVITOTAL,
NULL AS FK_ASIENTO,
NULL AS FK_AUDITMV,NULL AS FK_ARQUEO,NULL AS ID_TARJETA, NULL AS RECIBO_CAJA,NULL AS FK_CTAM_PLAN,NULL AS NUMERO_UNIDAD,NULL AS JSON_PAGOS,
SUBSTRING(secuencialFacturaRet, 9, 9) AS SECUENCIA_REL_DOC,
transacciones.claveFacturaRet AS CLAVE_REL_TRANS,
YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
MONTH(FEC_TRAC) AS FEC_MES_TRAC,
FEC_TRAC AS FEC_TRAC,
fechaFacturaRet AS FEC_REL_TRAC,
FEC_MERC_TRAC AS FEC_MERC_TRAC,
METPAG_TRAC,
OBS_TRAC,
FK_USU_CAJA AS FK_USER_VEND,
FK_COD_CLI AS FK_PERSON,
estado_compra AS ESTADO_REL,
SUB_TRAC AS SUB_TRAC,
IVA_TRAC,
SUB0_TRAC,
SUB12_TRAC,
TOTRET_TRAC AS TOT_RET_TRAC,
TOTPAG_TRAC AS TOT_PAG_TRAC,
NULL AS FECHA_AUTORIZACION_REL,
CASE WHEN secuencialFacturaRet <> '' THEN 'retencion-venta' ELSE NULL
END AS TIP_DOC_REL,
NULL AS FK_AUDITTR,
fecha AS FECHA_REG,
NULL AS PUNTO_EMISION_REC,
fechaAnulacion AS FECHA_ANULACION,
NULL AS FK_AUDIT_REL,
secuencialFacturaRet AS NUM_REL_DOC,
detalle_anticipos.ID_DET_ANT,
SECU_DET_ANT,
FDP_DET_ANT,
OBS_DET_ANT,
IMPOR_DET_ANT,
m.ID_MOVI,
CONCAT(
    'RETENCION DE VENTA N°: ',
    IFNULL(NULLIF(secuencialFacturaRet, ''), 'S/N')
) AS CONCEP_MOVI

FROM
    transacciones
LEFT JOIN detalle_anticipos ON detalle_anticipos.FK_COD_TRAC = transacciones.COD_TRAC
LEFT JOIN movimientos AS m
ON
    m.FK_TRAC_MOVI = transacciones.COD_TRAC AND m.CONCEP_MOVI LIKE '%RETENCION%'
    LEFT JOIN clientes on clientes.COD_CLI = transacciones.FK_COD_CLI
WHERE
    transacciones.TIP_TRAC IN(
        'Electronica',
        'Fisica',
        'comprobante-ingreso'
    ) AND secuencialFacturaRet <> '' AND (detalle_anticipos.ID_DET_ANT IS  NULL or detalle_anticipos.ID_DET_ANT = '') and  (m.ID_MOVI IS  NULL or m.ID_MOVI = '')
ORDER BY
    COD_TRAC
DESC;`);


    const ventas = rows as any[];

    if (!ventas.length) {
        return { mapRetMovements, mapRetAuditSales };
    }

    //Ultima secuencia de auditoria
    const secuencia_query = `SELECT IFNULL(MAX(CODIGO_AUT+0)+1,1) as codigoAuditoria FROM audit WHERE FK_COD_EMP=?;`;
    const [dataSecuencia] = await conn.query(secuencia_query, [newCompanyId]);
    let codigoAuditoria = dataSecuencia[0]['codigoAuditoria'];

    const BATCH_SIZE = 1000;


    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {
            // Preparar inserción en lote para la tabla `audit`
            const auditValues: any[] = [];
            for (let j = 0; j < batch.length; j++) {
                auditValues.push([codigoAuditoria, 'RETENCION-VENTA', newCompanyId]);
                codigoAuditoria++;
            }

            const [auditRes]: any = await conn.query(`INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`, [auditValues]);

            // El insertId corresponde al primer id insertado; mapearlos por orden
            let firstAuditInsertId = auditRes.insertId;
            for (let j = 0; j < batch.length; j++) {
                const codTrans = batch[j].COD_TRAC;
                const auditId = firstAuditInsertId + j;
                mapRetAuditSales[codTrans] = auditId;
            }
            // Preparar valores para INSERT en batch
            const values = batch.map((t, index) => {
                console.log(`transformando y normalizando ${t.NUM_TRANS}`);
                const idDocumento = mapSales[t.COD_TRAC];
                const auditId = mapRetAuditSales[t.COD_TRAC];
                /*  return [
                     auditId,
                     idDocumento
                 ]; */
                return { auditId, idDocumento };
            });
            // Insertar clientes en batch

            await Promise.all(values.map(v =>
                conn.query(
                    `UPDATE transactions SET FK_AUDIT_REL = ? WHERE COD_TRANS = ?`,
                    [v.auditId, v.idDocumento]
                )
            ));

            /*  const [res]: any = await conn.query(`UPDATE transactions SET FK_AUDIT_REL = ?  WHERE COD_TRANS = ?`, [values]); */
            console.log(` -> Batch migrado: ${batch.length} retenciones de ventas.`);
        } catch (err) {
            throw err;
        }
    }

    const [[{ nextSecu }]]: any = await conn.query(
        `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS nextSecu FROM movements WHERE MODULO = 'RETENCION-VENTA' AND FK_COD_EMP = ?`,
        [newCompanyId]
    );
    let secuenciaMovimiento = nextSecu;

    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {

            // Preparar valores para INSERT en batch
            const values = batch.map((o, index) => {
                console.log(`transformando y normalizando movmientos de retenciones a credito ${o.NUM_REL_DOC}`);
                // Lógica de Negocio
                let idPlanCuenta = null;
                const currentAuditId = mapRetAuditSales[o.COD_TRAC];



                return [
                    bankMap[o.FKBANCO] ?? null,
                    mapSales[o.COD_TRAC] ?? null,
                    mapConciliation[o.FK_CONCILIADO] ?? null,
                    userMap[o.FK_USER] ?? null,
                    o.FECHA_MOVI,
                    o.FECHA_MANUAL,
                    o.TIP_MOVI,
                    o.ORIGEN_MOVI,
                    o.TIPO_MOVI,
                    o.REF_MOVI,
                    o.CONCEP_MOVI,
                    o.NUM_VOUCHER,
                    o.NUM_LOTE,
                    o.CAUSA_MOVI,
                    o.MODULO,
                    secuenciaMovimiento++,
                    o.IMPOR_MOVI,
                    o.ESTADO_MOVI,
                    o.PER_BENE_MOVI,
                    o.CONCILIATED ?? null,
                    newCompanyId,
                    boxMap[o.IDCAJA] ?? null,
                    o.OBS_MOVI,
                    o.IMPOR_MOVITOTAL,
                    null, // FK_ASIENTO
                    currentAuditId,
                    null, // FK_ARQUEO
                    null,
                    null, // RECIBO_CAJA
                    idPlanCuenta,
                    null, // NUM_UNIDAD
                    '[]'  // JSON_PAGOS
                ];

            });
            // Insertar clientes en batch
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
                [values]
            );

            // Actualizar mapeo de movimientos
            let currentMovId = resMov.insertId;
            batch.forEach(o => {
                mapRetMovements[o.COD_TRAC] = currentMovId++;
            });
            console.log(` -> Batch migrado: ${batch.length} retenciones de ventas por credito.`);
            /*   return { mapRetMovements, mapRetAuditSales };
   */
        } catch (err) {
            /* return { mapRetMovements, mapRetAuditSales }; */
            throw err;
        }
    }
    const recalcular = true;
    await detRetSale(
        conn,
        ventas,
        mapRetAuditSales,
        mapRetentions,
        mapRetMovements,
        mapSales,
        newCompanyId,
        recalcular
    )




    return { mapRetMovements, mapRetAuditSales };
}

export async function migratePaymentDetails(
    legacyConn: any,
    conn: any,
    mapObligationsCustomers: Record<number, number | null>,
    mapRetMovements: Record<number, number | null>,
): Promise<{

    mapDetailObligationsAplicate: Record<number, number>
}> {
    console.log("🚀 Migrando movimientos  por retenciones");


    const mapDetailObligationsAplicate: Record<number, number> = {};

    try {

        const [rows]: any[] = await legacyConn.query(`SELECT
                                                            detalles_cuentas.fk_cod_cuenta,
                                                            detalles_cuentas.FK_COD_GD,
                                                            detalles_cuentas.fecha,
                                                            detalles_cuentas.importe,
                                                            detalles_cuentas.saldo,
                                                            detalles_cuentas.nuevo_saldo,
                                                            transacciones.COD_TRAC, 
                                                            transacciones.TIP_TRAC, 
                                                            detalles_cuentas.observacion_cp, 
                                                            detalles_cuentas.forma_pago_cp
                                                        FROM
                                                            cuentascp
                                                        INNER JOIN transacciones ON transacciones.COD_TRAC = cuentascp.FK_TRAC_CUENTA    
                                                        INNER JOIN detalles_cuentas ON cuentascp.cod_cp = detalles_cuentas.fk_cod_cuenta
                                                        INNER JOIN grupo_detalles_t ON detalles_cuentas.FK_COD_GD = grupo_detalles_t.ID_GD
                                                        LEFT JOIN movimientos ON movimientos.FK_COD_CX = grupo_detalles_t.ID_GD
                                                        WHERE
                                                            cuentascp.Tipo_cxp = 'CXC' AND detalles_cuentas.forma_pago_cp  IN ('16')
                                                        ORDER BY
                                                            cod_detalle
                                                        DESC;`);


        if (!rows.length) return { mapDetailObligationsAplicate };

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const movementValues = batch.map((o, index) => {
                const idCuenta = mapObligationsCustomers[o.fk_cod_cuenta];
                const idMovimiento = mapRetMovements[o.COD_TRAC];

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

            await Promise.all(
                batch.map(o => {
                    const idMovimiento = mapRetMovements[o.COD_TRAC];
                    return conn.query(
                        `UPDATE movements SET IMPOR_MOVI = ?, IMPOR_MOVITOTAL = ? WHERE ID_MOVI = ?`,
                        [o.importe, o.importe, idMovimiento]
                    );
                })
            );


        }







        console.log("✅ Migración completada de pagos en retenciones");
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
    mapRetMovements: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapRetAuditSales: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {
    console.log("🚀 Migrando encabezado de asiento contables retenciones..........");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const [rows]: any[] = await legacyConn.query(`SELECT  cod_asiento,
                                                        fecha_asiento AS FECHA_ASI,
                                                        descripcion_asiento AS DESCRIP_ASI,
                                                        numero_asiento AS NUM_ASI,
                                                        'RET-VENTA' AS ORG_ASI,
                                                        debe_asiento AS TDEBE_ASI,
                                                        haber_asiento AS THABER_ASI,
                                                        'RETENCION-VENTA' AS TIP_ASI,
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
                                                        CAST(REGEXP_REPLACE(RIGHT(numero_asiento, 9), '[^0-9]', '') AS UNSIGNED)  AS SEC_ASI,
                                                        cod_origen,
                                                        NULL AS FK_MOV FROM contabilidad_asientos inner join transacciones on  transacciones.COD_TRAC= contabilidad_asientos.FK_CODTRAC  WHERE secuencialFacturaRet IS NOT NULL and secuencialFacturaRet<>'' and TIP_TRAC in ('Electronica','Fisica') and descripcion_asiento like '%RETENCION%';` );

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
                const idAuditTr = mapRetAuditSales[o.COD_TRAC];
                const idMovimiento = mapRetMovements[o.COD_TRAC];

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
    secuencialFacturaRet IS NOT NULL AND secuencialFacturaRet <> '' AND TIP_TRAC IN('Electronica', 'Fisica') AND descripcion_asiento LIKE '%RETENCION%';`);

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

export async function detRetSale(
    conn,
    ventas,
    mapRetAuditSales,
    mapRetentions,
    mapRetMovements,
    mapSales,
    newCompanyId,
    recalcular
) {
    const obtenerSaldos = (estructuraAntigua: any): { saldoRenta: number; saldoIva: number } => {
        try {
            let datos = estructuraAntigua;

            if (typeof estructuraAntigua === 'string') {
                try {
                    datos = JSON.parse(estructuraAntigua);
                } catch (parseError) {
                    console.error('Error al parsear JSON de retencion:', parseError);
                    return { saldoRenta: 0, saldoIva: 0 };
                }
            }
            const datosOriginales = Array.isArray(datos) ? datos[0] : datos;
            if (!datosOriginales || typeof datosOriginales !== 'object') {
                console.warn('Estructura de retención vacía o inválida');
                return { saldoRenta: 0, saldoIva: 0 };
            }

            const saldoRenta = (datosOriginales.listadoRetenciones || [])
                .filter((ret: any) => ret && ret.renta)
                .reduce((acc: number, ret: any) => {
                    const val = Number(ret.valorRetenido) || 0;
                    return acc + val;
                }, 0);

            const saldoIva = (datosOriginales.listadoRetencionesIva || [])
                .filter((ret: any) => ret && ret.rentaIva)
                .reduce((acc: number, ret: any) => {
                    const val = Number(ret.valorRetenidoIva) || 0;
                    return acc + val;
                }, 0);

            return {
                saldoRenta: Number(saldoRenta.toFixed(2)),
                saldoIva: Number(saldoIva.toFixed(2)),
            };

        } catch (error) {
            console.error('Error en obtenerSaldos:', error);
            return { saldoRenta: 0, saldoIva: 0 };
        }
    };

    const formatDecimal = (value: any, decimals: number = 2): string => {
        const num = typeof value === 'number' ? value : parseFloat(value || 0);
        return isNaN(num) ? '0.00' : num.toFixed(decimals);
    };

    const parseInteger = (value: any): number | null => {
        const num = parseInt(value, 10);
        return isNaN(num) ? null : num;
    };

    const adaptarEstructuraMultiple = (estructuraAntigua: any): any[] => {
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
            }

            // Normalizar: si viene como array, tomar el primer elemento; si es objeto, usarlo directamente
            const datosOriginales = Array.isArray(datos) ? datos[0] : datos;

            if (!datosOriginales || typeof datosOriginales !== 'object') {
                console.warn('Estructura de retención vacía o inválida');
                return [];
            }

            // Mapear retenciones por renta (Retención en la Fuente)
            const nuevasRetencionesRenta = (datosOriginales.listadoRetenciones || [])
                .filter((ret: any) => ret && ret.renta) // Filtrar nulos/undefined y renta vacío
                .map((ret: any) => ({
                    codigoRenta: ret.renta || null,
                    idRetRenta: mapRetentions[ret.idRetencionRenta],
                    nombreRenta: ret.nombreRetencionFuente || null,
                    porcentajeRenta: formatDecimal(ret.porcentaje),
                    subtotalBase0: formatDecimal(ret.subtotalBase0),
                    subtotalDiferente: formatDecimal(ret.subtotalDiferente),
                    valorRetenido: formatDecimal(ret.valorRetenido)
                }));

            // Mapear retenciones por IVA
            const nuevasRetencionesIva = (datosOriginales.listadoRetencionesIva || [])
                .filter((ret: any) => ret && ret.rentaIva)
                .map((ret: any) => {
                    // Filtrar impuestos activos con tarifa > 0
                    const impuestosActivos = (ret.arraryImpuestos || [])
                        .filter((imp: any) => imp && imp.impuestoActivoIva === 1 && parseFloat(imp.tarifa) > 0)
                        .map((imp: any) => ({
                            codigo: parseInteger(imp.codigo),
                            tarifa: parseInteger(imp.tarifa),
                            total: formatDecimal(imp.total)
                        }));

                    return {
                        codigoIva: ret.rentaIva || null,
                        idRetIva: mapRetentions[ret.idRetencionIva],
                        nombreIva: ret.nombreRetencionIva || null,
                        porcentajeIva: formatDecimal(ret.porcentajeIva),
                        subtotalDiferenteIva: formatDecimal(ret.subtotalDiferenteIva),
                        valorRetenido: formatDecimal(ret.valorRetenidoIva),
                        impuestos: impuestosActivos
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



    const BATCH_SIZE = 1000;
    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {
            const values = batch.map((o, index) => {
                console.log(`transformando y normalizando movmientos ${o.NUM_REL_DOC}`);
                // Lógica de Negocio
                const currentAuditId = mapRetAuditSales[o.COD_TRAC];
                const retencionVentaNueva = adaptarEstructuraMultiple(o.DOCUMENT_REL_DETAIL);


                if (recalcular == true) {
                    const { saldoRenta, saldoIva } = obtenerSaldos(o.DOCUMENT_REL_DETAIL);
                    const totalRetenido = Number((saldoRenta + saldoIva).toFixed(2));
                    console.log("Total retenido:" + totalRetenido);
                    o.IMPOR_MOVI = Number(totalRetenido);
                }
                const totalNeto = Number(o.TOT_PAG_TRAC) - Number(o.IMPOR_MOVI);
                const idMov = mapRetMovements[o.COD_TRAC]
                return [
                    o.FECHA_MOVI,
                    o.SUB12_TRAC,
                    o.SUB0_TRAC,
                    o.IVA_TRAC,
                    o.IMPOR_MOVI,
                    totalNeto,
                    JSON.stringify(retencionVentaNueva),
                    null,
                    null,
                    mapSales[o.COD_TRAC] ?? null,
                    currentAuditId,
                    idMov,
                    newCompanyId,
                    o.NUM_REL_DOC,
                    o.CLAVE_REL_TRANS
                ];
            });

            const [resMov]: any = await conn.query(
                `INSERT INTO card_holds(FECREG_RETENCION, SUBDIFERENTE_TARJETA, SUBCERO_TARJETA, IVA_TARJETA, TOTAL_TARJETA, TOTALNETO_TARJETA, TARJETA_DETAIL, 
                FK_TARJETA, FK_BANKRET, FK_CODTRAN, FK_AUDITMV, FK_MOV, FK_COD_EMP, NUMERO_TARJETA, 
                CLAVE_TARJETA) VALUES ?`,
                [values]
            );

        } catch (err) {
            throw err;
        }
    } console.log("Migrar detalle de retenciones :" + ventas.length);
}
