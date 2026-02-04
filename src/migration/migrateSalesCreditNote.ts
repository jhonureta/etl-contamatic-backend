import { upsertTotaledEntry } from "./migrationTools";
import { normalizarProducto } from "./normalizador";

export interface oldDetailAcountCodeMap {
    id: number;
    valor: number;
}

export type ProductoNuevo = {
    idProducto: number | null;
    idBodega: number | null;
    codigo: string | null;
    nombre: string | null;
    observacion: string | null;
    cantidad: string | number | null;
    total: number | null;
    impuesto: number | null;
    codigoImpuesto: number | null;
    nombreImpuesto: string | null;
    cantidadAnterior: string | number | null;
    precioProducto: string | null;
    porcentajeDescuento: string | number | null;
    valorDescuento: string | number | null;
    precios: {
        name: string;
        price: string;
        discount: number;
        select: boolean;
    }[] | null;
    tota: number | null;
    tarifaice: string | number | null;
};

export async function migrateCreditNote(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    branchMap: any,
    userMap: any,
    mapClients: any,
    mapProducts: any,
    oldProductCodeMap: any,
    mapAuditSales: any,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
    mapObligationsCustomers: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    storeMap: Record<number, number>
): Promise<{ mapCreditNote: Record<number, number>; mapAuditCreditNote: Record<number, number> }> {
    console.log("Migrando notas de credito...");

    const mapCreditNote: Record<number, number> = {};
    const mapAuditCreditNote: Record<number, number> = {};

    const [rows] = await legacyConn.query(`SELECT
tn.TIP_TRAC as fact,
    tf.COD_TRAC AS COD_TRANS,
    tf.NUM_TRACNOT,
    tf.NUM_TRACNOT AS NUM_TRANS,
    SUBSTRING(tf.NUM_TRACNOT, 1, 7) AS PUNTO_EMISION_DOC,
    SUBSTRING(tf.NUM_TRACNOT, 9, 9) AS SECUENCIA_DOC,
    SUBSTRING(tf.NUMFACNOT_TRAC, 9, 9) AS SECUENCIA_REL_DOC,
    tf.claveFactura AS CLAVE_TRANS,
    tn.claveFactura AS CLAVE_REL_TRANS,
    CASE WHEN tf.tipo_nota = 'descuento' THEN 'descuento' ELSE 'devolucion'
END AS TIP_DET_DOC,
YEAR(tf.FEC_TRAC) AS FEC_PERIODO_TRAC,
MONTH(tf.FEC_TRAC) AS FEC_MES_TRAC,
tf.FEC_TRAC AS FEC_TRAC,
tn.FEC_TRAC AS FEC_REL_TRAC,
tf.FEC_MERC_TRAC AS FEC_MERC_TRAC,
CASE WHEN tf.METPAG_TRAC = 'MULTIPLES' THEN tf.METPAG_TRAC ELSE tf.METPAG_TRAC
END AS MET_PAG_TRAC,
tf.OBS_TRAC,
tf.FK_COD_USU AS FK_USER,
tf.FK_COD_USU AS FK_USER_VEND,
tf.FK_COD_CLI AS FK_PERSON,
tf.estado AS ESTADO,
tf.estado_compra AS ESTADO_REL,
tf.documentoAnulado AS RUTA_DOC_ANULADO,
tf.SUB_BASE_5 AS SUB_BASE_5,
tf.SUB_BASE_8 AS SUB_BASE_8,
tf.SUB_BASE_12 AS SUB_BASE_12,
tf.SUB_BASE_13 AS SUB_BASE_13,
tf.SUB_BASE_14 AS SUB_BASE_14,
tf.SUB_BASE_15 AS SUB_BASE_15,
tf.SUB12_TRAC AS SUB_12_TRAC,
tf.SUB0_TRAC AS SUB_0_TRAC,
tf.SUBNOBJETO_TRAC AS SUB_N_OBJETO_TRAC,
tf.SUBEXENTO_TRAC AS SUB_EXENTO_TRAC,
tf.SUB_TRAC AS SUB_TRAC,
tf.IVA_TRAC AS IVA_TRAC,
tf.IVA_TRAC,
tf.TOT_TRAC AS TOT_TRAC,
tf.TOTRET_TRAC AS TOT_RET_TRAC,
tf.TOTPAG_TRAC AS TOT_PAG_TRAC,
tf.PROPINA_TRAC,
tf.OTRA_PER,
'04' AS COD_COMPROBANTE,
'01' AS COD_COMPROBANTE_REL,
NULL AS COD_DOCSUS_TRIB,
NULL AS FK_COD_EMP,
tf.firmado AS FRIMADO,
tf.enviado AS ENVIADO,
tf.autorizado AS AUTORIZADO,
tf.enviadoCliente AS ENVIADO_CLIEMAIL,
tf.fechaAutorizado AS FECHA_AUTORIZACION,
NULL AS FECHA_AUTORIZACION_REL,
CASE WHEN tn.TIP_TRAC = 'Electronica' THEN 'factura' ELSE 'fisica'
END AS TIP_DOC_REL,
tf.DES_TRAC AS DSTO_TRAC,
NULL AS FK_CODSUCURSAL,
NULL AS FK_AUDITTR,
tf.TIP_TRAC TIP_TRAC,
tf.fecha AS FECHA_REG,
tf.DET_TRAC AS DOCUMENT_DETAIL,
NULL AS PUNTO_EMISION_REC,
tf.fechaAnulacion AS FECHA_ANULACION,
tf.TIP_TRAC AS TIP_DOC,
NULL AS SRI_PAY_CODE,
tf.IP_CLIENTE AS CLIENT_IP,
NULL AS FK_AUDIT_REL,
tf.NUMFACNOT_TRAC AS NUM_REL_DOC,
tf.serial_producto AS DIV_PAY_YEAR,
tf.cabecera_compra AS DOCUMENT_REL_DETAIL,
tf.RESPU_SRI AS RESP_SRI,
tf.INFO_ADIC AS INFO_ADIC,
tf.DET_REMBOLSO AS DET_EXP_REEMBOLSO,
tf.METPAG_JSON_TRAC AS JSON_METODO,
tf.INFO_ADIC AS ITEMS_PROF,
tf.OBS_AUXILIAR AS OBS_AUXILIAR,
tf.OBS_ORD AS OBS_ORDEN,
tf.COD_TRAC_FACT
FROM
    transacciones tf
INNER JOIN transacciones tn ON
    tf.COD_TRAC_FACT = tn.COD_TRAC
WHERE
    tf.TIP_TRAC IN('nota')
ORDER BY
    tf.COD_TRAC
DESC;`);
    const ventas = rows as any[];

    if (!ventas.length) {
        return { mapCreditNote, mapAuditCreditNote };
    }

    //Ultima secuencia de auditoria
    const secuenciaSucursal = `SELECT
    COD_SURC,
    SUBSTRING(secuencial, 1, 7) AS ELECTRONICA,
    SUBSTRING(secuencialFisica, 1, 7) AS FISICA,
    SUBSTRING(SURC_SEC_COMPINGR, 1, 7) AS COMPINGRESO
    FROM
    sucursales;`;
    const [dataSucursal] = await legacyConn.query(secuenciaSucursal, [newCompanyId]);

    const listElectronica = [];
    const listFisica = [];
    const listComprobante = [];

    for (let x = 0; x < dataSucursal.length; x++) {

        listElectronica[dataSucursal[x].ELECTRONICA] = dataSucursal[x].COD_SURC;
        listFisica[dataSucursal[x].FISICA] = dataSucursal[x].COD_SURC;
        listComprobante[dataSucursal[x].COMPINGRESO] = dataSucursal[x].COD_SURC;
    }

    const secuencia_query = `SELECT IFNULL(MAX(CODIGO_AUT+0)+1,1) as codigoAuditoria FROM audit WHERE FK_COD_EMP=?;`;
    const [dataSecuencia] = await conn.query(secuencia_query, [newCompanyId]);
    let codigoAuditoria = dataSecuencia[0]['codigoAuditoria'];


    const BATCH_SIZE = 1000;
    const mapSalesNoteBound: Record<number, number> = {};

    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {
            // Inicio de transacción por batch: agrupar audit + insert customers

            // Preparar inserción en lote para la tabla `audit`
            const auditValues: any[] = [];
            for (let j = 0; j < batch.length; j++) {
                auditValues.push([codigoAuditoria, 'NCVENTA', newCompanyId]);
                codigoAuditoria++;
            }

            const [auditRes]: any = await conn.query(
                `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
                [auditValues]
            );

            // El insertId corresponde al primer id insertado; mapearlos por orden
            let firstAuditInsertId = auditRes.insertId;
            for (let j = 0; j < batch.length; j++) {
                const codTrans = batch[j].COD_TRANS;
                const auditId = firstAuditInsertId + j;
                mapAuditCreditNote[codTrans] = auditId;
            }




            // Preparar valores para INSERT en batch
            const values = batch.map((t, index) => {
                console.log(`transformando y normalizando ${t.NUM_TRACNOT}`);
                let productos = t.DOCUMENT_DETAIL ?? [];
                if (typeof t.DOCUMENT_DETAIL === 'string') {
                    try {
                        productos = JSON.parse(t.DOCUMENT_DETAIL);
                    } catch (e) {
                        console.error("Error al parsear JSON:", e);
                        return [];
                    }
                }
                let idSucursal = null;

                if (t.TIP_DOC_REL == 'fisica') {
                    idSucursal = branchMap[listFisica[t.PUNTO_EMISION_DOC]];
                }
                if (t.TIP_DOC_REL == 'factura') {
                    idSucursal = branchMap[listElectronica[t.PUNTO_EMISION_DOC]];
                }
                const productosNormalizados = productos.map(p => normalizarProducto(p, mapProducts, branchMap, oldProductCodeMap, idSucursal, storeMap));

                const auditId = mapAuditCreditNote[t.COD_TRANS];
                const vendedor = userMap[t.FK_USER_VEND];
                const creador = userMap[t.FK_USER];
                const cliente = mapClients[t.FK_PERSON];

                t.FK_AUDIT_REL = mapAuditSales[t.COD_TRAC_FACT] ?? null;




                function safeJson(input: any) {
                    try {
                        if (typeof input === "string") {
                            JSON.parse(input);        // verificar validez
                            return input;
                        }
                        return JSON.stringify(input ?? {});
                    } catch {
                        return "{}"; // fallback JSON válido
                    }
                }




                return [
                    t.PUNTO_EMISION_DOC,
                    t.SECUENCIA_DOC,
                    t.SECUENCIA_REL_DOC,
                    t.CLAVE_TRANS,
                    t.CLAVE_REL_TRANS,
                    t.TIP_DET_DOC,
                    t.FEC_PERIODO_TRAC,
                    t.FEC_MES_TRAC,
                    t.FEC_TRAC,
                    t.FEC_REL_TRAC,
                    t.FEC_MERC_TRAC,
                    t.MET_PAG_TRAC,
                    t.OBS_TRAC,
                    creador,
                    vendedor,
                    cliente,
                    t.ESTADO,
                    t.ESTADO_REL,
                    t.RUTA_DOC_ANULADO,
                    t.SUB_BASE_5,
                    t.SUB_BASE_8,
                    t.SUB_BASE_12,
                    t.SUB_BASE_13,
                    t.SUB_BASE_14,
                    t.SUB_BASE_15,
                    t.SUB_12_TRAC,
                    t.SUB_0_TRAC,
                    t.SUB_N_OBJETO_TRAC,
                    t.SUB_EXENTO_TRAC,
                    t.SUB_TRAC,
                    t.IVA_TRAC,
                    t.TOT_TRAC,
                    t.TOT_RET_TRAC,
                    t.TOT_PAG_TRAC,
                    t.PROPINA_TRAC ?? 0,
                    t.OTRA_PER,
                    t.COD_COMPROBANTE,
                    t.COD_COMPROBANTE_REL,
                    t.COD_DOCSUS_TRIB,
                    newCompanyId,
                    t.FRIMADO,
                    t.ENVIADO,
                    t.AUTORIZADO,
                    t.ENVIADO_CLIEMAIL,
                    t.FECHA_AUTORIZACION,
                    t.FECHA_AUTORIZACION_REL,
                    t.TIP_DOC_REL,
                    t.DSTO_TRAC,
                    idSucursal,
                    auditId,
                    t.TIP_TRAC,
                    t.FECHA_REG,
                    JSON.stringify(productosNormalizados),
                    t.PUNTO_EMISION_REC,
                    t.FECHA_ANULACION,
                    t.TIP_DOC,
                    t.SRI_PAY_CODE,
                    t.CLIENT_IP ?? '0.0.0.0',
                    t.FK_AUDIT_REL,
                    t.NUM_TRANS,
                    t.NUM_REL_DOC,
                    t.DIV_PAY_YEAR,
                    null,
                    t.RESP_SRI,
                    t.INFO_ADIC,
                    t.DET_EXP_REEMBOLSO,
                    safeJson(t.JSON_METODO),
                    t.ITEMS_PROF,
                    t.OBS_AUXILIAR,
                    t.OBS_ORDEN
                ];
            });

            // Insertar clientes en batch
            const [res]: any = await conn.query(`
                INSERT INTO transactions (PUNTO_EMISION_DOC,
                    SECUENCIA_DOC,
                    SECUENCIA_REL_DOC,
                    CLAVE_TRANS,
                    CLAVE_REL_TRANS,
                    TIP_DET_DOC,
                    FEC_PERIODO_TRAC,
                    FEC_MES_TRAC,
                    FEC_TRAC,
                    FEC_REL_TRAC,
                    FEC_MERC_TRAC,
                    MET_PAG_TRAC,
                    OBS_TRAC,
                    FK_USER,
                    FK_USER_VEND,
                    FK_PERSON,
                    ESTADO,
                    ESTADO_REL,
                    RUTA_DOC_ANULADO,
                    SUB_BASE_5,
                    SUB_BASE_8,
                    SUB_BASE_12,
                    SUB_BASE_13,
                    SUB_BASE_14,
                    SUB_BASE_15,
                    SUB_12_TRAC,
                    SUB_0_TRAC,
                    SUB_N_OBJETO_TRAC,
                    SUB_EXENTO_TRAC,
                    SUB_TRAC,
                    IVA_TRAC,
                    TOT_TRAC,
                    TOT_RET_TRAC,
                    TOT_PAG_TRAC,
                    PROPINA_TRAC,
                    OTRA_PER,
                    COD_COMPROBANTE,
                    COD_COMPROBANTE_REL,
                    COD_DOCSUS_TRIB,
                    FK_COD_EMP,
                    FRIMADO,
                    ENVIADO,
                    AUTORIZADO,
                    ENVIADO_CLIEMAIL,
                    FECHA_AUTORIZACION,
                    FECHA_AUTORIZACION_REL,
                    TIP_DOC_REL,
                    DSTO_TRAC,
                    FK_CODSUCURSAL,
                    FK_AUDITTR,
                    TIP_TRAC,
                    FECHA_REG,
                    DOCUMENT_DETAIL,
                    PUNTO_EMISION_REC,
                    FECHA_ANULACION,
                    TIP_DOC,
                    SRI_PAY_CODE,
                    CLIENT_IP,
                    FK_AUDIT_REL,
                    NUM_TRANS,
                    NUM_REL_DOC,
                    DIV_PAY_YEAR,
                    DOCUMENT_REL_DETAIL,
                    RESP_SRI,
                    INFO_ADIC,
                    DET_EXP_REEMBOLSO,
                    JSON_METODO,
                    ITEMS_PROF,
                    OBS_AUXILIAR,
                    OBS_ORDEN
                ) VALUES ?
            `, [values]);
            // Mapear ventas
            let newId = res.insertId;
            for (const s of batch) {
                mapCreditNote[s.COD_TRANS] = newId++;
                /*   mapSalesNoteBound[s.COD_TRAC_FACT] = newId++; */
            }
            console.log(` -> Batch migrado: ${batch.length} notas de credito`);
        } catch (err) {

            throw err;
        }

        /* return { mapCreditNote, mapAuditCreditNote }; */
    }

    const { mapNoteMovements } = await migrateMovementeAdvancesNote(
        legacyConn,
        conn,
        newCompanyId,
        userMap,
        bankMap,
        boxMap,
        mapConciliation,
        mapObligationsCustomers,
        mapPeriodo,
        mapProject,
        mapCenterCost,
        mapAccounts,
        mapCreditNote,
        mapAuditCreditNote
    )


    return { mapCreditNote, mapAuditCreditNote };
}


export async function migrateMovementeAdvancesNote(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    userMap: any,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
    mapObligationsCustomers: Record<number, number | null>,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapCreditNote: Record<number, number | null>,
    mapAuditCreditNote: Record<number, number | null>
): Promise<{ mapNoteMovements: Record<number, number> }> {
    console.log("Migrando notas de credito en ventas...");

    let mapNoteMovements: Record<number, number> = {};

    const [rows] = await legacyConn.query(`SELECT
    COD_TRAC,
    NUM_TRACNOT,
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
    CASE WHEN COALESCE(ID_DET_ANT, '') <> '' THEN 'CREDITO' WHEN m.TIP_MOVI IS NULL THEN 'CREDITO' ELSE m.TIP_MOVI
END AS TIP_MOVI,
IFNULL(
    m.ORIGEN_MOVI,
    'NOTA CREDITO VENTA'
) AS ORIGEN_MOVI,
'CREDITO' AS TIPO_MOVI,
IFNULL(CONCEP_MOVI, OBS_DET_ANT) AS REF_MOVI,
IFNULL(
    transacciones.NUM_TRACNOT,
    OBS_DET_ANT
) AS CONCEP_MOVI,
NULL AS NUM_VOUCHER,
NULL AS NUM_LOTE,
IFNULL(CAUSA_MOVI, 'INGRESO') AS CAUSA_MOVI,
'NCVENTA' AS MODULO,
NULL AS SECU_MOVI,
IFNULL(
    m.IMPOR_MOVI,
    TOTPAG_TRAC
) AS IMPOR_MOVI,
CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 1
END AS ESTADO_MOVI,
IFNULL(
    m.PER_BENE_MOVI,
    clientes.NOM_CLI
) AS PER_BENE_MOVI,
NULL AS FK_COD_EMP,
m.FK_CONCILIADO,
m.CONCILIADO,
FK_COD_CAJAS_MOVI AS IDCAJA,
IFNULL(
    transacciones.OBS_TRAC,
    m.CONCEP_MOVI
) AS OBS_MOVI,
IFNULL(
    m.IMPOR_MOVI,
    TOTPAG_TRAC
) AS IMPOR_MOVITOTAL,
NULL AS FK_ASIENTO,
NULL AS FK_AUDITMV,
NULL AS FK_ARQUEO,
NULL AS ID_TARJETA,
NULL AS RECIBO_CAJA,
NULL AS FK_CTAM_PLAN,
NULL AS NUMERO_UNIDAD,
NULL AS JSON_PAGOS,
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
NUM_TRACNOT AS NUM_REL_DOC,
detalle_anticipos.ID_DET_ANT,
SECU_DET_ANT,
FDP_DET_ANT,
OBS_DET_ANT,
IMPOR_DET_ANT,
m.ID_MOVI,detalle_anticipos.ID_DET_ANT, transacciones.COD_TRAC_FACT
FROM
    transacciones
LEFT JOIN detalle_anticipos ON detalle_anticipos.FK_COD_TRAC = transacciones.COD_TRAC
LEFT JOIN clientes on clientes.COD_CLI = transacciones.FK_COD_CLI
LEFT JOIN movimientos m ON
    m.FK_TRAC_MOVI = transacciones.COD_TRAC
WHERE
    transacciones.TIP_TRAC IN('nota') AND (detalle_anticipos.ID_DET_ANT IS  NULL or detalle_anticipos.ID_DET_ANT = '') and  (m.ID_MOVI IS  NULL or m.ID_MOVI = '')

ORDER BY
    COD_TRAC
DESC;`);

    const ventas = rows as any[];
    if (!ventas.length) {
        return { mapNoteMovements };
    }
    const BATCH_SIZE = 1000;

    const oldDetailAcountCodeMap = [];

    const [[{ nextSecu }]]: any = await conn.query(
        `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS nextSecu FROM movements WHERE MODULO = 'NCVENTA' AND FK_COD_EMP = ?`,
        [newCompanyId]
    );
    let secuenciaMovimiento = nextSecu;

    /* const mapNoteAuditSales: Record<number, number> = {}; */
    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {
            // Preparar valores para INSERT en batch
            const values = batch.map((o, index) => {
                console.log(`transformando y normalizando movimientos nota de credito ${o.NUM_TRACNOT}`);
                // Lógica de Negocio
                let idPlanCuenta = null;
                const currentAuditId = mapAuditCreditNote[o.FK_COD_TRAN];
                o.REF_MOVI = `NOTA CREDITO VENTA N°:${o.NUM_TRACNOT}`;
                return [
                    bankMap[o.FKBANCO] ?? null,
                    mapCreditNote[o.COD_TRAC] ?? null,
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
                mapNoteMovements[o.COD_TRAC] = currentMovId++;
                oldDetailAcountCodeMap.push({
                    idFactura: o.COD_TRAC_FACT,
                    importe: o.IMPOR_MOVI,
                    fecha: o.FECHA_MANUAL,
                    idTrn: o.COD_TRAC,
                    audit: mapNoteMovements[o.COD_TRAC]
                });
            });
            console.log(` -> Batch migrado: ${batch.length} notas de credito por anticipos.`);


        } catch (err) {
            throw err;
        }
    }


    const { mapNoteMovementsFull } = await migrateRetentionsCredit(
        legacyConn,
        conn,
        newCompanyId,
        userMap,
        bankMap,
        boxMap,
        mapConciliation,
        mapCreditNote,
        mapAuditCreditNote,
        mapNoteMovements
    );

    const mapObligations = await migratePaymentDetails(
        legacyConn,
        conn,
        mapObligationsCustomers,
        mapNoteMovementsFull,
        oldDetailAcountCodeMap
    );

    const mapEntryAccount = await migrateAccountingEntriesCustomerObligations(
        legacyConn,
        conn,
        newCompanyId,
        mapNoteMovementsFull,
        mapPeriodo,
        mapAuditCreditNote
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


    return { mapNoteMovements };
}

export async function migrateRetentionsCredit(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    userMap: any,
    bankMap: Record<number, number | null>,
    boxMap: Record<number, number | null>,
    mapConciliation: Record<number, number | null>,
    mapCreditNote: Record<number, number | null>,
    mapAuditCreditNote: Record<number, number | null>,
    mapNoteMovements: Record<number, number | null>,

): Promise<{ mapNoteMovementsFull: Record<number, number> }> {


    console.log("Migrando notas por credito...");
    /*  const mapRetMovements: Record<number, number> = {}; */
    const [rows] = await legacyConn.query(`SELECT
    COD_TRAC,
    NUM_TRACNOT,
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
   'ANTICIPO' AS  TIP_MOVI,
IFNULL(
    m.ORIGEN_MOVI,
    'NOTA CREDITO VENTA'
) AS ORIGEN_MOVI,
'ANTICIPO' AS TIPO_MOVI,
IFNULL(CONCEP_MOVI, OBS_DET_ANT) AS REF_MOVI,
IFNULL(
    transacciones.NUM_TRACNOT,
    OBS_DET_ANT
) AS CONCEP_MOVI,
NULL AS NUM_VOUCHER,
NULL AS NUM_LOTE,
IFNULL(CAUSA_MOVI, 'INGRESO') AS CAUSA_MOVI,
'NCVENTA' AS MODULO,
detalle_anticipos.SECU_DET_ANT AS SECU_MOVI,
IFNULL(
    m.IMPOR_MOVI,
    TOTPAG_TRAC
) AS IMPOR_MOVI,
CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 1
END AS ESTADO_MOVI,
IFNULL(
    m.PER_BENE_MOVI,
    clientes.NOM_CLI
) AS PER_BENE_MOVI,
NULL AS FK_COD_EMP,
m.FK_CONCILIADO,
m.CONCILIADO,
FK_COD_CAJAS_MOVI AS IDCAJA,
IFNULL(
    transacciones.OBS_TRAC,
    m.CONCEP_MOVI
) AS OBS_MOVI,
IFNULL(
    m.IMPOR_MOVI,
    TOTPAG_TRAC
) AS IMPOR_MOVITOTAL,
NULL AS FK_ASIENTO,
NULL AS FK_AUDITMV,
NULL AS FK_ARQUEO,
NULL AS ID_TARJETA,
NULL AS RECIBO_CAJA,
NULL AS FK_CTAM_PLAN,
NULL AS NUMERO_UNIDAD,
NULL AS JSON_PAGOS,
SUBSTRING(NUM_TRACNOT, 9, 9) AS SECUENCIA_REL_DOC,
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
NULL AS TIP_DOC_REL,
NULL AS FK_AUDITTR,
fecha AS FECHA_REG,
NULL AS PUNTO_EMISION_REC,
fechaAnulacion AS FECHA_ANULACION,
NULL AS FK_AUDIT_REL,
NUM_TRACNOT AS NUM_REL_DOC,
detalle_anticipos.ID_DET_ANT,
SECU_DET_ANT,
FDP_DET_ANT,
OBS_DET_ANT,
IMPOR_DET_ANT,
m.ID_MOVI,detalle_anticipos.ID_DET_ANT
FROM
    transacciones
LEFT JOIN detalle_anticipos ON detalle_anticipos.FK_COD_TRAC = transacciones.COD_TRAC
LEFT JOIN clientes on clientes.COD_CLI = transacciones.FK_COD_CLI
LEFT JOIN movimientos m ON  m.FK_TRAC_MOVI = transacciones.COD_TRAC
WHERE
    transacciones.TIP_TRAC IN('nota') AND
      ( (detalle_anticipos.ID_DET_ANT IS NOT NULL AND detalle_anticipos.ID_DET_ANT <> '')
             OR (m.ID_MOVI IS NOT NULL AND m.ID_MOVI  <> '')
          )
ORDER BY
    COD_TRAC
DESC;`);

    // AND (detalle_anticipos.ID_DET_ANT IS  NULL or detalle_anticipos.ID_DET_ANT = '') and  (m.ID_MOVI    IS  NULL or m.ID_MOVI = '')

    const ventas = rows as any[];

    const mapNoteMovementsFull: Record<number, number> = { ...mapNoteMovements };


    if (!ventas.length) {

        return { mapNoteMovementsFull };
    }
    /* const mapNoteMovementsFull= mapNoteMovements; */

    const BATCH_SIZE = 1000;


    const [[{ nextSecu }]]: any = await conn.query(
        `SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS nextSecu FROM movements WHERE MODULO = 'NCVENTA' AND FK_COD_EMP = ?`,
        [newCompanyId]
    );
    let secuenciaMovimiento = nextSecu;

    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {

            // Preparar valores para INSERT en batch
            const values = batch.map((o, index) => {
                console.log(`transformando y normalizando movImientos de NC a credito ${o.NUM_TRACNOT}`);
                // Lógica de Negocio
                let idPlanCuenta = null;
                const currentAuditId = mapAuditCreditNote[o.COD_TRAC];
                o.REF_MOVI = `NOTA CREDITO VENTA N°:${o.NUM_TRACNOT}`;

                return [
                    bankMap[o.FKBANCO] ?? null,
                    mapCreditNote[o.COD_TRAC] ?? null,
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
                mapNoteMovementsFull[o.COD_TRAC] = currentMovId++;
            });
            console.log(` -> Batch migrado: ${batch.length} nota de credito por venta.`);

        } catch (err) {

            throw err;
        }
    }
    return { mapNoteMovementsFull };
}


export async function migratePaymentDetails(
    legacyConn: any,
    conn: any,
    mapObligationsCustomers: Record<number, number | null>,
    mapNoteMovementsFull: Record<number, number | null>,
    oldDetailAcountCodeMap: any
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
                                                            cuentascp.Tipo_cxp = 'CXC' AND detalles_cuentas.forma_pago_cp  IN ('17')
                                                        ORDER BY
                                                            cod_detalle
                                                        DESC;`);


        if (!rows.length) return { mapDetailObligationsAplicate };

        const BATCH_SIZE = 1500;


        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const movementValues = batch.map((o, index) => {
                const idCuenta = mapObligationsCustomers[o.fk_cod_cuenta];


                let idFactNote = null;
                for (let i = 0; i < oldDetailAcountCodeMap.length; i++) {

                    const importeOld = Number(oldDetailAcountCodeMap[i].importe).toFixed(2);
                    const importeNew = Number(o.importe).toFixed(2);
                    if (Number(oldDetailAcountCodeMap[i].idFactura) === Number(o.COD_TRAC) && importeOld === importeNew) {

                        idFactNote = oldDetailAcountCodeMap[i].idTrn;
                        break;
                    }

                }
                const idMovimiento = idFactNote != null ? mapNoteMovementsFull[idFactNote] : null;

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

        console.log("✅ Migración completada de pagos en retenciones");
        return { mapDetailObligationsAplicate };

    } catch (err) {
        console.error("❌ Error:", err);
        throw err;
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