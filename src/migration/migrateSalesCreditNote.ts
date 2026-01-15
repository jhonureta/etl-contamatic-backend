import { normalizarProducto } from "./normalizador";

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
    mapRetentions: any,
    oldProductCodeMap: any,
    mapAuditSales: any
): Promise<{ mapCreditNote: Record<number, number>; mapAuditCreditNote: Record<number, number> }> {
    console.log("Migrando notas de credito...");

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
        throw new Error(" -> No hay notas de credito para migrar.");
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
    const mapCreditNote: Record<number, number> = {};
    const mapAuditCreditNote: Record<number, number> = {};

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
                const productosNormalizados = productos.map(p => normalizarProducto(p, mapProducts, branchMap, oldProductCodeMap, idSucursal));

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
                    t.PROPINA_TRAC,
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
                    t.CLIENT_IP,
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
            }
            console.log(` -> Batch migrado: ${batch.length} ventas`);
        } catch (err) {

            throw err;
        }
    }

    return { mapCreditNote, mapAuditCreditNote };
}

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
            `SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ? LIMIT 1`,
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
tn.TIP_TRAC as fact,
    tf.COD_TRAC AS COD_TRANS,
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
tf.fechaFacturaRet AS FEC_REL_TRAC,
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
NULL AS COD_COMPROBANTE_REL,
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
tf.OBS_ORD AS OBS_ORDEN, movimientos.*, detalle_anticipos.*
FROM
    transacciones tf
INNER JOIN transacciones tn ON
    tf.COD_TRAC_FACT = tn.COD_TRAC inner join movimientos on movimientos.FK_TRAC_MOVI = tn.COD_TRAC LEFT JOIN detalle_anticipos ON detalle_anticipos.FK_COD_TRAC = tf.COD_TRAC
WHERE
    tf.TIP_TRAC IN('nota')
ORDER BY
    tf.COD_TRAC
DESC;`);
        if (!rows.length) return { mapMovements, mapAuditMovements };

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);

            // --- PASO A: INSERTAR AUDITORÍA EN BATCH ---
            // Solo creamos auditoría si hay datos válidos
            const auditValues = batch.map(o => [auditSeq++, o.forma, newCompanyId]);
            const [resAudit]: any = await conn.query(`INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,[auditValues]);
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

               /*  if (o.forma === 'CCTACONT') {
                    const codigoPlan = o.REF_MOVI?.split('--')[0].trim();
                    idPlanCuenta = accountMap.get(codigoPlan) || null;
                    tipMovi = tipoMovi = 'CCTACONT';
                } else if (o.forma === 'RET-VENTA') {
                    tipMovi = tipoMovi = origen = 'RET-VENTA';
                    modulo = 'RETENCION-VENTA'; */
              /*   } else if (o.forma === 'NOTA DE CREDITO') { */
                    tipMovi = tipoMovi = 'CREDITO';
                    modulo = 'NCVENTA';
                    origen = 'NOTA CREDITO VENTA';
                /* } */

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
                    o.CONCILIATED ?? 0,
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
        /*   const mapDetailObligationsAplicate = await migratePaymentDetails(
              legacyConn,
              conn,
              mapObligationsCustomers,
              mapMovements
          );
          console.log("Movimientos de cobros realizados:", Object.keys(mapMovements).length);
          console.log("Detalle migrado:", Object.keys(mapDetailObligationsAplicate).length);
          console.log("✅ Migración de cobros completadas"); */



        /*   const mapEntryAccount = await migrateAccountingEntriesCustomerObligations(
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
          ) */

        /*    console.log("Detalle de asientos contables migrados:", Object.keys(mapDetailAsiento).length);
    */
        return { mapMovements, mapAuditMovements };

    } catch (err) {
        console.error("❌ Error:", err);
        throw err;
    }
}