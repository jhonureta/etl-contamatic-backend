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
    branchMap: any, userMap: any, mapClients: any, mapProducts: any, mapRetentions: any
): Promise<{ mapCreditNote: Record<number, number>; mapAuditCreditNote: Record<number, number> }> {
    console.log("Migrando notas de credito...");

    const [rows] = await legacyConn.query(`SELECT
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
tf.OBS_ORD AS OBS_ORDEN
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
        throw new Error(" -> No hay ventas para migrar.");
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
                console.log(`transformando y normalizando ${t.NUM_TRANS}`);

                const productosNormalizados = (t.DOCUMENT_DETAIL ?? []).map(normalizarProducto);

                console.log(productosNormalizados);

                const auditId = mapAuditCreditNote[t.COD_TRANS];
                const vendedor = userMap[t.FK_USER_VEND];
                const creador = userMap[t.FK_USER];
                const cliente = mapClients[t.FK_PERSON];

                let idSucursal = null;

                if (t.TIP_TRAC == 'fisica') {
                    idSucursal = branchMap[listFisica[t.PUNTO_EMISION_DOC]];
                }
                if (t.TIP_TRAC == 'factura') {
                    idSucursal = branchMap[listElectronica[t.PUNTO_EMISION_DOC]];
                }

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
                    t.CLAVE_TRANS.trim(),
                    t.CLAVE_REL_TRANS.trim(),
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
