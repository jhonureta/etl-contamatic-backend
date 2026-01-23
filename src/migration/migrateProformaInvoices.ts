import { findNextAuditCode, toJSONArray, toNumber } from "./purchaseHelpers";

export async function migrateProformaInvoices({
  legacyConn,
  conn,
  newCompanyId,
  userMap,
  mapClients,
  mapProducts,
  branchMap,
  storeMap,
  idFirstBranch
}) {
  try {
    console.log("Migrando proformas...");
    const proformaIdMap: Record<number, number> = {};
    const proformaAuditIdMap: Record<number, number> = {};

    const [proformas]: any[] = await legacyConn.query(`
    SELECT 
        COD_TRAC AS COD_TRANS,
        NULL AS PUNTO_EMISION_DOC,
        SUBSTRING_INDEX(NUM_TRACPROF, '-', -1) AS SECUENCIA_DOC,
        NULL AS SECUENCIA_REL_DOC,
        NULL AS CLAVE_TRANS,
        NULL AS CLAVE_REL_TRANS,
        NULL AS TIP_DET_DOC,
        YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
        MONTH(FEC_TRAC) AS FEC_MES_TRAC,
        FEC_TRAC AS FEC_TRAC,
        NULL AS FEC_REL_TRAC,
        FEC_MERC_TRAC,
        NULL AS MET_PAG_TRAC,
        OBS_TRAC,
        FK_COD_USU AS FK_USER,
        FK_USU_CAJA AS FK_USER_VEND,
        FK_COD_CLI AS FK_PERSON,
        estado AS ESTADO,
        estado_compra AS ESTADO_REL,
        documentoAnulado AS RUTA_DOC_ANULADO,
        SUB_BASE_5 AS SUB_BASE_5,
        SUB_BASE_8 AS SUB_BASE_8,
        SUB_BASE_12 AS SUB_BASE_12,
        SUB_BASE_13 AS SUB_BASE_13,
        SUB_BASE_14 AS SUB_BASE_14,
        SUB_BASE_15 AS SUB_BASE_15,
        SUB12_TRAC AS SUB_12_TRAC,
        SUB0_TRAC AS SUB_0_TRAC,
        SUBNOBJETO_TRAC AS SUB_N_OBJETO_TRAC,
        SUBEXENTO_TRAC AS SUB_EXENTO_TRAC,
        SUB_TRAC AS SUB_TRAC,
        IVA_TRAC AS IVA_TRAC,
        IVA_TRAC,
        TOTRET_TRAC AS TOT_RET_TRAC,
        TOTPAG_TRAC AS TOT_PAG_TRAC,
        PROPINA_TRAC,
        OTRA_PER,
        NULL AS COD_COMPROBANTE,
        NULL AS COD_COMPROBANTE_REL,
        NULL AS COD_DOCSUS_TRIB,
        NULL AS FK_COD_EMP,
        firmado AS FRIMADO,
        enviado AS ENVIADO,
        autorizado AS AUTORIZADO,
        enviadoCliente AS ENVIADO_CLIEMAIL,
        fechaAutorizado AS FECHA_AUTORIZACION,
        NULL AS FECHA_AUTORIZACION_REL,
        NULL TIP_DOC_REL,
        DES_TRAC AS DSTO_TRAC,
        NULL AS FK_CODSUCURSAL,
        NULL AS FK_AUDITTR,
        'proforma' AS TIP_TRAC,
        fecha AS FECHA_REG,
        DET_TRAC AS DOCUMENT_DETAIL,
        NULL AS PUNTO_EMISION_REC,
        fechaAnulacion AS FECHA_ANULACION,
        NULL AS TIP_DOC,
        NULL AS SRI_PAY_CODE,
        IP_CLIENTE AS CLIENT_IP,
        NULL AS FK_AUDIT_REL,
        NUM_TRACPROF AS NUM_TRANS,
        secuencialFacturaRet AS NUM_REL_DOC,
        serial_producto AS DIV_PAY_YEAR,
        NULL AS DOCUMENT_REL_DETAIL,
        RESPU_SRI AS RESP_SRI,
        INFO_ADIC AS INFO_ADIC,
        DET_REMBOLSO AS DET_EXP_REEMBOLSO,
        METPAG_JSON_TRAC AS JSON_METODO,
        INFO_ADIC AS ITEMS_PROF,
        OBS_AUXILIAR AS OBS_AUXILIAR,
        OBS_ORD AS OBS_ORDEN
    FROM
        transacciones
    WHERE
        transacciones.TIP_TRAC = 'Proforma'
    ORDER BY
        COD_TRAC
    DESC
        ;
    `);

    if (proformas.length === 0) {
      return { proformaIdMap, proformaAuditIdMap };
    }

    let nextAudit = await findNextAuditCode({ conn, companyId: newCompanyId });

    const BATCH_SIZE = 1000;

    for (let i = 0; i < proformas.length; i += BATCH_SIZE) {
      const batch = proformas.slice(i, i + BATCH_SIZE);

      const auditValues = batch.map(() => [
        nextAudit++,
        "PROFORMA",
        newCompanyId,
      ]);

      const [resultCreateAudit]: any = await conn.query(
        `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
        [auditValues]
      );

      const firstInsertedAuditId = resultCreateAudit.insertId;

      const proformaValues = batch.map((proforma, index: number) => {
        console.log(`transformando y normalizando ${proforma.NUM_TRANS}`);
        const productDetails = toJSONArray(proforma.DOCUMENT_DETAIL);
        const sellerId = userMap[proforma.FK_USER_VEND];
        const userId = userMap[proforma.FK_USER];
        const clientId = mapClients[proforma.FK_PERSON];
        //mapeo de auditoria por transaccion
        const auditId = firstInsertedAuditId + index;
        proformaAuditIdMap[proforma.COD_TRANS] = auditId;

        const { detailTransformed, branchId  } = transformProductDetail(
          productDetails,
          mapProducts,
          idFirstBranch,
          storeMap
        );

        return [
          proforma.PUNTO_EMISION_DOC,
          proforma.SECUENCIA_DOC,
          proforma.SECUENCIA_REL_DOC,
          proforma.CLAVE_TRANS,
          proforma.CLAVE_REL_TRANS,
          proforma.TIP_DET_DOC,
          proforma.FEC_PERIODO_TRAC,
          proforma.FEC_MES_TRAC,
          proforma.FEC_TRAC,
          proforma.FEC_REL_TRAC,
          proforma.FEC_MERC_TRAC,
          proforma.MET_PAG_TRAC,
          proforma.OBS_TRAC,
          userId,
          sellerId,
          clientId,
          proforma.ESTADO,
          proforma.ESTADO_REL,
          proforma.RUTA_DOC_ANULADO,
          proforma.SUB_BASE_5,
          proforma.SUB_BASE_8,
          proforma.SUB_BASE_12,
          proforma.SUB_BASE_13,
          proforma.SUB_BASE_14,
          proforma.SUB_BASE_15,
          proforma.SUB_12_TRAC,
          proforma.SUB_0_TRAC,
          proforma.SUB_N_OBJETO_TRAC,
          proforma.SUB_EXENTO_TRAC,
          proforma.SUB_TRAC,
          proforma.IVA_TRAC,
          proforma.TOT_RET_TRAC,
          proforma.TOT_PAG_TRAC,
          proforma.PROPINA_TRAC,
          proforma.OTRA_PER,
          proforma.COD_COMPROBANTE,
          proforma.COD_COMPROBANTE_REL,
          proforma.COD_DOCSUS_TRIB,
          newCompanyId,
          proforma.FRIMADO,
          proforma.ENVIADO,
          proforma.AUTORIZADO,
          proforma.ENVIADO_CLIEMAIL,
          proforma.FECHA_AUTORIZACION,
          proforma.FECHA_AUTORIZACION_REL,
          proforma.TIP_DOC_REL,
          proforma.DSTO_TRAC,
          branchId,
          auditId,
          proforma.TIP_TRAC,
          proforma.FECHA_REG,
          JSON.stringify(detailTransformed),
          proforma.PUNTO_EMISION_REC,
          proforma.FECHA_ANULACION,
          proforma.TIP_DOC,
          proforma.SRI_PAY_CODE,
          proforma.CLIENT_IP,
          proforma.FK_AUDIT_REL,
          proforma.NUM_TRANS,
          proforma.NUM_REL_DOC,
          proforma.DIV_PAY_YEAR,
          null,
          proforma.RESP_SRI,
          proforma.INFO_ADIC,
          proforma.DET_EXP_REEMBOLSO,
          JSON.stringify(toJSONArray(proforma.JSON_METODO)),
          proforma.ITEMS_PROF,
          proforma.OBS_AUXILIAR,
          proforma.OBS_ORDEN,
        ];
      });

      const [resulCreateProformas]: any = await conn.query(`
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
      `,
        [proformaValues]
      );
      let nextId = resulCreateProformas.insertId;
      batch.forEach(({ COD_TRANS }) => {
        proformaIdMap[COD_TRANS] = nextId++;
      });
      console.log(` -> Batch migrado: ${batch.length} proformas`);
    }

    return { proformaIdMap, proformaAuditIdMap };
  } catch (error) {
    throw error;
  }
}

function transformProductDetail(
  inputDetail: any,
  mapProducts: Record<number, number>,
  idFirstBranch: number | null, // id de la primera bodega, en caso no exista
  storeMap: Record<number, number> // mapa de bodegas
) {
  let branchId = idFirstBranch;
  const detailTransformed = inputDetail.map((item: any, index: number) => {
   
    const idProducto = mapProducts[item?.idProducto] || '';
    const mappedBodega = storeMap[item?.idBodega];
    if (index === 0 && mappedBodega) branchId = mappedBodega;
    const idBodega = mappedBodega || idFirstBranch;
    return {
      idProducto,
      idBodega,
      esCombo: false,
      codigo: item.codigo || "",
      nombre: item.nombre || "",
      stock: item.stock || 0,
      costo: toNumber(item.precioProducto),
      marca: "SIN MARCA",
      cantidad: toNumber(item.cantidad),
      impuesto: toNumber(item.impuesto),
      codigoImpuesto: toNumber(item.codigoimpuesto),
      nombreImpuesto: item.nombreImpuesto,
      precioProducto: toNumber(item.precioProducto),
      precioSinIva: toNumber(item.precioSinIva),
      precioPlusIva: toNumber(item.precioPlusIva),
      porcentajeDescuento: toNumber(item.porcentajeDescuento),
      valorDescuento: toNumber(item.valorDescuento),
      total: toNumber(item.tota),
      tota: toNumber(item.tota),
      preciomanual: toNumber(item.preciomanual),
      codigoAuxiliar: item.codigoAuxiliar || "",
      precios: item.valores.map((p: any, index: number) => ({
        name: p.nombre || "",
        price: toNumber(p.valor),
        discount: toNumber(p.descuento),
        select: index === 0 ? true : false,
      })),
    };
  });
  return { detailTransformed, branchId };
}
