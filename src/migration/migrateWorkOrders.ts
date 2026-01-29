import { findNextAuditCode, toJSONArray, toNumber } from "./purchaseHelpers";

export async function migrateWorkOrders({
  legacyConn,
  conn,
  newCompanyId,
  userMap,
  mapClients,
  mapProducts,
  storeMap,
  idFirstBranch
}) {
  try {
    console.log("Migrando ordenes de trabajos");
    const workOrderIdMap: Record<number, number> = {};
    const workOrderAuditIdMap: Record<number, number> = {};
    const workOrderSecuencieMap: Record<number, number> = {};

    const [workOrders]: any[] = await legacyConn.query(`
      SELECT
          COD_TRAC AS COD_TRANS,
          NULL AS PUNTO_EMISION_DOC,
          SUBSTRING_INDEX(NUM_TRACORD, '-', -1) AS SECUENCIA_DOC,
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
          'orden' AS TIP_TRAC,
          fecha AS FECHA_REG,
          DET_TRAC AS DOCUMENT_DETAIL,
          NULL AS PUNTO_EMISION_REC,
          fechaAnulacion AS FECHA_ANULACION,
          NULL AS TIP_DOC,
          NULL AS SRI_PAY_CODE,
          IP_CLIENTE AS CLIENT_IP,
          NULL AS FK_AUDIT_REL,
          NUM_TRACORD AS NUM_TRANS,
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
          transacciones.TIP_TRAC = 'orden'
      ORDER BY
          COD_TRAC
      DESC
          ;      
    `);

    if (workOrders.length === 0) {
      return { workOrderIdMap, workOrderAuditIdMap, workOrderSecuencieMap };
    }

    let nextAudit = await findNextAuditCode({ conn, companyId: newCompanyId });

    const BATCH_SIZE = 1000;
    for (let i = 0; i < workOrders.length; i += BATCH_SIZE) {

      const batch = workOrders.slice(i, i + BATCH_SIZE);

      const auditValues = batch.map(() => [
        nextAudit++,
        "ORDEN",
        newCompanyId,
      ]);

      const [resultCreateAudit]: any = await conn.query(
        `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
        [auditValues]
      );

      const firstInsertedAuditId = resultCreateAudit.insertId;

      const workOrderValues = batch.map((workOrder, index: number) => {
        console.log(`transformando y normalizando ORDEN DE TRABAJO ${workOrder.NUM_TRANS}`);
        const productDetails = toJSONArray(workOrder.DOCUMENT_DETAIL);
        const sellerId = userMap[workOrder.FK_USER_VEND];
        const userId = userMap[workOrder.FK_USER];
        const clientId = mapClients[workOrder.FK_PERSON];

        const auditId = firstInsertedAuditId + index;
        workOrderAuditIdMap[workOrder.COD_TRANS] = auditId;

        const { detailTransformed, branchId } = transformProductDetail(
          productDetails,
          mapProducts,
          idFirstBranch,
          storeMap
        );

        return [
          workOrder.PUNTO_EMISION_DOC,
          workOrder.SECUENCIA_DOC,
          workOrder.SECUENCIA_REL_DOC,
          workOrder.CLAVE_TRANS,
          workOrder.CLAVE_REL_TRANS,
          workOrder.TIP_DET_DOC,
          workOrder.FEC_PERIODO_TRAC,
          workOrder.FEC_MES_TRAC,
          workOrder.FEC_TRAC,
          workOrder.FEC_REL_TRAC,
          workOrder.FEC_MERC_TRAC,
          workOrder.MET_PAG_TRAC,
          workOrder.OBS_TRAC,
          userId,
          sellerId,
          clientId,
          workOrder.ESTADO,
          workOrder.ESTADO_REL,
          workOrder.RUTA_DOC_ANULADO,
          workOrder.SUB_BASE_5,
          workOrder.SUB_BASE_8,
          workOrder.SUB_BASE_12,
          workOrder.SUB_BASE_13,
          workOrder.SUB_BASE_14,
          workOrder.SUB_BASE_15,
          workOrder.SUB_12_TRAC,
          workOrder.SUB_0_TRAC,
          workOrder.SUB_N_OBJETO_TRAC,
          workOrder.SUB_EXENTO_TRAC,
          workOrder.SUB_TRAC,
          workOrder.IVA_TRAC,
          workOrder.TOT_RET_TRAC,
          workOrder.TOT_PAG_TRAC,
          workOrder.PROPINA_TRAC,
          workOrder.OTRA_PER,
          workOrder.COD_COMPROBANTE,
          workOrder.COD_COMPROBANTE_REL,
          workOrder.COD_DOCSUS_TRIB,
          newCompanyId,
          workOrder.FRIMADO,
          workOrder.ENVIADO,
          workOrder.AUTORIZADO,
          workOrder.ENVIADO_CLIEMAIL,
          workOrder.FECHA_AUTORIZACION,
          workOrder.FECHA_AUTORIZACION_REL,
          workOrder.TIP_DOC_REL,
          workOrder.DSTO_TRAC,
          branchId,
          auditId,
          workOrder.TIP_TRAC,
          workOrder.FECHA_REG,
          JSON.stringify(detailTransformed),
          workOrder.PUNTO_EMISION_REC,
          workOrder.FECHA_ANULACION,
          workOrder.TIP_DOC,
          workOrder.SRI_PAY_CODE,
          workOrder.CLIENT_IP,
          workOrder.FK_AUDIT_REL,
          workOrder.NUM_TRANS,
          workOrder.NUM_REL_DOC,
          workOrder.DIV_PAY_YEAR,
          null,
          workOrder.RESP_SRI,
          workOrder.INFO_ADIC,
          workOrder.DET_EXP_REEMBOLSO,
          JSON.stringify(toJSONArray(workOrder.JSON_METODO)),
          workOrder.ITEMS_PROF,
          workOrder.OBS_AUXILIAR,
          workOrder.OBS_ORDEN,
        ];
      });

      const [resultCreateWorkOrders]: any = await conn.query(`
        INSERT INTO transactions(
            PUNTO_EMISION_DOC,
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
        )
        VALUES ?
      `, [workOrderValues]);

      /*    let nextId = resultCreateWorkOrders.insertId;
         batch.forEach(({ COD_TRANS, NUM_TRANS }) => {
           let secNext = nextId++;
           workOrderIdMap[COD_TRANS] = secNext;
           workOrderSecuencieMap[NUM_TRANS] = secNext;
         }); */

      let nextId = resultCreateWorkOrders.insertId;

      for (const { COD_TRANS, NUM_TRANS } of batch) {
        const secNext = nextId++;
        workOrderIdMap[COD_TRANS] = secNext;
        workOrderSecuencieMap[NUM_TRANS] = secNext;
      }

      console.log(` -> Batch migrado: ${batch.length} ordenes de trabajos`);
    }

    return { workOrderIdMap, workOrderAuditIdMap, workOrderSecuencieMap };
  } catch (error) {
    console.error("Error al migrar ordenes de trabajos:", error);
    throw error;
  }
}

function transformProductDetail(
  inputDetail: any,
  mapProducts: Record<number, number>,
  idFirstBranch: number | null,
  storeMap: Record<number, number>
) {
  let branchId = idFirstBranch;
  const detailTransformed = inputDetail.map((item: any, index: number) => {

    const idProducto = mapProducts[item?.idProducto] || "";
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
      precios: (item?.valores || [{ name: "Publico", price: toNumber(item.precioProducto), discount: 0, select: true }]).map((p: any, index: number) => ({
        name: p?.nombre || "",
        price: toNumber(p?.valor),
        discount: toNumber(p?.descuento),
        select: index === 0 ? true : false,
      })),
    };
  });
  return { detailTransformed, branchId };
}
