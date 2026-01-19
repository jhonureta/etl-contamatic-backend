import { ClientIdentity, UserIdentity } from "./migrationTools";
import { findFirstDefaultCustomer, findFirstDefaultUser, findNextAuditCode, toJSONArray, toNumber } from "./purchaseHelpers";


interface MigrateShippingGuideParams {
  legacyConn: any;
  conn: any;
  newCompanyId: number;
  mapClients: Record<number, number>;
  mapProducts: Record<number, number>;
  branchMap: Record<number, number>;
  userNameIdMap: Map<string, UserIdentity>;
  clientNameIdMap: Map<string, ClientIdentity>;
  vehicleIdMap: Record<number, number>
}

export async function migrateShippingGuide({
  legacyConn,
  conn,
  newCompanyId,
  mapClients,
  mapProducts,
  branchMap,
  userNameIdMap,
  clientNameIdMap,
  vehicleIdMap
}: MigrateShippingGuideParams) {
  try {
    const [shippingGuide]: any[] = await legacyConn.query(`
      SELECT
          g.COD_GUIAR AS COD_TRANS,
          SUBSTRING_INDEX(g.NUM_GUIA, '-', 2) AS PUNTO_EMISION_DOC,
          SUBSTRING_INDEX(g.NUM_GUIA, '-', -1) AS SECUENCIA_DOC,
          NULL AS SECUENCIA_REL_DOC,
          g.claveFactura AS CLAVE_TRANS,
          NULL AS CLAVE_REL_TRANS,
          NULL AS TIP_DET_DOC,
          YEAR(g.fechaIngreso) AS FEC_PERIODO_TRAC,
          MONTH(fechaIngreso) AS FEC_MES_TRAC,
          g.fechaIngreso AS FEC_TRAC,
          NULL AS FEC_REL_TRAC,
          NULL AS FEC_MERC_TRAC,
          NULL AS MET_PAG_TRAC,
          NULL AS OBS_TRAC,
          g.usuario AS NAME_USER,
          NULL AS FK_USER,
          NULL AS FK_USER_VEND,
          g.cliente AS NAME_CLIENT,
          NULL AS FK_PERSON,
          CASE WHEN g.estado = 'creado' THEN 'pendiente' WHEN g.estado = 'activo' THEN 'activo' ELSE 'anulado'
      END AS ESTADO,
      NULL AS ESTADO_REL,
      NULL AS RUTA_DOC_ANULADO,
      0 AS SUB_BASE_5,
      0 AS SUB_BASE_8,
      0 AS SUB_BASE_12,
      0 AS SUB_BASE_13,
      0 AS SUB_BASE_14,
      0 AS SUB_BASE_15,
      0 AS SUB_12_TRAC,
      0 AS SUB_0_TRAC,
      0 AS SUB_N_OBJETO_TRAC,
      0 AS SUB_EXENTO_TRAC,
      0 AS SUB_TRAC,
      0 AS IVA_TRAC,
      0,
      0 AS TOT_RET_TRAC,
      0 AS TOT_PAG_TRAC,
      0,
      NULL AS OTRA_PER,
      06 AS COD_COMPROBANTE,
      NULL AS COD_COMPROBANTE_REL,
      NULL AS COD_DOCSUS_TRIB,
      NULL AS FK_COD_EMP,
      g.firmado AS FRIMADO,
      g.enviado AS ENVIADO,
      g.autorizado AS AUTORIZADO,
      g.enviadoCliente AS ENVIADO_CLIEMAIL,
      g.fechaAutorizado AS FECHA_AUTORIZACION,
      NULL AS FECHA_AUTORIZACION_REL,
      NULL TIP_DOC_REL,
      0 AS DSTO_TRAC,
      NULL AS FK_CODSUCURSAL,
      NULL AS FK_AUDITTR,
      'Guia' AS TIP_TRAC,
      g.fechaIngreso AS FECHA_REG,
      IFNULL(g.detalleGuia, tr.DET_TRAC) AS DOCUMENT_DETAIL,
      NULL AS PUNTO_EMISION_REC,
      NULL AS FECHA_ANULACION,
      NULL AS TIP_DOC,
      NULL AS SRI_PAY_CODE,
      NULL AS CLIENT_IP,
      NULL AS FK_AUDIT_REL,
      g.NUM_GUIA AS NUM_TRANS,
      NULL AS NUM_REL_DOC,
      NULL AS DIV_PAY_YEAR,
      NULL AS DOCUMENT_REL_DETAIL,
      NULL AS RESP_SRI,
      g.CAMP_ADIC AS INFO_ADIC,
      NULL AS DET_EXP_REEMBOLSO,
      NULL AS JSON_METODO,
      NULL AS ITEMS_PROF,
      NULL AS OBS_AUXILIAR,
      NULL AS OBS_ORDEN,
      g.emailCliente AS EMAIL_CLIENTE,
      g.NUM_AUTORIZACION,
      IFNULL(tr.NUM_TRAC, '001-001-000000001') AS NUM_FACTURA,
      g.FECHA_INICIO,
      g.FECHA_FIN,
      g.MOTIVO_TRASLADO,
      g.NUM_ADUANERA,
      g.P_PARTIDA,
      g.P_LLEGADA,
      g.H_SALIDA,
      g.H_LLEGADA,
      v.COD_VEH,
      v.PLACA
      FROM
          guia_remision g
      JOIN vehiculo v ON
          g.vehiculo = v.COD_VEH
      LEFT JOIN transacciones tr ON
          g.FK_COD_TRANSACCION = tr.COD_TRAC
      ORDER BY
          COD_GUIAR
      DESC
          ;
    `);
    if (shippingGuide.length === 0) {
      throw new Error(" -> No hay guías de envío para migrar.");
    }
    const branchSequenseQuery: string = `
    SELECT
        COD_SURC,
        SUBSTRING(secuencial, 1, 7) AS ELECTRONICA,
        SUBSTRING(secuencialFisica, 1, 7) AS FISICA,
        SUBSTRING(SURC_SEC_COMPINGR, 1, 7) AS COMPINGRESO
    FROM
        sucursales;`;

    const [sequentialBranches]: any[] = await legacyConn.query(branchSequenseQuery, [newCompanyId]);

    let idFirstBranch: number | null = null;
    if (sequentialBranches && sequentialBranches.length > 0) {
      idFirstBranch = Number(sequentialBranches[0].COD_SURC);
    }

    const electronicSequences = new Map<string, number>();
    sequentialBranches.forEach((branch: any, index: number) => {
      electronicSequences.set(branch.ELECTRONICA, branch.COD_SURC);
    });

    const BATCH_SIZE = 1000;
    const shippingGuideIdMap: Record<number, number> = {};
    const shippingGuideAuditIdMap: Record<number, number> = {};

    const [ [defaultUser], [defaultCustomer] ] = await Promise.all([
      findFirstDefaultUser({ conn, companyId: newCompanyId }),
      findFirstDefaultCustomer({ conn, companyId: newCompanyId })
    ]);

    let defaultUserId = null;
    let defaultCustomerId = null;
    let defacultCiUser = null;
    let defacultCiCustomer = null;
    if (defaultUser) {
      defaultUserId = defaultUser.COD_USUEMP;
      defacultCiUser = defaultUser.IDE_USUEMP;
    }
    if (defaultCustomer) {
      defaultCustomerId = defaultCustomer.CUST_ID;
      defacultCiCustomer = defaultCustomer.CUST_CI;
    }

    let nextAudit = await findNextAuditCode({ conn, companyId: newCompanyId });

    for (let i = 0; i < shippingGuide.length; i += BATCH_SIZE) {
      const batch = shippingGuide.slice(i, i + BATCH_SIZE);
      const auditValues = batch.map(() => [
        nextAudit++,
        "GUIA REMISION",
        newCompanyId,
      ]);
      const [resultCreateAudit]: any = await conn.query(
        `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
        [auditValues]
      );
      let firstInsertedAuditId = resultCreateAudit.insertId;

      const shippingGuideValues = batch.map((shippingGuide, index: number) => {
        console.log(`transformando y normalizando guias ${shippingGuide.NUM_TRANS}`);

        const userId = userNameIdMap.get(shippingGuide.NAME_USER?.toUpperCase())?.id || defaultUserId;
        const clientId = clientNameIdMap.get(shippingGuide.NAME_CLIENT?.toUpperCase())?.id || defaultCustomerId;
        const carrierId = userNameIdMap.get(shippingGuide.NAME_USER?.toUpperCase())?.id || defaultUserId;

        const userIdCi = userNameIdMap.get(shippingGuide.NAME_USER?.toUpperCase())?.ci || defacultCiUser;
        const clientCi = clientNameIdMap.get(shippingGuide.NAME_CLIENT?.toUpperCase())?.ci || defacultCiCustomer;

        const productDetails = toJSONArray(shippingGuide.DOCUMENT_DETAIL);

        const auditId = firstInsertedAuditId + index;
        shippingGuideAuditIdMap[shippingGuide.COD_TRANS] = auditId;

        let branchId: number = idFirstBranch;
        if (electronicSequences.has(shippingGuide.PUNTO_EMISION_DOC)) {
          branchId = electronicSequences.get(shippingGuide.PUNTO_EMISION_DOC);
        }

        const detailTransformed = transformProductDetail(
          productDetails,
          mapProducts,
          branchMap,
          idFirstBranch
        );

        const detailedShippingGuide = generateDetailShippingGuide(
          shippingGuide.NUM_TRANS,
          shippingGuide.NUM_FACTURA,
          shippingGuide.NUM_AUTORIZACION,
          'FACTURA',
          shippingGuide.FECHA_INICIO,
          shippingGuide.FECHA_FIN,
          shippingGuide.MOTIVO_TRASLADO,
          shippingGuide.NUM_ADUANERA,
          shippingGuide.P_PARTIDA,
          shippingGuide.H_SALIDA,
          shippingGuide.P_LLEGADA,
          shippingGuide.H_LLEGADA,
          clientCi, // cedula de destinatario
          userIdCi, // cedula de transportista
          shippingGuide.NAME_USER, // nombre del transportista
          shippingGuide.PLACA, // placa del vehiculo
          clientId, // destinatario
          carrierId, // transportista
          branchId, // sucursal
          toJSONArray(shippingGuide.INFO_ADIC)
        );


        return [
          shippingGuide.PUNTO_EMISION_DOC,
          shippingGuide.SECUENCIA_DOC,
          shippingGuide.SECUENCIA_REL_DOC,
          shippingGuide.CLAVE_TRANS,
          shippingGuide.CLAVE_REL_TRANS,
          shippingGuide.TIP_DET_DOC,
          shippingGuide.FEC_PERIODO_TRAC,
          shippingGuide.FEC_MES_TRAC,
          shippingGuide.FEC_TRAC,
          shippingGuide.FEC_REL_TRAC,
          shippingGuide.FEC_MERC_TRAC,
          shippingGuide.MET_PAG_TRAC,
          shippingGuide.OBS_TRAC,
          userId,
          userId,
          clientId,
          shippingGuide.ESTADO,
          shippingGuide.ESTADO_REL,
          shippingGuide.RUTA_DOC_ANULADO,
          shippingGuide.SUB_BASE_5,
          shippingGuide.SUB_BASE_8,
          shippingGuide.SUB_BASE_12,
          shippingGuide.SUB_BASE_13,
          shippingGuide.SUB_BASE_14,
          shippingGuide.SUB_BASE_15,
          shippingGuide.SUB_12_TRAC,
          shippingGuide.SUB_0_TRAC,
          shippingGuide.SUB_N_OBJETO_TRAC,
          shippingGuide.SUB_EXENTO_TRAC,
          shippingGuide.SUB_TRAC,
          shippingGuide.IVA_TRAC,
          shippingGuide.TOT_RET_TRAC,
          shippingGuide.TOT_PAG_TRAC,
          shippingGuide.PROPINA_TRAC,
          shippingGuide.OTRA_PER,
          shippingGuide.COD_COMPROBANTE,
          shippingGuide.COD_COMPROBANTE_REL,
          shippingGuide.COD_DOCSUS_TRIB,
          newCompanyId,
          shippingGuide.FRIMADO,
          shippingGuide.ENVIADO,
          shippingGuide.AUTORIZADO,
          shippingGuide.ENVIADO_CLIEMAIL,
          shippingGuide.FECHA_AUTORIZACION,
          shippingGuide.FECHA_AUTORIZACION_REL,
          shippingGuide.TIP_DOC_REL,
          shippingGuide.DSTO_TRAC,
          branchId,
          auditId,
          shippingGuide.TIP_TRAC,
          shippingGuide.FECHA_REG,
          JSON.stringify(detailTransformed),
          shippingGuide.PUNTO_EMISION_REC,
          shippingGuide.FECHA_ANULACION,
          shippingGuide.TIP_DOC,
          shippingGuide.SRI_PAY_CODE,
          shippingGuide.CLIENT_IP,
          shippingGuide.FK_AUDIT_REL,
          shippingGuide.NUM_TRANS,
          shippingGuide.NUM_REL_DOC,
          shippingGuide.DIV_PAY_YEAR,
          JSON.stringify(detailedShippingGuide),
          shippingGuide.RESP_SRI,
          shippingGuide.INFO_ADIC,
          shippingGuide.DET_EXP_REEMBOLSO,
          shippingGuide.JSON_METODO,
          shippingGuide.ITEMS_PROF,
          shippingGuide.OBS_AUXILIAR,
          shippingGuide.OBS_ORDEN,
        ];
      });

      const [resCreateShippingGuide]: any = await conn.query(`
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
      `, [shippingGuideValues]);
      let newId = resCreateShippingGuide.insertId;
      for (const s of batch) {
        shippingGuideIdMap[s.COD_TRANS] = newId++;
      }
      console.log(` -> Batch migrado: ${batch.length} guías de envío`);
    }
    return { shippingGuideIdMap, shippingGuideAuditIdMap };
  } catch (error) {
    console.error('Error al migrar guías de envío:', error);
    throw error;
  }
}

function transformProductDetail(
  inputDetail: any,
  mapProducts: Record<number, number>,
  branchMap: Record<number, number>,
  idFirstBranch: number | null
) {
  const detailTransformed = inputDetail.map((item: any) => {
    const idProducto = mapProducts[item.idProducto] || null;
    const idBodega = branchMap[item.idBodega] || idFirstBranch;
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
      precios: (item.valores || []).map((p: any, index: number) => ({
        name: p.nombre || "",
        price: toNumber(p.valor),
        discount: toNumber(p.descuento),
        select: index === 0 ? true : false,
      })),
    };
  });
  return detailTransformed as any[];
}

function generateDetailShippingGuide(
  sequential: string,
  voucher: string,
  authorization: string,
  document: string,
  startDate: string,
  endDate: string,
  reason: string,
  authorizationCutoms: string,
  origin: string,
  startTime: string,
  destination: string,
  endTime: string,
  ciRecipient: string,
  ciCarrier: string,
  nameCarrier: string,
  plate: string,
  recipient: number,
  carrier: number,
  sucursal: number,
  detalleItemsAdic: any[]
) {
  return [
    {
      sequential,
      voucher,
      authorization,
      document,
      startDate,
      endDate,
      reason,
      authorizationCutoms,
      origin,
      startTime,
      destination,
      endTime,
      ciRecipient,
      ciCarrier,
      nameCarrier,
      plate,
      recipient,
      carrier,
      sucursal,
      detalleItemsAdic,
    },
  ];
}