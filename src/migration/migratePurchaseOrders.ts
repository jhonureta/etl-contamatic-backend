import { Connection, FieldPacket, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { findNextAuditCode, restructureProductDetails, RetentionCodeValue, toJSONArray, toNumber } from "./purchaseHelpers";
import { upsertTotaledEntry } from "./migrationTools";

type ResultSet = [RowDataPacket[] | RowDataPacket[][] | ResultSetHeader, FieldPacket[]];

type MigratePurchaseOrdersParams = {
  legacyConn: Connection;
  conn: Connection;
  newCompanyId: number;
  branchMap: Record<number, number>;
  userMap: Record<number, number>;
  mapSuppliers: Record<number, number>;
  mapProducts: Record<number, number>;
  oldRetentionCodeMap: Map<string, RetentionCodeValue>;
  newRetentionIdMap: Record<number, number>;
  mapCostExpenses: Record<number, number>;
}

type MigrateMovementsParams = {
  legacyConn: Connection;
  conn: Connection;
  newCompanyId: number;
  purchaseOrderIdMap: Record<number, number>;
  purchaseOrderAuditIdMap: Record<number, number>;
  mapSuppliers: Record<number, number>;
  mapPeriodo: Record<number, number>;
  mapProject: Record<number, number>;
  mapCenterCost: Record<number, number>;
  userMap: Record<number, number>;
  mapAccounts: Record<number, number>;
  bankMap: Record<number, number>;
  boxMap: Record<number, number>;
  mapConciliation: Record<number, number>
}

type MigrateObligationsParams = Omit<
  MigrateMovementsParams,
  'mapPeriodo' |
  'mapProject' |
  'mapCenterCost' |
  'bankMap' |
  'boxMap' |
  'mapConciliation' |
  'userMap' |
  'mapAccounts'
>;

type MigrateOrderMovementsParams = Omit<MigrateMovementsParams, 'mapCenterCost' | 'mapProject' | 'mapPeriodo' | 'mapAccounts' | 'mapSuppliers'>;

type MigrateAccountingEntriesPurchaseOrderParams = {
  legacyConn: Connection;
  conn: Connection;
  newCompanyId: number;
  movementOrderIdMap: Record<number, number>;
  purchaseOrderIdMap: Record<number, number>;
  purchaseOrderAuditIdMap: Record<number, number>;
  mapPeriodo: Record<number, number>;
}

type MigrateAccountingEntriesPurchaseOrderDetailParams = {
  legacyConn: Connection;
  conn: Connection;
  newCompanyId: number;
  mapProject: Record<number, number>;
  mapCenterCost: Record<number, number>;
  mapAccounts: Record<number, number>;
  accountingEntryOrderIdMap: Record<number, number>;
}

export async function migratePurchaseOrders({
  legacyConn,
  conn,
  newCompanyId,
  branchMap,
  userMap,
  mapSuppliers,
  mapProducts,
  oldRetentionCodeMap,
  newRetentionIdMap,
  mapCostExpenses
}: MigratePurchaseOrdersParams): Promise<{ purchaseOrderIdMap: Record<number, number>; purchaseOrderAuditIdMap: Record<number, number> }> {
  try {
    console.log("Migrando pedidos de compra...");

    const resultOrdersQuery: ResultSet = await legacyConn.query(`
      SELECT
          COD_TRAC AS COD_TRANS,
          NULL AS PUNTO_EMISION_DOC,
          SUBSTRING_INDEX(NUM_PED, '-', -1) AS SECUENCIA_DOC,
          NULL AS SECUENCIA_REL_DOC,
          NULL AS CLAVE_TRANS,
          NULL AS CLAVE_REL_TRANS,
          'mercaderia' AS TIP_DET_DOC,
          YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
          MONTH(FEC_TRAC) AS FEC_MES_TRAC,
          FEC_TRAC AS FEC_TRAC,
          NULL AS FEC_REL_TRAC,
          FEC_MERC_TRAC,
          METPAG_TRAC AS MET_PAG_TRAC,
          OBS_TRAC,
          FK_COD_USU AS FK_USER,
          FK_COD_USU AS FK_USER_VEND,
          FK_COD_PROVE AS FK_PERSON,
          'Activo' AS ESTADO,
          NULL AS ESTADO_REL,
          NULL AS RUTA_DOC_ANULADO,
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
          COALESCE(PROPINA_TRAC, 0) AS PROPINA_TRAC,
          OTRA_PER,
          NULL AS COD_COMPROBANTE,
          NULL AS COD_COMPROBANTE_REL,
          NULL AS COD_DOCSUS_TRIB,
          NULL AS FK_COD_EMP,
          NULL AS COD_ASIENTO,
          firmado AS FRIMADO,
          enviado AS ENVIADO,
          autorizado AS AUTORIZADO,
          enviadoCliente AS ENVIADO_CLIEMAIL,
          NULL AS FECHA_AUTORIZACION,
          NULL AS FECHA_AUTORIZACION_REL,
          NULL AS TIP_DOC_REL,
          DES_TRAC AS DSTO_TRAC,
          NULL AS FK_CODSUCURSAL,
          NULL AS FK_AUDITTR,
          'Pedido' AS TIP_TRAC,
          fecha AS FECHA_REG,
          DET_TRAC AS DOCUMENT_DETAIL,
          NULL AS PUNTO_EMISION_REC,
          NULL AS FK_DOC_REL,
          NULL AS FECHA_ANULACION,
          NULL AS TIP_DOC,
          NULL AS SRI_PAY_CODE,
          IP_CLIENTE AS CLIENT_IP,
          NULL AS FK_AUDIT_REL,
          NULL AS NUM_TRANS,
          NULL AS NUM_REL_DOC,
          NULL AS DIV_PAY_YEAR,
          NULL AS DOCUMENT_REL_DETAIL,
          NULL AS RESP_SRI,
          NULL AS INFO_ADIC,
          NULL AS DET_EXP_REEMBOLSO,
          METPAG_JSON_TRAC AS JSON_METODO,
          INFO_ADIC AS ITEMS_PROF,
          OBS_AUXILIAR AS OBS_AUXILIAR,
          OBS_ORD AS OBS_ORDEN
      FROM
          transacciones
      WHERE
          transacciones.TIP_TRAC = 'pedido'
      ORDER BY
          COD_TRAC
      DESC
          ;
    `);

    const [purchaseOrders]: any[] = resultOrdersQuery as Array<any>;
    if (purchaseOrders.length === 0) {
      throw new Error(" -> No existen registros de pedidos de compra para migrar");
    }

    const branchSequenseQuery: string = `
    SELECT
        COD_SURC,
        SUBSTRING(secuencial, 1, 7) AS ELECTRONICA,
        SUBSTRING(secuencialFisica, 1, 7) AS FISICA,
        SUBSTRING(SURC_SEC_COMPINGR, 1, 7) AS COMPINGRESO
    FROM
        sucursales;`;

    const resultSequentialQuery: ResultSet = await legacyConn.query(branchSequenseQuery, [newCompanyId]);
    const [sequentialBranches]: any[] = resultSequentialQuery as Array<any>;

    let idFirstBranch: number | null = null;
    if (sequentialBranches.length > 0) {
      idFirstBranch = sequentialBranches[0].COD_SURC;
    }

    const auditId = await findNextAuditCode({ conn, companyId: newCompanyId });
    const BATCH_SIZE: number = 1000;
    const purchaseOrderIdMap: Record<number, number> = {};
    const purchaseOrderAuditIdMap: Record<number, number> = {};

    for (let i = 0; i < purchaseOrders.length; i += BATCH_SIZE) {
      const batchOrders = purchaseOrders.slice(i, i + BATCH_SIZE);

      const auditValues = batchOrders.map((order: any, index: number) => {
        const auditIdIsert = auditId + index;
        return [
          auditIdIsert,
          'PEDIDO',
          newCompanyId
        ]
      });

      const resultCreateAudit: ResultSet = await conn.query(
        `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
        [auditValues]
      );
      const firstAuditId = (resultCreateAudit[0] as ResultSetHeader).insertId;

      const orderValues = batchOrders.map((order: any, index: number) => {

        console.log(`transformando y normalizando secuencial de pedido: ${order.SECUENCIA_DOC}`);

        const codTrans = order.COD_TRANS;
        const userId = userMap[order.FK_USER];// id usuario
        const sellerId = userMap[order.FK_USER];// id vendedor
        const supplierId = mapSuppliers[order.FK_PERSON];// id proveedor
        const auditId = firstAuditId + index;
        purchaseOrderAuditIdMap[codTrans] = auditId;

        const productDetails = toJSONArray(order.DOCUMENT_DETAIL);
        const { detailTransformed, branchId } = transformProductDetail(
          productDetails,
          mapProducts,
          branchMap,
          idFirstBranch
        );

        return [
          order.PUNTO_EMISION_DOC,
          order.SECUENCIA_DOC,
          order.SECUENCIA_REL_DOC,
          order.CLAVE_TRANS,
          order.CLAVE_REL_TRANS,
          order.TIP_DET_DOC,
          order.FEC_PERIODO_TRAC,
          order.FEC_MES_TRAC,
          order.FEC_TRAC,
          order.FEC_REL_TRAC,
          order.FEC_MERC_TRAC,
          order.MET_PAG_TRAC,
          order.OBS_TRAC,
          userId,
          sellerId,
          supplierId,
          order.ESTADO,
          order.ESTADO_REL,
          order.RUTA_DOC_ANULADO,
          order.SUB_BASE_5,
          order.SUB_BASE_8,
          order.SUB_BASE_12,
          order.SUB_BASE_13,
          order.SUB_BASE_14,
          order.SUB_BASE_15,
          order.SUB_12_TRAC,
          order.SUB_0_TRAC,
          order.SUB_N_OBJETO_TRAC,
          order.SUB_EXENTO_TRAC,
          order.SUB_TRAC,
          order.IVA_TRAC,
          order.TOT_RET_TRAC,
          order.TOT_PAG_TRAC,
          order.PROPINA_TRAC,
          order.OTRA_PER,
          order.COD_COMPROBANTE,
          order.COD_COMPROBANTE_REL,
          order.COD_DOCSUS_TRIB,
          newCompanyId,
          order.FRIMADO,
          order.ENVIADO,
          order.AUTORIZADO,
          order.ENVIADO_CLIEMAIL,
          order.FECHA_AUTORIZACION,
          order.FECHA_AUTORIZACION_REL,
          order.TIP_DOC_REL,
          order.DSTO_TRAC,
          branchId,
          auditId,
          order.TIP_TRAC,
          order.FECHA_REG,
          JSON.stringify(detailTransformed),
          order.PUNTO_EMISION_REC,
          order.FECHA_ANULACION,
          order.TIP_DOC,
          order.SRI_PAY_CODE,
          order.CLIENT_IP,
          order.FK_AUDIT_REL,
          order.NUM_TRANS,
          order.NUM_REL_DOC,
          order.DIV_PAY_YEAR,
          order.DOCUMENT_REL_DETAIL,
          order.RESP_SRI,
          order.INFO_ADIC,
          order.DET_EXP_REEMBOLSO,
          JSON.stringify(toJSONArray(order.JSON_METODO)),
          order.ITEMS_PROF,
          order.OBS_AUXILIAR,
          order.OBS_ORDEN
        ];

      });

      const resultCreatePurchase: ResultSet = await conn.query(`
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
      `, [orderValues]);
      let nextOrderId = (resultCreatePurchase[0] as ResultSetHeader).insertId;
      batchOrders.forEach(({ COD_TRANS }) => {
        purchaseOrderIdMap[COD_TRANS] = nextOrderId++;
      });

      console.log(` -> Batch migrado: ${batchOrders.length} pedidos de compra`);
    }
    return { purchaseOrderIdMap, purchaseOrderAuditIdMap };
  } catch (error) {
    console.error("Error al migrar pedidos de compra:", error);
    throw error;
  }
}

function transformProductDetail(
  inputDetail: any,
  mapProducts: Record<number, number>,
  branchMap: Record<number, number>,
  idFirstBranch: number | null
) {
  let branchId = null;
  const detailTransformed = inputDetail.map((item: any, index: number) => {
    if (index === 0 && item.idBodega) {
      branchId = branchMap[item.idBodega] || idFirstBranch; // Cambiar null por id de primera bodega
    }
    const idProducto = mapProducts[item.idProducto] || null;
    const idBodega = branchMap[item.idBodega] || idFirstBranch;
    return {
      idProducto,
      idBodega,
      bodega: String(item.bodega ?? ''),
      codigo: item.codigo || "",
      nombre: item.nombre || "",
      stock: toNumber(item.stock) || 0,
      observacion: String(item.descripcion ?? ""),
      cantidad: toNumber(item.cantidad),
      impuesto: toNumber(item.impuesto),
      codigoImpuesto: toNumber(item.codigoimpuesto),
      nombreImpuesto: item.nombreImpuesto || toNumber(item.impuesto),
      precioProducto: toNumber(item.precioProducto),
      costoProducto: toNumber(item.costoProducto),
      porcentajeDescuento: toNumber(item.porcentajeDescuento),
      valorDescuento: toNumber(item.valorDescuento),
      total: toNumber(item.total),
      preciomanual: toNumber(item.preciomanual),
      codigoAuxiliar: item.codigoAuxiliar || "",
      tipo: ''
    };
  });
  return { detailTransformed, branchId };
}

export async function migratePurchaseOrderMovements({
  legacyConn,
  conn,
  newCompanyId,
  mapPeriodo,
  mapProject,
  mapCenterCost,
  userMap,
  mapSuppliers,
  purchaseOrderAuditIdMap,
  purchaseOrderIdMap,
  mapAccounts,
  mapConciliation,
  bankMap,
  boxMap
}: MigrateMovementsParams) {

  console.log("Iniciando migración de obligaciones de pedidos de compra");

  const { orderObligationIdMap, orderObligationAuditIdMap } = await migratePurchaseOrderObligations({
    legacyConn,
    conn,
    newCompanyId,
    purchaseOrderIdMap,
    mapSuppliers,
    purchaseOrderAuditIdMap,
  })
  console.log(` -> Migracion de obligaciones de pedidos completada`);

  console.log(" -> migrando movimientos de pedidos");
  const { movementOrderIdMap } = await migrateOrderMovements({
    legacyConn,
    conn,
    newCompanyId,
    purchaseOrderIdMap,
    purchaseOrderAuditIdMap,
    mapConciliation,
    userMap,
    bankMap,
    boxMap
  });
  console.log(` -> Migracion de movimientos de pedidos completada`);

  console.log(" -> Migrando asientos contables de pedidos");
  const { accountingEntryOrderIdMap } = await migrateAccountingEntriesPurchaseOrder({
    legacyConn,
    conn,
    newCompanyId,
    movementOrderIdMap,
    purchaseOrderIdMap,
    purchaseOrderAuditIdMap,
    mapPeriodo,
  });
  console.log(` -> Migracion de asientos contables de pedidos completada`);

  console.log(" -> Migrando detalle de asientos contables de pedidos");
  const { accountingEntryOrderDetailIdMap } = await migrateAccountingEntriesPurchaseOrderDetail({
    legacyConn,
    conn,
    newCompanyId,
    mapProject,
    mapCenterCost,
    mapAccounts,
    accountingEntryOrderIdMap
  });
  console.log(` -> Migracion de pedidos completada`);
  return { accountingEntryOrderIdMap, orderObligationIdMap, accountingEntryOrderDetailIdMap };
}

async function migratePurchaseOrderObligations({
  legacyConn,
  conn,
  newCompanyId,
  purchaseOrderIdMap,
  mapSuppliers,
  purchaseOrderAuditIdMap,
}: MigrateObligationsParams) {

  try {
    console.log("Migrando obligaciones de pedidos de compra...");
    const orderObligationIdMap: Record<number, number> = {};
    const orderObligationAuditIdMap: Record<number, number> = {};

    const auditId = await findNextAuditCode({ conn, companyId: newCompanyId });

    const resultObligationsQuery: ResultSet = await legacyConn.query(`
      SELECT
          cp.cod_cp AS old_id,
          cp.fk_cod_prov_cp AS fk_persona_old,
          cp.FK_TRAC_CUENTA AS fk_cod_trans_old,
          cp.OBL_ASI AS obl_asi_group,
          cp.Tipo_cxp AS tipo_obl,
          cp.fecha_emision_cxp AS fech_emision,
          cp.fecha_vence_cxp AS fech_vencimiento,
          cp.tipo_documento AS tip_doc,
          cp.estado_cxp AS estado,
          cp.saldo_cxp AS saldo,
          cp.valor_cxp AS total,
          cp.referencia_cxp AS ref_secuencia,
          cp.TIPO_CUENTA AS tipo_cuenta,
          cp.FK_SERVICIO AS fk_id_infcon,
          cp.TIPO_ESTADO_CUENTA AS tipo_estado_cuenta,
          t.TIP_TRAC AS tipoTransaccion,
          fk_cod_usu_cp
      FROM
          cuentascp cp
      LEFT JOIN transacciones t ON
          t.COD_TRAC = cp.FK_TRAC_CUENTA
      WHERE
          cp.Tipo_cxp = 'CXP' AND t.TIP_TRAC = 'pedido'
      ORDER BY
          cp.cod_cp;
    `);

    const [obligations]: any[] = resultObligationsQuery as Array<any>;
    if (obligations.length === 0) {
      return { orderObligationIdMap, orderObligationAuditIdMap };
    }

    const BATCH_SIZE: number = 500;
    let nexAuditId = auditId;

    for (let i = 0; i < obligations.length; i += BATCH_SIZE) {
      const batchObligation = obligations.slice(i, i + BATCH_SIZE);
      const valuesInsertObligations: any[] = [];
      for (const o of batchObligation) {
        let auditIdInsert: number | null = null;
        if (o.fk_cod_trans_old && purchaseOrderAuditIdMap[o.fk_cod_trans_old]) {
          auditIdInsert = purchaseOrderAuditIdMap[o.fk_cod_trans_old];
        }
        if (!auditIdInsert) {
          throw new Error(`No se pudo resolver AUDIT para obligación ${o.old_id}`);
        }
        orderObligationAuditIdMap[o.old_id] = auditIdInsert;

        const supplierId = mapSuppliers[o.fk_persona_old];
        if (!supplierId) {
          throw new Error(`Cliente no mapeado: ${o.fk_persona_old}`);
        }

        valuesInsertObligations.push([
          supplierId,
          o.tipo_obl,
          o.fech_emision,
          o.fech_vencimiento,
          o.tip_doc,
          o.estado,
          o.saldo,
          o.total,
          o.ref_secuencia,
          purchaseOrderIdMap[o.fk_cod_trans_old] ?? null,
          o.tipo_cuenta,
          o.fk_id_infcon,
          o.tipo_estado_cuenta,
          auditIdInsert,
          new Date(),
          newCompanyId
        ]);
      };

      const resultCreateObligations: ResultSet = await conn.query(`
			INSERT INTO cuentas_obl(
					FK_PERSONA,
					TIPO_OBL,
					FECH_EMISION,
					FECH_VENCIMIENTO,
					TIP_DOC,
					ESTADO,
					SALDO,
					TOTAL,
					REF_SECUENCIA,
					FK_COD_TRANS,
					TIPO_CUENTA,
					FK_ID_INFCON,
					TIPO_ESTADO_CUENTA,
					FK_AUDITOB,
					OBLG_FEC_REG,
					FK_COD_EMP
			)
			VALUES ?
		`, [valuesInsertObligations]);
      let nextOrderObligationId = (resultCreateObligations[0] as ResultSetHeader).insertId;
      batchObligation.forEach((o: any) => {
        orderObligationIdMap[o.old_id] = nextOrderObligationId++;
      });
    }

    return { orderObligationIdMap, orderObligationAuditIdMap };

  } catch (error) {
    console.error("Error al migrar obligaciones de pedidos de compra:", error);
    throw error;
  }
}

async function migrateOrderMovements({
  legacyConn,
  conn,
  newCompanyId,
  purchaseOrderIdMap,
  purchaseOrderAuditIdMap,
  mapConciliation,
  userMap,
  bankMap,
  boxMap
}: MigrateOrderMovementsParams) {
  try {
    console.log("Migrando movimientos de pedidos de compra...");
    const movementOrderIdMap: Record<number, number> = {};
    const movementsQuery: ResultSet = await legacyConn.query(`
      SELECT
          m.ID_MOVI,
          t.COD_TRAC AS COD_TRANS,
          m.FK_COD_CAJAS_MOVI,
          m.FK_COD_BANCO_MOVI,
          IFNULL(m.TIP_MOVI, t.METPAG_TRAC) AS TIP_MOVI,
          m.periodo_caja,
          IFNULL(m.FECHA_MOVI, t.fecha) AS FECHA_MOVI,
          CASE WHEN t.TIP_TRAC = 'pedido' THEN 'PEDIDO'
      END AS ORIGEN_MOVI,
      IFNULL(m.IMPOR_MOVI, t.TOTPAG_TRAC) AS IMPOR_MOVI,
      IFNULL(m.TIPO_MOVI, t.METPAG_TRAC) AS TIPO_MOVI,
      m.REF_MOVI,
      t.NUM_TRAC AS CONCEP_MOVIMIG,
      CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0
      END AS ESTADO_MOVI,
      IFNULL(
          m.PER_BENE_MOVI,
          IFNULL(mt.PER_BENE_MOVI, 'MIG')
      ) AS PER_BENE_MOVI,
      'EGRESO' AS CAUSA_MOVI,
      CASE WHEN t.TIP_TRAC = 'pedido' THEN 'PEDIDO'
      END AS MODULO,
      IFNULL(m.FECHA_MANUAL, t.FEC_TRAC) AS FECHA_MANUAL,
      m.CONCILIADO,
      m.FK_COD_CX,
      m.SECU_MOVI,
      m.FK_CONCILIADO,
      m.FK_ANT_MOVI,
      IFNULL(
          m.FK_USER_EMP_MOVI,
          t.FK_COD_USU
      ) AS FK_USER_EMP_MOVI,
      IFNULL(m.FK_TRAC_MOVI, t.COD_TRAC) AS FK_TRAC_MOVI,
      mt.NUM_VOUCHER AS NUM_VOUCHER,
      mt.NUM_LOTE AS NUM_LOTE,
      m.CONCEP_MOVI AS OBS_MOVI,
      t.TOTPAG_TRAC,
      NULL AS FK_ASIENTO,
      NULL AS FK_ARQUEO,
      m.RECIBO_CAJA,
      m.NUM_UNIDAD
      FROM
          transacciones t
      LEFT JOIN movimientos m ON
          m.FK_TRAC_MOVI = t.COD_TRAC
      LEFT JOIN movimientos_tarjeta mt ON
          mt.FK_TRAC_MOVI = t.COD_TRAC
      WHERE
          t.TIP_TRAC = 'pedido'
      ORDER BY
          ID_MOVI ASC;
    `);

    const [movements]: any[] = movementsQuery as Array<any>;
    if (movements.length === 0) {
      return { movementOrderIdMap };
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
      const movementValues: any[] = batchMovements.map((m) => {
        const bankId = bankMap[m.FK_COD_BANCO_MOVI];
        const idBoxDetail = boxMap[m.FK_COD_CAJAS_MOVI];
        const transactionId = purchaseOrderIdMap[m.FK_TRAC_MOVI];
        const userId = userMap[m.FK_USER_EMP_MOVI];
        const transAuditId = purchaseOrderAuditIdMap[m.COD_TRANS];
        const idFkConciliation = mapConciliation[m.FK_CONCILIADO] ?? null;
        const idPlanAccount = null;

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
          movementSequence++,
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
          m.TIPO_MOVI === 'TARJETA' ? cardId : null,
          m.RECIBO_CAJA,
          idPlanAccount,
          m.NUM_UNIDAD,
          m.JSON_PAGOS
        ];
      });

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
        movementOrderIdMap[COD_TRANS] = nextMovementId++;
      });
    }
    console.log("✅ Migración de movimientos pedidos completada correctamente");
    return { movementOrderIdMap };
  } catch (error) {
    console.error("Error al migrar movimientos de pedidos de compra:", error);
    throw error;
  }
}

async function migrateAccountingEntriesPurchaseOrder({
  legacyConn,
  conn,
  newCompanyId,
  movementOrderIdMap,
  purchaseOrderIdMap,
  purchaseOrderAuditIdMap,
  mapPeriodo,
}: MigrateAccountingEntriesPurchaseOrderParams): Promise<{ accountingEntryOrderIdMap: Record<number, number> }> {
  try {
    console.log("Migrando asientos contables de pedidos ...");
    const accountingEntryOrderIdMap: Record<number, number> = {};
    const resultAccountingEntries: ResultSet = await legacyConn.query(`
      SELECT
          cod_asiento,
          fecha_asiento AS FECHA_ASI,
          descripcion_asiento AS DESCRIP_ASI,
          numero_asiento AS NUM_ASI,
          origen_asiento AS ORG_ASI,
          debe_asiento AS TDEBE_ASI,
          haber_asiento AS THABER_ASI,
          origen_asiento AS TIP_ASI,
          fk_cod_periodo AS FK_PERIODO,
          fecha_registro_asiento AS FECHA_REG,
          fecha_update_asiento AS FECHA_ACT,
          json_asi AS JSON_ASI,
          res_asiento AS RES_ASI,
          ben_asiento AS BEN_ASI,
          NULL AS FK_AUDIT,
          NULL AS FK_COD_EMP,
          CAST(
              REGEXP_REPLACE(
                  RIGHT(numero_asiento, 9),
                  '[^0-9]',
                  ''
              ) AS UNSIGNED
          ) AS SEC_ASI,
          transacciones.COD_TRAC AS FK_MOVTRAC,
          NULL AS FK_MOV
      FROM
          transacciones
      LEFT JOIN contabilidad_asientos ON contabilidad_asientos.FK_CODTRAC = transacciones.COD_TRAC
      WHERE
          TIP_TRAC = 'pedido' AND contabilidad_asientos.descripcion_asiento NOT LIKE '%(RETENCION%'
      ORDER BY
          transacciones.COD_TRAC;
		`);

    const [accountingEntries]: any[] = resultAccountingEntries as Array<any>;
    if (accountingEntries.length === 0) {
      return { accountingEntryOrderIdMap };
    }

    const BATCH_SIZE = 1000;
    for (let i = 0; i < accountingEntries.length; i += BATCH_SIZE) {
      const batchAccountingEntries = accountingEntries.slice(i, i + BATCH_SIZE);
      const accountingEntryValues = batchAccountingEntries.map((acc) => {
        const transaccionId = purchaseOrderIdMap[acc.FK_MOVTRAC];
        const periodId = mapPeriodo[acc.FK_PERIODO];
        const transAuditId = purchaseOrderAuditIdMap[acc.FK_MOVTRAC];
        const movementId = movementOrderIdMap[acc.FK_MOVTRAC];

        return [
          acc.FECHA_ASI,
          acc.DESCRIP_ASI,
          acc.NUM_ASI,
          acc.ORG_ASI,
          acc.TDEBE_ASI,
          acc.THABER_ASI,
          acc.TIP_ASI,
          periodId,
          acc.FECHA_REG,
          acc.FECHA_ACT,
          acc.JSON_ASI,
          acc.RES_ASI,
          acc.BEN_ASI,
          transAuditId,
          newCompanyId,
          acc.SEC_ASI,
          transaccionId,
          movementId
        ];
      });
      const createAccountingEntries: ResultSet = await conn.query(`
				INSERT INTO accounting_movements(
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
						FK_MOV
				)
				VALUES ?
			`, [accountingEntryValues]);

      let nextAccountingEntryId = (createAccountingEntries[0] as ResultSetHeader).insertId;
      batchAccountingEntries.forEach(({ cod_asiento }) => {
        accountingEntryOrderIdMap[cod_asiento] = nextAccountingEntryId++;
      });
    }
    return { accountingEntryOrderIdMap };
  } catch (error) {
    console.error("Error al migrar asientos contables de pedidos de compra:", error);
    throw error;
  }
}

async function migrateAccountingEntriesPurchaseOrderDetail({
  legacyConn,
  conn,
  newCompanyId,
  mapProject,
  mapCenterCost,
  mapAccounts,
  accountingEntryOrderIdMap
}: MigrateAccountingEntriesPurchaseOrderDetailParams): Promise<{
  accountingEntryOrderDetailIdMap: Record<number, number>
}> {
  try {
    console.log("Migrando detalle de asientos contables de pedidos ...");
    const accountingEntryOrderDetailIdMap: Record<number, number> = {};

    const resultAccountingEntriesDetail: ResultSet = await legacyConn.query(`
			SELECT
					d.cod_detalle_asiento,
					a.fecha_asiento,
					a.cod_asiento AS FK_COD_ASIENTO,
					d.debe_detalle_asiento AS DEBE_DET,
					d.haber_detalle_asiento AS HABER_DET,
					d.fk_cod_plan AS FK_CTAC_PLAN,
					d.fkProyectoCosto AS FK_COD_PROJECT,
					d.fkCentroCosto AS FK_COD_COST
			FROM
					transacciones t
			INNER JOIN contabilidad_asientos a ON
					a.FK_CODTRAC = t.COD_TRAC
			INNER JOIN contabilidad_detalle_asiento d ON
					d.fk_cod_asiento = a.cod_asiento
			WHERE
					t.TIP_TRAC IN('Compra', 'liquidacion') AND a.descripcion_asiento NOT LIKE '%(RETENCION%'
			ORDER BY
					t.COD_TRAC;
		`);

    const [accountingEntriesDetails]: any[] = resultAccountingEntriesDetail as Array<any>;
    if (accountingEntriesDetails.length === 0) {
      return { accountingEntryOrderDetailIdMap };
    }
    console.log(`📦 Total detalle asientos a migrar: ${accountingEntriesDetails.length}`);

    const BATCH_SIZE = 1000;
    let totalDebe: number = 0;
    let totalHaber: number = 0;
    for (let i = 0; i < accountingEntriesDetails.length; i += BATCH_SIZE) {
      const batchAccountingEntries = accountingEntriesDetails.slice(i, i + BATCH_SIZE);
      const accountingEntryValues: any[] = [];
      const totalsMap = new Map<string, any>();

      for (const a of batchAccountingEntries) {
        const planId = mapAccounts[a.FK_CTAC_PLAN];
        const projectId = mapProject[a.FK_COD_PROJECT] ?? null;
        const costCenterId = mapCenterCost[a.FK_COD_COST] ?? null;
        const seatCodeId = accountingEntryOrderIdMap[a.FK_COD_ASIENTO] ?? null;

        if (!planId || !seatCodeId) continue;

        const debe = toNumber(a.DEBE_DET);
        const haber = toNumber(a.HABER_DET);

        accountingEntryValues.push([
          seatCodeId,
          debe,
          haber,
          planId,
          projectId,
          costCenterId
        ]);

        const key = `${newCompanyId}-${planId}-${a.fecha_asiento}`;
        if (!totalsMap.has(key)) {
          totalsMap.set(key, {
            id_plan: planId,
            fecha: a.fecha_asiento,
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
      if (accountingEntryValues.length === 0) {
        console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
        continue;
      }

      const createAccountingEntryDetails: ResultSet = await conn.query(`
				INSERT INTO accounting_movements_det(
						FK_COD_ASIENTO,
						DEBE_DET,
						HABER_DET,
						FK_CTAC_PLAN,
						FK_COD_PROJECT,
						FK_COD_COST
				)
				VALUES ?
			`, [accountingEntryValues]);
      let nextId = (createAccountingEntryDetails[0] as ResultSetHeader).insertId;
      for (const o of batchAccountingEntries) {
        const planId = mapAccounts[o.FK_CTAC_PLAN];
        const seatCodeId = accountingEntryOrderDetailIdMap[o.FK_COD_ASIENTO];
        if (!planId || !seatCodeId) continue;
        accountingEntryOrderDetailIdMap[o.cod_detalle_asiento] = nextId++;
      };
      for (const t of totalsMap.values()) {
        await upsertTotaledEntry(conn, t, newCompanyId);
      }
      console.log(`✅ Batch detalle contable asiento ${i / BATCH_SIZE + 1} procesado`)
    }
    return { accountingEntryOrderDetailIdMap };
  } catch (error) {
    console.error("Error al migrar detalle de asientos contables de pedidos:", error);
    throw error;
  }
}

export async function migratePurchaseOrderObligationDetail({

  
}){

}