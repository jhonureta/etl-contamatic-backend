import { codigoIvaPorcentaje } from "./migrationTools";
import { toJSONArray } from "./purchaseHelpers";

export async function migrateTransactionDetails({
  legacyConn,
  conn,
  mapProducts,
  transactionIdToAuditIdMap,
  prodWareDetailIdMap,
  storeMap
}) {
  try {
    console.log("Migrando detalle de transacciones...");
    const transactionDetailMap: Record<number, number> = {};
    const [transactionsDetails]: any[] = await legacyConn.query(`
        SELECT
          COD_DET AS COD_DETID,
          CAN_DET AS CAN_DET,
          PRE_DET AS PRE_DET,
          SUB_DET AS SUB_DET,
          NULL AS IVA_COD,
          IVA_DET AS IVA_TAR,
          DESC_TRAC AS DESC_TRAC,
          DESCVAL_TRAC AS DESCVAL_TRAC,
          FK_COD_PRO AS FK_PROD_ID,
          NULL AS FK_WHDET_ID,
          NULL AS FK_AUDITDET,
          FECCAR_DET AS FECHA_REG,
          FK_COD_TRA,
          CASE 
          	WHEN transacciones.TIP_TRAC = 'Compra' THEN transacciones.DET_TRACCOM
          	WHEN transacciones.TIP_TRAC = 'liquidacion' THEN transacciones.DET_TRACCOM
          	ELSE transacciones.DET_TRAC
          END AS DETALLE
      FROM
          detalle
      LEFT JOIN transacciones ON detalle.FK_COD_TRA = transacciones.COD_TRAC;
    `);

    if (transactionsDetails.length === 0) {
      return;
    }

    const whdetCache = new Map<string, number | null>();      // Key compuesto
    const firstWhdetCache = new Map<number, number | null>(); // Por producto

    const BATCH_SIZE = 1000;
    for (let i = 0; i < transactionsDetails.length; i += BATCH_SIZE) {
      const batch = transactionsDetails.slice(i, i + BATCH_SIZE);
      const detailValues: any = [];
      for (const detail of batch) {
        const oldProductId = detail?.FK_PROD_ID;
        const newProductId = mapProducts[oldProductId];
        const ivaCode = codigoIvaPorcentaje[detail?.IVA_TAR] ?? 0;
        const auditId = transactionIdToAuditIdMap[detail?.FK_COD_TRA] ?? null;

        const transactionDetail = toJSONArray(detail?.DETALLE);

        const { oldWarehouseId } = findWarehouseDetailIdByTransaction({
          productId: oldProductId,
          detail: transactionDetail
        });

        let whdetId = prodWareDetailIdMap[`${oldProductId}:${oldWarehouseId}`] ?? null;
        if(!whdetId && newProductId && storeMap[oldWarehouseId]) {
          whdetId = await insertWarehouseDetail({
            conn,
            productId: newProductId,
            warehouseId: storeMap[oldWarehouseId]
          });
        }

        detailValues.push([
          detail.CAN_DET,
          detail.PRE_DET,
          detail.SUB_DET,
          ivaCode,
          detail.IVA_TAR,
          detail.DESC_TRAC,
          detail.DESCVAL_TRAC,
          newProductId,
          whdetId,
          auditId,
          detail.FECHA_REG
        ]);
      };

      const [resultCreateDetails] = await conn.query(`
        INSERT INTO transaction_details(
            CAN_DET,
            PRE_DET,
            SUB_DET,
            IVA_COD,
            IVA_TAR,
            DESC_TRAC,
            DESCVAL_TRAC,
            FK_PROD_ID,
            FK_WHDET_ID,
            FK_AUDITDET,
            FECHA_REG
        )
        VALUES ?`,
        [detailValues],
      );
      let nextId = resultCreateDetails.insertId;
      batch.forEach(({ COD_DET }) => {
        transactionDetailMap[COD_DET] = nextId++;
      });
      console.log(` -> Batch migrado: ${batch.length} detalle de transacciones`);
    }
    return { transactionDetailMap };
  } catch (error) {
    console.error("Error al migrar detalle de transacciones:", error);
    throw error;
  }
}

function findWarehouseDetailIdByTransaction({
  productId,
  detail
}: {
  productId: number;
  detail: any[];
}) {
  const productDetail = detail.find(item => 
    Number(item.idProducto) === Number(productId)
  );
  const warehouseId = productDetail ? Number(productDetail.idBodega) : null;
  return { oldWarehouseId: warehouseId };
}

async function insertWarehouseDetail({
  conn,
  productId,
  warehouseId
}) {
  const [res]: any = await conn.query(`
    INSERT INTO warehouse_detail(  
        WHDET_STOCK,
        FK_PROD_ID, 
        FK_WH_ID, 
        PROD_STATE) VALUES  (?, ?, ?, ?)`,
    [0, productId, warehouseId, 1]
  );
  let newId = res.insertId;
  return newId;
}