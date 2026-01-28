import { codigoIvaPorcentaje } from "./migrationTools";

export async function migrateTransactionDetails({
  legacyConn,
  conn,
  newCompanyId,
  storeMap,
  userMap,
  mapProducts,
  idFirstBranch,
  transactionIdMap,
  transactionIdToAuditIdMap
}) {
  try {
    console.log("Migrando detalle de transacciones...");
    const transactionDetailMap: Record<number, number> = {};
    const [transactionsDetails]: any[] = await legacyConn.query(`
      SELECT
          CAN_DET AS COD_DETID,
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
          FK_COD_TRA
      FROM
          detalle;
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
        const newProductId = mapProducts[detail?.FK_PROD_ID];
        const ivaCode = codigoIvaPorcentaje[detail?.IVA_TAR] ?? 0;
        const auditId = transactionIdToAuditIdMap[detail?.FK_COD_TRA] ?? null;

        const hasTransaction = detail.FK_COD_TRA !== null && detail.FK_COD_TRA !== 0;
        let whdetId: number | null = hasTransaction ? await findWarehouseDetailIdByTransaction({
          conn,
          legacyConn,
          cache: whdetCache,
          transactionId: Number(detail.FK_COD_TRA),
          legacyProductId: Number(detail.FK_PROD_ID),
          amount: Number(detail.CAN_DET),
          newProductId,
          storeMap
        }): null;

        if (!whdetId) {
          whdetId = await findFirstWarehouseDetailByProduct({
            cache: firstWhdetCache,
            conn,
            newProductId
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
    console.log(`Total cacheados de detalle de transacciones: ${Object.keys(transactionDetailMap).length}`);
    return { transactionDetailMap };
  } catch (error) {
    console.error("Error al migrar detalle de transacciones:", error);
    throw error;
  }
}


async function findWarehouseDetailIdByTransaction({
  cache,
  conn,
  legacyConn,
  transactionId,
  legacyProductId,
  amount,
  newProductId,
  storeMap
}: {
  cache: Map<string, number | null>;
  legacyConn: any;
  conn: any;
  transactionId: number;
  legacyProductId: number;
  newProductId: number;
  amount: number;
  storeMap: Record<number, number>;
}) {
  const key = `${transactionId}:${legacyProductId}`;
  if (cache.has(key)) return cache.get(key);

  const [warehouseData]: any[] = await legacyConn.query(`
    SELECT
        jt.idBodega
    FROM
        transacciones t
    JOIN JSON_TABLE(
            t.DET_TRAC,
            '$[*]' COLUMNS(
                idProducto INT PATH '$.idProducto',
                idBodega INT PATH '$.idBodega',
                cantidad DECIMAL(18, 6) PATH '$.cantidad'
            )
        ) jt
    WHERE
        t.COD_TRAC = ? AND jt.idProducto = ? AND jt.cantidad = ?
    LIMIT 1;
  `, [transactionId, legacyProductId, amount]);

  if (warehouseData.length === 0) {
    cache.set(key, null);
    return null;
  }

  const legacyWarehouseId = warehouseData[0].idBodega;
  const newWarehouseId = storeMap[legacyWarehouseId];

  const [warehouseDetail] = await conn.query(`
    SELECT
        WHDET_ID
    FROM
        warehouse_detail
    WHERE
        FK_PROD_ID = ? AND FK_WH_ID = ?
    ORDER BY
        WHDET_ID ASC
    LIMIT 1;
  `, [newProductId, newWarehouseId]);

  const whdetId = warehouseDetail.length ? warehouseDetail[0].WHDET_ID : null;
  cache.set(key, whdetId);
  return whdetId;
}


async function findFirstWarehouseDetailByProduct({
  conn,
  newProductId,
  cache
}: {
  conn: any;
  newProductId: number;
  cache: Map<number, number | null>;
}) {
  if (cache.has(newProductId)) return cache.get(newProductId);
  const [rows] = await conn.query(`
    SELECT
        WHDET_ID,
        FK_PROD_ID,
        FK_WH_ID,
        PROD_STATE
    FROM
        warehouse_detail
    WHERE
        FK_PROD_ID = ?
    ORDER BY
        WHDET_ID ASC
    LIMIT 1;
  `, [newProductId]);

  const id = rows.length ? rows[0].WHDET_ID : null;
  cache.set(newProductId, id);
  return id;
}