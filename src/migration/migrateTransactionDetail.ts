import { codigoIvaPorcentaje } from "./migrationTools";

export async function migrateTransactionDetails({
  legacyConn,
  conn,
  newCompanyId,
  storeMap,
  userMap,
  mapProducts,
}) {
  try {
    console.log("Migrando detalle de transacciones...");
    const transactionDetailMap: Record<number, number> = {};
    const [transactionsDetails]: any[] = await legacyConn.query(`
      SELECT
          CAN_DET AS CAN_DET,
          PRE_DET AS PRE_DET,
          SUB_DET AS SUB_DET,
          IVA_DET AS IVA_COD,
          NULL AS IVA_TAR,
          DESC_TRAC AS DESC_TRAC,
          DESCVAL_TRAC AS DESCVAL_TRAC,
          FK_COD_PRO AS FK_PROD_ID,
          NULL AS FK_WHDET_ID,
          NULL AS FK_AUDITDET,
          FECCAR_DET AS FECHA_REG
      FROM
          detalle;
    `);

    if (transactionsDetails.length === 0) {
      return;
    }
    const BATCH_SIZE = 1000;
    for (let i = 0; i < transactionsDetails.length; i += BATCH_SIZE) {
      const batch = transactionsDetails.slice(i, i + BATCH_SIZE);

      const detailValues = batch.map((detail, index: number) => {
        const productId = mapProducts[detail.FK_PROD_ID];
        const ivaCode = codigoIvaPorcentaje[detail.IVA_DET?.toString()] || 0;
        const auditId = null; // detail.FK_AUDITDET

        return [
          detail.CAN_DET,
          detail.PRE_DET,
          detail.SUB_DET,
          detail.IVA_COD,
          ivaCode,
          detail.DESC_TRAC,
          detail.DESCVAL_TRAC,
          productId,
          detail.FK_WHDET_ID,
          auditId,
          detail.FECHA_REG,
        ];
      });

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
       batch.forEach(({ COD_DETID, }) => {
        transactionDetailMap[COD_DETID] = nextId++;
      });
      console.log(` -> Batch migrado: ${batch.length} detalle de transacciones`);
    }
    return { transactionDetailMap };
  } catch (error) {
    console.error("Error al migrar detalle de transacciones:", error);
    throw error;
  }
}
