
import { Connection, FieldPacket, ResultSetHeader, RowDataPacket } from "mysql2/promise";

type MigrateKardexParams = {
  legacyConn: Connection;
  conn: Connection;
  newCompanyId: number;
  storeMap: Record<number, number>;
  idFirstBranch: number;
  transactionIdMap: Record<number, number>;
  transactionIdToAuditIdMap: Record<number, number>;
  userMap: Record<number, number>;
  mapProducts: Record<number, number>;
}

export async function migrateKardex({
  legacyConn,
  conn,
  newCompanyId,
  storeMap,
  userMap,
  mapProducts,
  transactionIdMap,
  transactionIdToAuditIdMap,
  idFirstBranch
}: MigrateKardexParams) {
  try {
    console.log("Migrando kardex inventario...");
    const [kardexInventory]: any[] = await legacyConn.query(`
      SELECT
          COD_KARD AS KDX_ID,
          CASE WHEN DES_KARK LIKE 'ENTRADA%' THEN 'ENTRADA' WHEN DES_KARK LIKE 'SALIDA%' THEN 'SALIDA'
      END AS KDX_TYPE,
      INFO_KARD AS KDX_TYPEDOC,
      DES_KARK AS KDX_DESCR,
      CANT_ING AS KDX_CANT_IN,
      VAL_ING AS KDX_VAL_IN,
      TOT_ING AS KDX_TOTAL_IN,
      CANT_EGR AS KDX_CANT_OUT,
      VAL_EGR AS KDX_VAL_OUT,
      TOT_EGR AS KDX_TOTAL_OUT,
      CANT_PRO AS KDX_CANT_AVG,
      VAL_PRO AS KDX_VAL_AVG,
      TOT_PRO AS KDX_TOTAL_AVG,
      FECH_REGISTRO AS FECH_REGISTRO,
      FECH_REGISTRO AS KDX_DATE_DOC,
      NULL AS FK_COD_EMP,
      FK_COD_DET AS FK_PROD_ID,
      ÍDTRAC_KARD AS FK_DOC_ID,
      FK_USUARIO AS FK_USER_ID,
      FK_BOD AS FK_WH_ID,
      NULL AS KDX_KEY,
      CANT_PRO_BOD,
      VAL_PRO_BOD,
      TOT_PRO_BOD,
      PREC_VENTA
      FROM
          kardex;
      `);

    if (kardexInventory.length === 0) {
      return;
    }

    const BATCH_SIZE = 1000;

    for (let i = 0; i < kardexInventory.length; i += BATCH_SIZE) {
      const batch = kardexInventory.slice(i, i + BATCH_SIZE);

      const kardexValues = batch.flatMap((kardex: any) => {

        const productId = mapProducts[kardex.FK_PROD_ID];
        const userId = userMap[kardex.FK_USER_ID];
        const warehouseId = storeMap[kardex.FK_WH_ID];
        const transactionId = transactionIdMap[kardex.FK_DOC_ID];

        const keyGeneral = `G`;
        const keyBodega = `W`;
        
        // Mapeo de kardex para doble registro como lo tiene el sistema actual
        return [
          [
            kardex.KDX_TYPE,
            kardex.KDX_TYPEDOC,
            kardex.KDX_DESCR,
            kardex.KDX_CANT_IN,
            kardex.KDX_VAL_IN,
            kardex.KDX_TOTAL_IN,
            kardex.KDX_CANT_OUT,
            kardex.KDX_VAL_OUT,
            kardex.KDX_TOTAL_OUT,
            kardex.CANT_PRO_BOD,
            kardex.VAL_PRO_BOD,
            kardex.TOT_PRO_BOD,
            kardex.KDX_DATE_REG,
            kardex.KDX_DATE_DOC,
            newCompanyId,
            productId,
            transactionId,
            userId,
            warehouseId,
            keyBodega
          ],
          [
            kardex.KDX_TYPE,
            kardex.KDX_TYPEDOC,
            kardex.KDX_DESCR,
            kardex.KDX_CANT_IN,
            kardex.KDX_VAL_IN,
            kardex.KDX_TOTAL_IN,
            kardex.KDX_CANT_OUT,
            kardex.KDX_VAL_OUT,
            kardex.KDX_TOTAL_OUT,
            kardex.KDX_CANT_AVG,
            kardex.KDX_VAL_AVG,
            kardex.KDX_TOTAL_AVG,
            kardex.KDX_DATE_REG,
            kardex.KDX_DATE_DOC,
            newCompanyId,
            productId,
            transactionId,
            userId,
            warehouseId,
            keyGeneral
          ]
        ];

      })

      const [resultCreateKardex]: any = await conn.query(`
        INSERT INTO kardex(
            KDX_ID,
            KDX_TYPE,
            KDX_TYPEDOC,
            KDX_DESCR,
            KDX_CANT_IN,
            KDX_VAL_IN,
            KDX_TOTAL_IN,
            KDX_CANT_OUT,
            KDX_VAL_OUT,
            KDX_TOTAL_OUT,
            KDX_CANT_AVG,
            KDX_VAL_AVG,
            KDX_TOTAL_AVG,
            KDX_DATE_REG,
            KDX_DATE_DOC,
            FK_COD_EMP,
            FK_PROD_ID,
            FK_DOC_ID,
            FK_USER_ID,
            FK_WH_ID,
            KDX_KEY
        )
        VALUES ?
      `, [kardexValues]);
    }
  } catch (error) {
    console.error("Error al migrar kardex:", error);
    throw error;
  }
}