
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
  mapTransfers: Record<number, number>;
  mapProducts: Record<number, number>;
  mapPhysical: Record<number, number>;
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
  mapTransfers,
  mapPhysical,
  idFirstBranch
}: MigrateKardexParams) {
  try {
    console.log("Migrando kardex inventario...");
    const kardexIdMap: Record<number, number> = {};
    const [kardexInventory]: any[] = await legacyConn.query(`
      SELECT
          COD_KARD AS KDX_ID,
      CASE
        WHEN COALESCE(CANT_ING, 0) > 0 THEN 'ENTRADA'
        WHEN COALESCE(CANT_EGR, 0) > 0 THEN 'SALIDA'
        ELSE NULL
      END AS KDX_TYPE,
      CASE
        WHEN DES_KARK LIKE 'SE DEVUELVE POR MODIFICACION DE VENTA%' THEN 'VENTA'
        WHEN DES_KARK LIKE 'VENTA DE MERCADERIA CON FACTURA%' THEN 'VENTA'
        WHEN DES_KARK LIKE 'SE DEVUELVE POR LOS POSIBLES MOTIVOS%'THEN 'VENTA'
        WHEN DES_KARK LIKE 'ORDEN DE VENTA%' THEN 'ORDEN VENTA'
        WHEN DES_KARK LIKE 'DEVOLUCION (NC) DE MERCADERIA%' THEN 'NOTA CREDITO VENTA'
        WHEN DES_KARK LIKE 'DEVOLUCION DE MERCADERIA A FACTURA DE VENTA%' THEN 'NOTA CREDITO VENTA'
        WHEN DES_KARK LIKE 'SE DEVUELVE POR ELIMINACION DE NOTA DE CREDITO DE VENTA%' THEN 'NOTA CREDITO VENTA'
        WHEN DES_KARK LIKE 'SE DEVUELVE POR MODIFICACION DE COMPRA%' THEN 'COMPRA'            
        WHEN DES_KARK LIKE 'COMPRA DE MERCADERIA CON FACTURA%' THEN 'COMPRA' 
        WHEN DES_KARK LIKE 'INGRESO DE MERCADERIA CON PEDIDO%' THEN 'PEDIDO' 
        WHEN INFO_KARD = 'APERTURA' THEN 'APERTURA' 
        WHEN DES_KARK LIKE 'SALIDA DE MERCADERIA MEDIANTE TOMA FISICA%' THEN 'TOMA FISICA'             
        WHEN DES_KARK LIKE 'TOMA FISICA%' THEN 'TOMA FISICA'
        WHEN DES_KARK LIKE 'APERTURA DE STOCK%' THEN 'TOMA FISICA'
        WHEN DES_KARK LIKE 'TRANSFERENCIA%' THEN 'TRANSFERENCIA'     
        WHEN DES_KARK LIKE 'ENTRADA DE MERCADERIA CON INGRESO%' THEN 'INGRESO MERCADERIA' 
        WHEN INFO_KARD = 'NOTA DE CREDITO VENTA' THEN 'NOTA CREDITO VENTA'
        WHEN INFO_KARD = 'LIQUIDACION DE COMPRA' THEN 'LIQUIDACION'
        WHEN INFO_KARD IN ('NOTA DE CREDITO COMPRA DESC', 'NOTA DE CREDITO COMPRA') THEN 'NOTA CREDITO COMPRA'
        WHEN INFO_KARD = 'COMPROBANTE DE INGRESO' THEN 'COMPROBANTE INGRESO'
        WHEN INFO_KARD = 'ORDEN DE VENTA' THEN 'ORDEN VENTA'
        ELSE INFO_KARD
      END AS KDX_TYPEDOC,
      DES_KARK AS KDX_DESCR,
      COALESCE(NULLIF(CANT_ING, ''), 0)  AS KDX_CANT_IN,
      COALESCE(NULLIF(VAL_ING, ''), 0) AS KDX_VAL_IN,
      COALESCE(NULLIF(TOT_ING, ''), 0) AS KDX_TOTAL_IN,
      COALESCE(NULLIF(CANT_EGR, ''), 0) AS KDX_CANT_OUT,
      COALESCE(NULLIF(VAL_EGR, ''), 0) AS KDX_VAL_OUT,
      COALESCE(NULLIF(TOT_EGR, ''), 0) AS KDX_TOTAL_OUT,
      COALESCE(NULLIF(CANT_PRO, ''), 0) AS KDX_CANT_AVG,
      COALESCE(NULLIF(VAL_PRO, ''), 0) AS KDX_VAL_AVG,
      COALESCE(NULLIF(TOT_PRO, ''), 0) AS KDX_TOTAL_AVG,
      FECH_REGISTRO AS FECH_REGISTRO,
      FECH_REGISTRO AS KDX_DATE_DOC,
      NULL AS FK_COD_EMP,
      FK_COD_DET AS FK_PROD_ID,
      ÍDTRAC_KARD AS FK_DOC_ID,
      FK_USUARIO AS FK_USER_ID,
      CANT_PRO_BOD AS STOCK_WAREHOUSE,
      VAL_PRO_BOD AS AVERAGE_WAREHOUSE,
      TOT_PRO_BOD AS TOTAL_WAREHOUSE,
      FK_BOD AS FK_WH_ID,
      PREC_VENTA,
      REGEXP_SUBSTR(
          DES_KARK,
          '([A-Z]{2,}-[0-9]+|[0-9]{3}-[0-9]{3}-[0-9]+)'
      ) AS SECUENCIAL,
      NULL AS KDX_KEY
      FROM
          kardex;
  `);

    if (kardexInventory.length === 0) {
      return { kardexIdMap };
    }

    const BATCH_SIZE = 1000;

    for (let i = 0; i < kardexInventory.length; i += BATCH_SIZE) {
      const batch = kardexInventory.slice(i, i + BATCH_SIZE);

      const kardexValues = batch.map((kardex: any, index: number) => {

        const productId   = mapProducts[kardex.FK_PROD_ID];
        const userId      = userMap[kardex.FK_USER_ID];
        const warehouseId = storeMap[kardex.FK_WH_ID];

         const transactionId = resolveTransactionId(kardex, {
          mapPhysical,
          mapTransfers,
          transactionIdMap
        });

        return [
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
          kardex.STOCK_WAREHOUSE,
          kardex.AVERAGE_WAREHOUSE,
          kardex.TOTAL_WAREHOUSE,
          warehouseId,
          null
        ];
      })

      const [resultCreateKardex]: any = await conn.query(`
        INSERT INTO kardex(
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
            STOCK_WAREHOUSE,
            AVERAGE_WAREHOUSE,
            TOTAL_WAREHOUSE,
            FK_WH_ID,
            KDX_KEY
        )
        VALUES ?
      `, [kardexValues]);
      let nextId = resultCreateKardex.insertId;
      batch.forEach(({ KDX_ID }) => {
        resultCreateKardex[KDX_ID] = nextId++;
      });
      console.log(` -> Batch migrado: ${batch.length} kardex`);
    }
    return { kardexIdMap };
  } catch (error) {
    console.error("Error al migrar kardex:", error);
    throw error;
  }
}


function resolveTransactionId(kardex, {
  mapPhysical,
  mapTransfers,
  transactionIdMap
}) {
  if (kardex.KDX_TYPEDOC === 'TOMA FISICA') {
    return mapPhysical[kardex.SECUENCIAL] ?? null;
  }

  if (kardex.KDX_TYPEDOC === 'TRANSFERENCIA') {
    return mapTransfers[kardex.SECUENCIAL] ?? null;
  }

  return transactionIdMap[kardex.FK_DOC_ID] ?? null;
}