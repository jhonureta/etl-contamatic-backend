
import { Connection, FieldPacket, ResultSetHeader, RowDataPacket } from "mysql2/promise";

type MigrateKardexParams  = {
  legacyConn: Connection;
	conn: Connection;
  newCompanyId: number;
  storeMap: Record<number, number>;
  idFirstBranch: number;
  transactionIdMap: Record<number, number>;
  transactionIdToAuditIdMap: Record<number, number>;
}

export async function migrateKardex({
  legacyConn,
	conn,
  newCompanyId,
  storeMap,
  transactionIdMap,
  transactionIdToAuditIdMap,
  idFirstBranch
}: MigrateKardexParams){


  const kardexInventory = await legacyConn.query(`
    SELECT 
COD_KARD AS KDX_ID,
INFO_KARD AS DES_KARK,
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
FECH_REGISTRO AS KDX_DATE_DOC, NULL AS FK_COD_EMP,
FK_COD_DET AS FK_PROD_ID,
ÍDTRAC_KARD AS FK_DOC_ID,
FK_USUARIO AS FK_USER_ID,
FK_BOD AS FK_WH_ID, 
NULL AS KDX_KEY,
CANT_PRO_BOD,
VAL_PRO_BOD,
TOT_PRO_BOD,
PREC_VENTA
FROM kardex
    
`)


}