/* import { erpPool } from '../config/db';
 */
export async function migrateBranchesForCompany(
  legacyConn: any,
  conn: any,
  newCompanyId: number
): Promise<Record<number, number>> {
  console.log("Migrando sucursales...");

  const [rows] = await legacyConn.query(`SELECT COD_SURC ,DIR_SURC, FECCREA_SURC, FK_COD_EMP, ESTADO, TRANS_SURC, TRANSDATOS_SURC, SUBSTRING(secuencial,1, 3) as NUM_EST_ELEC, SUBSTRING(secuencial,5, 3) AS PUNT_EMI_ELEC  , SUBSTRING(secuencialFisica,1, 3) as NUM_EST_FIS,SUBSTRING(secuencialFisica,5, 3) AS PUNT_EMI_FIS , SUBSTRING(secuencialFisica,9, 9) AS SURC_SEC_FIS, SUBSTRING(secuencial,9, 9) AS  SURC_SEC_ELECT, SUBSTRING(SECUENCIALRETENCION,9, 9) AS  SURC_SEC_RET, SUBSTRING(SURC_SEC_NOTACREDIT,9, 9) AS  SURC_SEC_NOTACREDIT,  SUBSTRING(SURC_SEC_NOTADEBIT,9, 9) AS  SURC_SEC_NOTADEBIT, SUBSTRING(SURC_SEC_GUIAREMI,9, 9) AS  SURC_SEC_GUIAREMI , SUBSTRING(SURC_SEC_LIQCOMP,9, 9) AS  SURC_SEC_LIQCOMP, SUBSTRING(SURC_SEC_COMPINGR,9, 9) AS  SURC_SEC_COMPINGR, SURC_NOMBRE, SURC_DIR,SURC_EMAIL,SURC_TELF, SURC_LOGO from sucursales;`);
  const sucursales = rows as any[];

  if (!sucursales.length) {
    throw new Error(" -> No hay sucursales para migrar.");
  }
  const BATCH_SIZE = 1000;
  const branchMap: Record<number, number> = {};
  for (let i = 0; i < sucursales.length; i += BATCH_SIZE) {
    const batch = sucursales.slice(i, i + BATCH_SIZE);

    const values = batch.map((s) => {
      const transDatos = s.TRANSDATOS_SURC && s.TRANSDATOS_SURC !== ''
        ? s.TRANSDATOS_SURC
        : '[]';
      return [
        s.DIR_SURC,
        s.FECCREA_SURC,
        newCompanyId,
        s.ESTADO,
        s.TRANS_SURC,
        transDatos,
        s.NUM_EST_ELEC,
        s.PUNT_EMI_ELEC,
        s.NUM_EST_FIS,
        s.PUNT_EMI_FIS,
        s.SURC_SEC_FIS,
        s.SURC_SEC_ELECT,
        s.SURC_SEC_RET,
        s.SURC_SEC_NOTACREDIT,
        s.SURC_SEC_NOTADEBIT,
        s.SURC_SEC_GUIAREMI,
        s.SURC_SEC_LIQCOMP,
        s.SURC_SEC_COMPINGR,
        s.SURC_NOMBRE,
        s.SURC_DIR,
        s.SURC_EMAIL,
        s.SURC_TELF,
        s.SURC_LOGO
      ];
    });

    const placeholders = values
      .map(
        () =>
          "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      )
      .join(",");

    try {
      const [res]: any = await conn.query(`
       INSERT INTO branch_offices (
                DIR_SURC, FECCREA_SURC, FK_COD_EMP, ESTADO, TRANS_SURC, TRANSDATOS_SURC,
                NUM_EST_ELEC, PUNT_EMI_ELEC, NUM_EST_FIS, PUNT_EMI_FIS, SURC_SEC_FIS, SURC_SEC_ELECT,
                SURC_SEC_RET, SURC_SEC_NOTACREDIT, SURC_SEC_NOTADEBIT, SURC_SEC_GUIAREMI, SURC_SEC_LIQCOMP,
                SURC_SEC_COMPINGR, SURC_NOMBRE, SURC_DIR, SURC_EMAIL, SURC_TELF, SURC_LOGO
            )  VALUES ${placeholders}
        `,
        values.flat()
      );

      let newId = res.insertId;
      for (const s of batch) {
        branchMap[s.COD_SURC] = newId;
        newId++;
      }
      //CLAVE DE SUCURSAL  COD_SURC  A ID NUEVA DE MIGRACION
      console.log(branchMap);

      console.log(` -> Batch migrado: ${batch.length} sucursales`);
    } catch (err) {
      throw err;
    }
  }

  return branchMap;
}
