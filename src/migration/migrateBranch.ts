import { erpPool } from '../config/db';

export async function migrateBranchesForCompany(
  legacyConn: any,
  newCompanyId: number
): Promise<Record<number, number>> {
  console.log("Migrando sucursales...");

  const [rows] = await legacyConn.query(`SELECT * FROM sucursales`);
  const sucursales = rows as any[];

  if (!sucursales.length) {
    console.log(" -> No hay sucursales para migrar.");
    return {};
  }

  const parseSec = (sec: string | null) => {
    if (!sec) return { est: null, emi: null, num: null };
    sec = sec.replace(/[^0-9]/g, "");
    if (sec.length < 9) return { est: null, emi: null, num: sec };
    return {
      est: sec.substring(0, 3),
      emi: sec.substring(3, 6),
      num: sec.substring(6),
    };
  };

  const BATCH_SIZE = 1000;
  const branchMap: Record<number, number> = {};

  for (let i = 0; i < sucursales.length; i += BATCH_SIZE) {
    const batch = sucursales.slice(i, i + BATCH_SIZE);

    const values = batch.map((s) => {
      const fis = parseSec(s.secuencialFisica);
      const elec = parseSec(s.secuencial);

      return [
        s.DIR_SURC,
        s.FECCREA_SURC,
        newCompanyId,
        s.estado,
        s.TRANS_SURC,
        s.TRANSDATOS_SURC,
        elec.est,
        elec.emi,
        fis.est,
        fis.emi,
        fis.num,
        elec.num,
        s.SECUENCIALRETENCION,
        s.SURC_SEC_NOTACREDIT,
        s.SURC_SEC_NOTADEBIT,
        s.SURC_SEC_GUIAREMI,
        s.SURC_SEC_LIQCOMP,
        s.SURC_SEC_COMPINGR,
        s.SURC_NOMBRE,
        s.SURC_DIR,
        s.SURC_EMAIL,
        s.SURC_TELF,
        s.SURC_LOGO,
      ];
    });

    const placeholders = values
      .map(
        () =>
          "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      )
      .join(",");


    const conn = await erpPool.getConnection();
    try {
      await conn.beginTransaction();

      const [res]: any = await conn.query(
        `
        INSERT INTO branch_offices (
          DIR_SURC, FECCREA_SURC, FK_COD_EMP, ESTADO, TRANS_SURC, TRANSDATOS_SURC,
          NUM_EST_ELEC, PUNT_EMI_ELEC, NUM_EST_FIS, PUNT_EMI_FIS,
          SURC_SEC_FIS, SURC_SEC_ELECT, SURC_SEC_RET, SURC_SEC_NOTACREDIT,
          SURC_SEC_NOTADEBIT, SURC_SEC_GUIAREMI, SURC_SEC_LIQCOMP,
          SURC_SEC_COMPINGR, SURC_NOMBRE, SURC_DIR, SURC_EMAIL, SURC_TELF,
          SURC_LOGO
        )
        VALUES ${placeholders}
        `,
        values.flat()
      );

      await conn.commit();

      let newId = res.insertId;
      for (const s of batch) {
        branchMap[s.COD_SURC] = newId;
        newId++;
      }

      console.log(` -> Batch migrado: ${batch.length} sucursales`);
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  }

  return branchMap;
}
