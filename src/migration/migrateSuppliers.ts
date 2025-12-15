/* import { erpPool } from '../config/db';
 */
export async function migrateSuppliersForCompany(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {
    console.log("Migrando proveedores...");

    const [rows] = await legacyConn.query(`SELECT
                                    ID_PROV AS CUST_ID,
                                    1 AS FK_COD_EMP,
                                    'PROVEEDOR' AS CUST_TYPE,
                                    TIPIDE_PROV AS CUST_TIPCI,
                                    NULL AS CUST_CODE_CI,
                                    TIPIDE_PROV AS CUST_CI,
                                    NULL AS CUST_REL,
                                    NOM_PROV AS CUST_NOM,
                                    CASE WHEN NOMC_PROV IS NULL OR NOMC_PROV = '' THEN NOM_PROV ELSE NOMC_PROV
                                END AS CUST_NOMCOM,
                                'ECUADOR' AS CUST_PAIS,
                                NULL AS CUST_PROV,
                                NULL AS CUST_CIU,
                                NULL CUST_PAR,
                                NULL AS CUST_ZON,
                                DIRM_PROV AS CUST_DIR,
                                DIRE_PROV AS CUST_DIR_MAT,
                                TEL1_PROV AS CUST_TELF,
                                TEL1_PROV AS CUST_TEL2,
                                TEL3_PROV AS CUST_TEL3,
                                EMA1_PROV AS CUST_EMA,
                                EMA2_PROV AS CUST_EMA2,
                                EMA3_PROV AS CUST_EMA3,
                                NULL AS CUST_GEN,
                                CASE WHEN EST_PROV = 1 THEN 'ACTIVO' ELSE 'DESACTIVO'
                                END AS CUST_EST,
                                NULL AS CUST_GRUP,
                                NULL AS CUST_VEND,
                                NULL AS CUST_INGR,
                                NULL AS CUST_COUNTRY_EXTR,
                                PROV_EXTRAN AS CUST_EXTR,
                                (
                                SELECT
                                    NOW()) AS CUST_FECH_REG,
                                    NULL AS CUST_NAME_EXT,
                                    NULL AS CUST_ADDR_EXT,
                                    NULL AS CUST_CI_EXT,
                                    NULL AS CUST_TIPCI_EXT,
                                    NULL AS CUST_CODE_CI_EXT,
                                    NULL CUST_FACT,
                                    NULL AS CUST_CIVIL_EST
                                FROM
                                    proveedores;`);
    const proveedores = rows as any[];

    if (!proveedores.length) {
        throw new Error(" -> No hay proveedores para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapSuppliers: Record<number, number> = {};
    for (let i = 0; i < proveedores.length; i += BATCH_SIZE) {
        const batch = proveedores.slice(i, i + BATCH_SIZE);

        const values = batch.map(u => {

            let CUST_TIPCI = '';
            let CUST_CODE_CI = '';

            if (u.CUST_NOM === 'CONSUMIDOR FINAL') {
                CUST_TIPCI = 'CF';
                CUST_CODE_CI = '07';
            } else {
                if (!u.CUST_TIPCI || u.CUST_TIPCI === '') {
                    CUST_TIPCI = u.CUST_CI.length === 10 ? 'CEDULA'
                        : u.CUST_CI.length === 13 ? 'RUC'
                            : 'PASAPORTE';
                    CUST_CODE_CI = u.CUST_CI.length === 10 ? '05'
                        : u.CUST_CI.length === 13 ? '04'
                            : '06';
                } else {
                    CUST_CODE_CI = u.CUST_CI.length === 10 ? '05'
                        : u.CUST_CI.length === 13 ? '04'
                            : '06';
                    CUST_TIPCI = u.CUST_TIPCI;
                }
            }

            return [
                newCompanyId,
                u.CUST_TYPE,
                CUST_TIPCI,
                CUST_CODE_CI,
                u.CUST_CI,
                u.CUST_REL,
                u.CUST_NOM?.toUpperCase() || 'SIN NOMBRE',
                u.CUST_NOMCOM?.toUpperCase() || '',
                u.CUST_PAIS?.toUpperCase() || 'ECUADOR',
                u.CUST_PROV?.toUpperCase() || null,
                u.CUST_CIU?.toUpperCase() || null,
                u.CUST_PAR?.toUpperCase() || null,
                u.CUST_ZON?.toUpperCase() || null,
                u.CUST_DIR || null,
                u.CUST_DIR_MAT || null,
                u.CUST_TELF || null,
                u.CUST_TEL2 || null,
                u.CUST_TEL3 || null,
                u.CUST_EMA || null,
                u.CUST_EMA2 || null,
                u.CUST_EMA3 || null,
                u.CUST_EST || 1,
                u.CUST_COUNTRY_EXTR || null,
                u.CUST_EXTR === '' ? 0 : 1,
                u.CUST_FECH_REG || new Date()
            ];
        });



        try {
            const [res]: any = await conn.query(`
       INSERT INTO customers (
                             FK_COD_EMP, CUST_TYPE, CUST_TIPCI, CUST_CODE_CI, CUST_CI, CUST_REL, CUST_NOM, 
                            CUST_NOMCOM, CUST_PAIS, CUST_PROV, CUST_CIU, CUST_PAR, CUST_ZON, CUST_DIR, CUST_DIR_MAT,
                            CUST_TELF, CUST_TEL2, CUST_TEL3, CUST_EMA, CUST_EMA2, CUST_EMA3, CUST_EST,
                            CUST_COUNTRY_EXTR, CUST_EXTR, CUST_FECH_REG
                            ) VALUES ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapSuppliers[s.CUST_ID] = newId;
                newId++;
            }
            //CLAVE DE SUCURSAL  COD_SURC  A ID NUEVA DE MIGRACION
            /*   console.log(mapClients); */

            console.log(` -> Batch migrado: ${batch.length} proveedores`);
        } catch (err) {
            throw err;
        }
    }

    return mapSuppliers;
}
