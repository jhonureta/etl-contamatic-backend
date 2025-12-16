/* import { erpPool } from '../config/db';
 */
export async function migrateClientsForCompany(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {
    console.log("Migrando clientes...");

    const [rows] = await legacyConn.query(`SELECT
                                    COD_CLI AS CUST_ID,
                                    1 AS FK_COD_EMP,
                                    'CLIENTE' AS CUST_TYPE,
                                    TIP_IDENCLI AS CUST_TIPCI,
                                    NULL AS CUST_CODE_CI,
                                    CED_CLI AS CUST_CI,
                                    REL_CLI AS CUST_REL,
                                    NOM_CLI AS CUST_NOM,
                                    CASE 
                                        WHEN NOMCOM_CLI IS NULL OR NOMCOM_CLI = '' THEN NOM_CLI 
                                        ELSE NOMCOM_CLI 
                                    END AS CUST_NOMCOM,
                                    'ECUADOR' AS CUST_PAIS,
                                    PROV_CLI AS CUST_PROV,
                                    CASE 
                                        WHEN CIU_CLI = 'Seleccione una ciudad' THEN NULL 
                                        ELSE CIU_CLI 
                                    END AS CUST_CIU,
                                    CASE 
                                        WHEN PAR_CLI = 'Seleccione una parroquia' THEN NULL 
                                        ELSE PAR_CLI 
                                    END AS CUST_PAR,
                                    CASE WHEN ZON_CLI = '' THEN NULL ELSE ZON_CLI END AS CUST_ZON,
                                    CASE WHEN DIR_CLI = '' THEN NULL ELSE DIR_CLI END AS CUST_DIR,
                                    CASE WHEN DIR_CLI = '' THEN NULL ELSE DIR_CLI END AS CUST_DIR_MAT,
                                    CASE WHEN TEL_CLI = '' THEN NULL ELSE TEL_CLI END AS CUST_TELF,
                                    CASE WHEN TEL2_CLI = '' THEN NULL ELSE TEL2_CLI END AS CUST_TEL2,
                                    CASE WHEN TEL3_CLI = '' THEN NULL ELSE TEL3_CLI END AS CUST_TEL3,
                                    CASE WHEN EMA_CLI = '' THEN NULL ELSE EMA_CLI END AS CUST_EMA,
                                    CASE WHEN EMA2_CLI = '' THEN NULL ELSE EMA2_CLI END AS CUST_EMA2,
                                    CASE WHEN EMA3_CLI = '' THEN NULL ELSE EMA3_CLI END AS CUST_EMA3,
                                    CASE WHEN GEN_CLI = '' THEN NULL ELSE GEN_CLI END AS CUST_GEN,
                                    'ACTIVO' AS CUST_EST,
                                    CASE 
                                        WHEN GRU_CLI IS NULL OR GRU_CLI = '' THEN 'SIN GRUPO' 
                                        ELSE GRU_CLI 
                                    END AS CUST_GRUP,
                                    CASE WHEN VEN_CLI = '' THEN NULL ELSE VEN_CLI END AS CUST_VEND,
                                    CASE 
                                        WHEN INGR_CLI IS NULL OR INGR_CLI = '' THEN 'NACIONALES' 
                                        ELSE INGR_CLI 
                                    END AS CUST_INGR,
                                    CASE WHEN pais_residencia = '' THEN NULL ELSE pais_residencia END AS CUST_COUNTRY_EXTR,
                                    CASE WHEN pais_residencia IS NULL OR pais_residencia = '' THEN 0 ELSE pais_residencia END AS CUST_EXTR,
                                    FECH_REG AS CUST_FECH_REG,
                                    CASE WHEN nombreAdicional = '' THEN NULL ELSE nombreAdicional END AS CUST_NAME_EXT,
                                    CASE WHEN direccionAdicional = '' THEN NULL ELSE direccionAdicional END AS CUST_ADDR_EXT,
                                    CASE WHEN identificacionAdicional = '' THEN NULL ELSE identificacionAdicional END AS CUST_CI_EXT,
                                    TIP_CLI AS CUST_TIPCI_EXT,
                                    NULL AS CUST_CODE_CI_EXT,
                                    CASE 
                                        WHEN direccionAdicional IS NOT NULL AND direccionAdicional != '' THEN 1 
                                        ELSE 0 
                                    END AS CUST_FACT,
                                    EST_CLI AS CUST_CIVIL_EST
                                FROM
                                    clientes;`);
    const clientes = rows as any[];

    if (!clientes.length) {
        throw new Error(" -> No hay clientes para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapClients: Record<number, number> = {};
    for (let i = 0; i < clientes.length; i += BATCH_SIZE) {
        const batch = clientes.slice(i, i + BATCH_SIZE);

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

            let CUST_TIPCI_EXT = null;
            let CUST_CODE_CI_EXT = null;

            if (u.CUST_CI_EXT && u.CUST_CI_EXT !== '') {
                CUST_TIPCI_EXT = u.CUST_CI_EXT.length === 10 ? 'CEDULA'
                    : u.CUST_CI_EXT.length === 13 ? 'RUC'
                        : 'PASAPORTE';
                CUST_CODE_CI_EXT = u.CUST_CI_EXT.length === 10 ? '05'
                    : u.CUST_CI_EXT.length === 13 ? '04'
                        : '06';
            } else {

                CUST_TIPCI_EXT = u.CUST_TIPCI_EXT;
                CUST_CODE_CI_EXT = u.CUST_CODE_CI_EXT;
            }

            return [
                newCompanyId,
                u.CUST_TYPE,
                CUST_TIPCI,
                CUST_CODE_CI,
                u.CUST_CI,
                u.CUST_REL,
                u.CUST_NOM?.toUpperCase() || 'SIN NOMBRE',
                u.CUST_NOMCOM?.toUpperCase() || null,
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
                u.CUST_GEN === 'masculino' ? 'MASCULINO' : 'FEMENINO',
                u.CUST_EST || 1,
                u.CUST_GRUP || null,
                u.CUST_VEND === 'Seleccione...' ? null : u.CUST_VEND,
                u.CUST_INGR || null,
                u.CUST_COUNTRY_EXTR || null,
                u.CUST_EXTR === null ? 0 : 1,
                u.CUST_FECH_REG || new Date(),
                u.CUST_NAME_EXT || null,
                u.CUST_ADDR_EXT || null,
                u.CUST_CI_EXT || null,
                CUST_TIPCI_EXT,
                CUST_CODE_CI_EXT,
                u.CUST_FACT === '' ? 0 : 1,
                u.CUST_CIVIL_EST === 'soltero'
                    ? 'SOLTERO'
                    : u.CUST_CIVIL_EST === 'casado'
                        ? 'CASADO'
                        : u.CUST_CIVIL_EST === 'viudo'
                            ? 'VIUDO'
                            : 'SOLTERO',
                u.CUST_TELF && u.CUST_TELF !== '' ? '593' : null
            ];
        });



        try {
            const [res]: any = await conn.query(`
       INSERT INTO customers (FK_COD_EMP, CUST_TYPE, CUST_TIPCI, CUST_CODE_CI, CUST_CI, CUST_REL, CUST_NOM, 
                    CUST_NOMCOM, CUST_PAIS, CUST_PROV, CUST_CIU, CUST_PAR, CUST_ZON, CUST_DIR, CUST_DIR_MAT,
                    CUST_TELF, CUST_TEL2, CUST_TEL3, CUST_EMA, CUST_EMA2, CUST_EMA3, CUST_GEN, CUST_EST,
                    CUST_GRUP, CUST_VEND, CUST_INGR, CUST_COUNTRY_EXTR, CUST_EXTR, CUST_FECH_REG, CUST_NAME_EXT,
                    CUST_ADDR_EXT, CUST_CI_EXT, CUST_TIPCI_EXT, CUST_CODE_CI_EXT, CUST_FACT, CUST_CIVIL_EST, CUST_TELF_CODE) VALUES  ?`,
                [values]
            ); 

            let newId = res.insertId;
            for (const s of batch) {
                mapClients[s.CUST_ID] = newId;
                newId++;
            }
            //CLAVE DE SUCURSAL  COD_SURC  A ID NUEVA DE MIGRACION
            console.log(` -> Batch migrado: ${batch.length} sucursales`);
        } catch (err) {
            throw err;
        }
    }

    return mapClients;
}
