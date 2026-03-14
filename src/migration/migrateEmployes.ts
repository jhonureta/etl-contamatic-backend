/* import { erpPool } from '../config/db';
 */
export async function migrateEmployesForCompany(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {
    console.log("Migrando empleados...");

    const [rows] = await legacyConn.query(`SELECT
    empleado_id AS CUST_ID,
    1 AS FK_COD_EMP,
    'EMPLEADO' AS CUST_TYPE,
    CASE WHEN emp_tipo_identificacion ='cedula' then '05' ELSE '04' END  AS CUST_CODE_CI,
    CASE WHEN emp_tipo_identificacion ='cedula' then 'CEDULA' ELSE emp_tipo_identificacion END AS CUST_TIPCI,
    emp_identificacion AS CUST_CI,
    0 AS CUST_REL,
    emp_nombre AS CUST_NOM,
    CASE WHEN emp_nombre IS NULL OR emp_nombre = '' THEN emp_nombre ELSE emp_nombre
    END AS CUST_NOMCOM,
    'ECUADOR' AS CUST_PAIS,
    NULL AS CUST_PROV,
    NULL AS CUST_CIU,
    NULL CUST_PAR,
    NULL AS CUST_ZON,
    emp_direccion AS CUST_DIR,
    emp_direccion AS CUST_DIR_MAT,
    emp_telefono AS CUST_TELF,
    NULL AS CUST_TEL2,
    NULL AS CUST_TEL3,
    emp_correo AS CUST_EMA,
    NULL AS CUST_EMA2,
    NULL AS CUST_EMA3,
    emp_genero AS CUST_GEN,
    CASE WHEN emp_estado = 'Activo' THEN 'ACTIVO' ELSE 'DESACTIVO' end as CUST_EST,
    emp_fecha_nacimiento as CUST_DATE_BIRTH,
    emp_estado_civil AS CUST_CIVIL_EST,
    emp_fecha_registro AS CUST_FEC_REG,
    emp_banco AS FK_BANK,
    CASE WHEN emp_tipo ='ahorros' then 'AHORROS' ELSE 'CORRIENTE' END as CUST_ACC_TYPE,
    emp_cuenta AS CUST_ACCOUNT,
    emp_referenciabancaria AS CUST_BANK_REF
    FROM
        tbEmpleados
    WHERE
        1;`);
    const employes = rows as any[];
    const mapEmployes: Record<number, number> = {};
    if (!employes.length) {
        return mapEmployes;
    }
    const BATCH_SIZE = 1000;

    for (let i = 0; i < employes.length; i += BATCH_SIZE) {
        const batch = employes.slice(i, i + BATCH_SIZE);

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
                u.CUST_FECH_REG || new Date(),
                u.CUST_DATE_BIRTH,
                u.FK_BANK,
                u.CUST_ACCOUNT,
                u.CUST_ACC_TYPE,
                u.CUST_BANK_REF
            ];
        });



        try {
            const [res]: any = await conn.query(`
       INSERT INTO customers (
                             FK_COD_EMP, CUST_TYPE, CUST_TIPCI, CUST_CODE_CI, CUST_CI, CUST_REL, CUST_NOM, 
                            CUST_NOMCOM, CUST_PAIS, CUST_PROV, CUST_CIU, CUST_PAR, CUST_ZON, CUST_DIR, CUST_DIR_MAT,
                            CUST_TELF, CUST_TEL2, CUST_TEL3, CUST_EMA, CUST_EMA2, CUST_EMA3, CUST_EST,
                            CUST_COUNTRY_EXTR, CUST_EXTR, CUST_FECH_REG,CUST_DATE_BIRTH,FK_BANK,CUST_ACCOUNT,CUST_ACC_TYPE,CUST_BANK_REF) VALUES ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapEmployes[s.CUST_ID] = newId;
                newId++;
            }
            //CLAVE DE SUCURSAL  COD_SURC  A ID NUEVA DE MIGRACION

            console.log(` -> Batch migrado: ${batch.length} empleados`);
        } catch (err) {
            throw err;
        }
    }

    return mapEmployes;
}