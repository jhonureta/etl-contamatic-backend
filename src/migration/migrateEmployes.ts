/* import { erpPool } from '../config/db';
 */
export async function migrateEmployesForCompany(
    legacyConn: any,
    conn: any,
    humanResourcesDb: any,
    newCompanyId: number
): Promise<Record<number, number>> {


    const [dbRhh] = await humanResourcesDb.query(
        `SELECT * FROM tbEmpresa WHERE empresa_codigo = ?`,
        [newCompanyId],
    );

    const empresas = dbRhh as any[];

    const mapEmployes: Record<number, number> = {};
    if (!empresas.length) {
        return mapEmployes;
    }

    const empresa = empresas[0];
    const idEmpresaRhh = empresa.empresa_id;

    console.log("Migrando empleados...");
    const [rows] = await humanResourcesDb.query(`SELECT
    e.empleado_id AS CUST_ID,
    1 AS FK_COD_EMP,
    'EMPLEADO' AS CUST_TYPE,
    CASE WHEN e.emp_tipo_identificacion ='cedula' then '05' ELSE '04' END  AS CUST_CODE_CI,
    CASE WHEN e.emp_tipo_identificacion ='cedula' then 'CEDULA' ELSE e.emp_tipo_identificacion END AS CUST_TIPCI,
    e.emp_identificacion AS CUST_CI,
    0 AS CUST_REL,
    e.emp_nombre AS CUST_NOM,
    e.emp_nombre AS CUST_NOMCOM,
    'ECUADOR' AS CUST_PAIS,
    NULL AS CUST_PROV,
    NULL AS CUST_CIU,
    NULL AS CUST_PAR,
    NULL AS CUST_ZON,
    e.emp_direccion AS CUST_DIR,
    e.emp_direccion AS CUST_DIR_MAT,
    e.emp_telefono AS CUST_TELF,
    NULL AS CUST_TEL2,
    NULL AS CUST_TEL3,
    e.emp_correo AS CUST_EMA,
    NULL AS CUST_EMA2,
    NULL AS CUST_EMA3,
    e.emp_genero AS CUST_GEN,
    CASE WHEN e.emp_estado = 'Activo' THEN 'ACTIVO' ELSE 'DESACTIVO' end as CUST_EST,
    e.emp_fecha_nacimiento as CUST_DATE_BIRTH,
    e.emp_estado_civil AS CUST_CIVIL_EST,
    e.emp_fecha_registro AS CUST_FEC_REG,
    e.emp_banco AS FK_BANK,
    CASE WHEN e.emp_tipo ='ahorros' then 'AHORROS' ELSE 'CORRIENTE' END as CUST_ACC_TYPE,
    e.emp_cuenta AS CUST_ACCOUNT,
    e.emp_referenciabancaria AS CUST_BANK_REF
FROM tbEmpleados e
INNER JOIN tbContratos c 
    ON c.id_empleado_detallle = e.empleado_id
WHERE e.fk_empresa_id =  ${idEmpresaRhh}
AND c.id_contrato = (
    SELECT MAX(c2.id_contrato)
    FROM tbContratos c2
    WHERE c2.id_empleado_detallle = e.empleado_id
);`);


    const employes = rows as any[];

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

export async function migrateEmployesContractForCompany(
    legacyConn: any,
    conn: any,
    humanResourcesDb: any,
    newCompanyId: number
): Promise<Record<number, number>> {


    const [dbRhh] = await humanResourcesDb.query(
        `SELECT * FROM tbEmpresa WHERE empresa_codigo = ?`,
        [newCompanyId],
    );

    const empresas = dbRhh as any[];

    const mapEmployes: Record<number, number> = {};
    if (!empresas.length) {
        return mapEmployes;
    }

    const empresa = empresas[0];
    const idEmpresaRhh = empresa.empresa_id;

    console.log("Migrando empleados...");
    const [rows] = await humanResourcesDb.query(`SELECT
    e.empleado_id AS FK_EMPLOYEE,
    c.id_contrato_tipo AS FK_CONTYPE,
    c.contrato_fechasuscripcion AS CTR_FEC,
    CASE WHEN c.contrado_estado = 'Activo' THEN 'ACTIVO' ELSE 'INACTIVO'
END AS CTR_STATUS,
CASE WHEN c.perfilPersona = 'TBPERMANENTES' THEN 'TRABAJADORES PERMANENTES' WHEN c.perfilPersona = 'TBTEMPORALES' THEN 'TRABAJADORES TEMPORALES' WHEN c.perfilPersona = 'TbPHORAS' THEN 'TRABAJADORES POR HORAS' WHEN c.perfilPersona = 'TBPOROBRA' THEN 'TRABAJADORES POR OBRA O SERVICIO' WHEN c.perfilPersona = 'TBDIRECTIVOSMANDOS' THEN 'DIRECTIVOS Y MANDOS' WHEN c.perfilPersona = 'TBGERENTE' THEN 'GERENTES' WHEN c.perfilPersona = 'TBDIRECTORES' THEN 'DIRECTORES' WHEN c.perfilPersona = 'TBLIDERESDEQUIPO' THEN 'LIDERES DE EQUIPO' WHEN c.perfilPersona = 'TPERSONALFORMACION' THEN 'PERSONAL EN FORMACION' WHEN c.perfilPersona = 'TPASANTE' THEN 'PASANTES O PRACTICANTES REMUNERADOS' WHEN c.perfilPersona = 'TAPRENDICES' THEN 'APRENDICES EN PROGRAMAS LABORALES' WHEN c.perfilPersona = 'TSOCIO' THEN 'SOCIO' ELSE 'PRESIDENTE'
END AS CTR_TYPE_REL,
c.contrato_lugartrabajo AS CTR_DIR_TRA,
c.fecha_fin AS CTR_FEC_INI,
c.fecha_inicio AS CTR_FEC_FIN,
c.contrato_diainiciojornada AS CTR_DIA_INI,
c.contrato_diafinjornada AS CTR_DIA_FIN,
c.contrato_horainiciojornada AS CTR_HORA_INI,
c.contrato_horafinjornada AS CTR_HORA_FIN,
c.dias_labor AS CTR_DIAS_LAB,
c.contrato_formapago AS CTR_FORM_PAG,
NULL AS CTR_RUTA_FILE,
c.labor_funciontrabajdor AS CTR_FUN_LAB,
labor_remuneracion AS CTR_REM_EMP,
NULL AS CTR_CAL_DMCTO,
NULL AS CTR_MEN_DMCTO,
NULL AS CTR_CAL_DMTER,
NULL AS CTR_MEN_DMTER,
NULL AS CTR_CAL_VAC,
NULL AS CTR_MEN_VAC,
NULL AS CTR_CAL_DESA,
NULL AS CTR_MEN_DESA,
NULL AS CTR_CALC_RESERVE,
NULL AS CTR_MONTH_RESERVE,
c.id_labor_departamento AS FK_DEPARTMENT,
c.id_labor_cargo AS FK_POSITION,
NULL AS FK_COD_EMP, 
c.contrato_beneficiossociales AS CONT_REG
FROM
    tbEmpleados e
INNER JOIN tbContratos c ON
    c.id_empleado_detallle = e.empleado_id
WHERE
    e.fk_empresa_id = ${idEmpresaRhh} AND c.id_contrato =(
    SELECT
        MAX(c2.id_contrato)
    FROM
        tbContratos c2
    WHERE
        c2.id_empleado_detallle = e.empleado_id
)
ORDER BY
    empleado_id
DESC;`);


    const employes = rows as any[];

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