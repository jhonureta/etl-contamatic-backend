
export async function migrateDepartments(
    humanResourcesDb: any,
    conn: any,
    newCompanyId: number,
    idEmpresaRhh: number
): Promise<{ mapDepartments: Record<number, number> }> {

    console.log("Migrando departamentos...");

    // Obtiene marcas únicas normalizadas
    const [rows] = await humanResourcesDb.query(`
        SELECT * FROM tbDepartamentos WHERE tbDepartamentos.fk_empresa_id=?;`,
        [idEmpresaRhh]);

    const departments = rows as any[];

    if (!departments.length) {
        throw new Error(" -> No hay departments para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapDepartments: Record<number, number> = {};

    for (let i = 0; i < departments.length; i += BATCH_SIZE) {
        const batch = departments.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {


            return [
                c.departamento_nombre,
                c.departamento_descripcion,
                new Date(),   // Fecha dinámica
                newCompanyId,
            ];
        });

        const [res]: any = await conn.query(
            `INSERT INTO departments
                (DEP_NAME, DEP_DESCR, DEP_FEC_REG, FK_COD_EMP)
             VALUES ?`,
            [values]
        );

        let newId = res.insertId;

        for (const item of batch) {
            mapDepartments[item.departamento_id] = newId;
            newId++;
        }

        console.log(` -> Batch migrado: ${batch.length} departamento`);
    }



    return { mapDepartments };
}

export async function migratePositions(
    humanResourcesDb: any,
    conn: any,
    newCompanyId: number,
    idEmpresaRhh
): Promise<{ mapPositions: Record<number, number> }> {

    console.log("Migrando cargos...");

    // Obtiene marcas únicas normalizadas
    const [rows] = await humanResourcesDb.query(`
        SELECT tbCargo.* FROM tbDepartamentos inner join tbCargo on tbDepartamentos.departamento_id = tbCargo.fk_id_departamento WHERE tbDepartamentos.fk_empresa_id=?;`, [idEmpresaRhh]);

    const positions = rows as any[];

    if (!positions.length) {
        throw new Error(" -> No hay positions para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapPositions: Record<string, number> = {};

    for (let i = 0; i < positions.length; i += BATCH_SIZE) {
        const batch = positions.slice(i, i + BATCH_SIZE);

        const values = batch.map(c => {


            return [
                c.cargo_nombre,
                c.cargo_descripcion,
                newCompanyId,
            ];
        });

        const [res]: any = await conn.query(`INSERT INTO positions ( POS_NAME, POS_DESCR, POS_NUM_HOUR, POS_SALARY, POS_HOUR_RATE, POS_HOUR_RATE_50, POS_HOUR_RATE_100, POS_APPOR_BONUS, POS_HOUR_RATE_LV, POS_HOUR_RATE_SA, POS_OVERTIME, POS_LOAN, POS_THIRTEENTH, POS_FOURTEENTH, POS_EVICTION, POS_TOTAL_ING, POS_IESS, POS_TOTAL, POS_FEC_REG, FK_COD_EMP) VALUES (?, ?, '8', '470', '2.9375', '4.40625', '5.875', '0', '0', '0', '0', '0', '39.166667', '39.166667', '0', '548.333333', '44.415', '503.918333', '2025-12-12 09:22:41', ?)`,
            [values]
        );

        let newId = res.insertId;

        for (const item of batch) {
            mapPositions[item.cargo_id] = newId;
            newId++;
        }

        console.log(` -> Batch migrado: ${batch.length} positions`);
    }



    return { mapPositions };
}


export async function migrateSalaries(
    humanResourcesDb: any,
    conn: any,
    newCompanyId: number,
    idEmpresaRhh
): Promise<{ mapSalaries: Record<number, number> }> {

    console.log("Migrando salarios...");

    // Obtiene EL SALARIO BASICO
    const [rows] = await humanResourcesDb.query(`
        SELECT
    salario_id as SALR_ID,    
    salario_descripcion AS SALR_NAME,
    salario_periodo AS SALR_PERIODO,
    salario_fecha AS SALR_FEC_INI,
    salario_fecha_fin AS SALR_FEC_FIN,
    salario_monto AS SALR_MONTO,
    ROUND(salario_monto/12,2) AS SALR_DEC_TERC,
    ROUND(salario_monto/12,2) AS SALR_DEC_CUAR,
    ROUND(salario_monto/12,2) AS SALR_FON_RES,
    ROUND(salario_monto/24,2) AS SALR_VACA,
    ROUND(salario_monto/24,2) AS SALR_DESAHU,
    CASE WHEN salario_estado='Pendiente' THEN 'PENDIENTE'
    WHEN salario_estado='Proceso' THEN 'PENDIENTE'
    ELSE 'TERMINADO' END
    AS SALR_STATUS,
    (SELECT NOW()) SALR_FEC_REG,
    NULL AS FK_COD_EMP
FROM
    tbSalarios WHERE fk_empresa_id = ?;
        `
        , [idEmpresaRhh]);

    const salarios = rows as any[];

    if (!salarios.length) {
        throw new Error(" -> No hay salarios para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapSalaries: Record<string, number> = {};

    for (let i = 0; i < salarios.length; i += BATCH_SIZE) {
        const batch = salarios.slice(i, i + BATCH_SIZE);
        const values = batch.map(s => {
            return [
                s.SALR_NAME,
                s.SALR_PERIODO,
                s.SALR_FEC_INI,
                s.SALR_FEC_FIN,
                s.SALR_MONTO,
                s.SALR_DEC_TERC,
                s.SALR_DEC_CUAR,
                s.SALR_FON_RES,
                s.SALR_VACA,
                s.SALR_DESAHU,
                s.SALR_STATUS,
                s.SALR_FEC_REG,
                newCompanyId,
            ];
        });

        const [res]: any = await conn.query(`INSERT INTO  salaries( SALR_NAME, SALR_PERIODO, SALR_FEC_INI, SALR_FEC_FIN, SALR_MONTO, SALR_DEC_TERC, SALR_DEC_CUAR, SALR_FON_RES, SALR_VACA, SALR_DESAHU, SALR_STATUS, SALR_FEC_REG, FK_COD_EMP) VALUES (?)`,
            [values]
        );

        let newId = res.insertId;

        for (const item of batch) {
            mapSalaries[item.SALR_ID] = newId;
            newId++;
        }

        console.log(` -> Batch migrado: ${batch.length} salarios`);
    }



    return { mapSalaries };
}
