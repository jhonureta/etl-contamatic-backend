export async function migrateDepartments(
    humanResourcesDb: any,
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<{ mapDepartments: Record<number, number> }> {

    console.log("Migrando departamentos...");

    // Obtiene marcas únicas normalizadas
    const [rows] = await humanResourcesDb.query(`
        SELECT * FROM tbDepartamentos WHERE tbDepartamentos.fk_empresa_id=33`);

    const departments = rows as any[];

    if (!departments.length) {
        throw new Error(" -> No hay departments para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapDepartments: Record<string, number> = {};

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
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<{ mapPositions: Record<number, number> }> {

    console.log("Migrando cargos...");

    // Obtiene marcas únicas normalizadas
    const [rows] = await humanResourcesDb.query(`
        SELECT tbCargo.* FROM tbDepartamentos inner join tbCargo on tbDepartamentos.departamento_id = tbCargo.fk_id_departamento WHERE tbDepartamentos.fk_empresa_id=33;`);

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
