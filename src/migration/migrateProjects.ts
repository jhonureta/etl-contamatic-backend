export async function migrateProjects(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<string, number>> {

    console.log("Migrando proyectos...");

    // Obtiene projects únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT id_proyecto as ID_PROJECT, codigo_proyecto as CODE_PROJECT, nombre_proyecto as NAME_PROYECT, descr_proyecto as DESCR_PROJECT, estado_proyecto as STATUS_PROJECT, fecha_registro_proyecto as DATE_REG FROM proyectos ;`);

    const projects = rows as any[];

    if (!projects.length) {
        throw new Error(" -> No hay proyectos para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapProject: Record<string, number> = {};

    for (let i = 0; i < projects.length; i += BATCH_SIZE) {
        const batch = projects.slice(i, i + BATCH_SIZE);

        const values = batch.map(p => {
            return [
                p.CODE_PROJECT, 
                p.NAME_PROYECT, 
                p.DESCR_PROJECT, 
                p.STATUS_PROJECT, 
                p.DATE_REG,
                newCompanyId,
            ];
        });

        const [res]: any = await conn.query(
            `INSERT INTO projects
                (CODE_PROJECT, NAME_PROYECT, DESCR_PROJECT, STATUS_PROJECT, DATE_REG, FK_COD_EMP)
             VALUES ?`,
            [values]
        );

        let newId = res.insertId;

        for (const b of batch) {
            mapProject[b.ID_PROJECT] = newId;
            newId++;
        }

        console.log(` -> Batch migrado: ${batch.length} proyectos`);
    }
    return mapProject;
}
