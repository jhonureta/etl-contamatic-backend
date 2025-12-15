
async function batchInsertUsers(
    filteredUsers: any[],
    newCompanyId: number,
    conn: any,
    userMap: Record<number, number>,
    dataBaseIds: any
) {
    const batchValues = filteredUsers.map(u => [
        newCompanyId,
        u.IDE_USUEMP, u.NOM_USUEMP, u.TEL_USUEMP, u.EMA_USUEMP, u.ALI_USUEMP, u.PASS_USUEMP, u.FOTO_USUEMP,
        u.ROL_USUEMP, u.EST_USUEMP, u.estado_usu, u.count_intentos,
        u.TWOFACT_USUEMP, u.KEY_TWOFACT_USUEMP, u.CODE_REC_USUEMP, u.EXP_CODE_USUEMP,
        u.FEC_REG, u.FEC_MOD, u.deleteAt, u.CONF_VENTAS
    ]);

    const query = `
        INSERT INTO users (
            FK_COD_EMP, IDE_USUEMP, NOM_USUEMP, TEL_USUEMP, EMA_USUEMP, ALI_USUEMP, PASS_USUEMP, FOTO_USUEMP,
            ROL_USUEMP, EST_USUEMP, estado_usu, count_intentos, TWOFACT_USUEMP, KEY_TWOFACT_USUEMP, CODE_REC_USUEMP, EXP_CODE_USUEMP,
            FEC_REG, FEC_MOD, deleteAt, CONF_VENTAS
        ) VALUES ? 
    `;

    try {
        console.log(` -> Iniciando inserción por lotes de ${filteredUsers.length} usuarios...`);
        const [res]: any = await conn.query(query, [batchValues]);
        console.log(` -> ${filteredUsers.length} usuarios migrados en lote. InsertId inicial: ${res.insertId}. Tiempo: ${new Date().toISOString()}`);
        let firstInsertedId = res.insertId;
        for (let i = 0; i < filteredUsers.length; i++) {
            userMap[filteredUsers[i].COD_USUEMP] = firstInsertedId + i;
        }
    } catch (err) {
        console.error("🔴 ERROR: Falló la inserción por lotes de usuarios:", err);
        throw err;
    }
}


export async function migrateUsersForCompany(
    legacyConn: any, conn: any, newCompanyId: number, dataBaseIds: any
): Promise<Record<number, number>> {

    console.log("Migrando usuarios...");

    const queryUsers = `
        SELECT COD_USUEMP, IDE_USUEMP, NOM_USUEMP, TEL_USUEMP, EMA_USUEMP, ALI_USUEMP, PASS_USUEMP, FOTO_USUEMP, 
               ROL_USUEMP, EST_USUEMP, ACCESOS_USU, estado_usu, count_intentos, 
               0 AS TWOFACT_USUEMP, NULL AS KEY_TWOFACT_USUEMP, NULL AS CODE_REC_USUEMP, NULL AS EXP_CODE_USUEMP, 
               NOW() AS FEC_REG, NOW() AS FEC_MOD, NULL AS deleteAt, '[]' AS CONF_VENTAS, FK_COD_SUC as idSucursal 
        FROM usuarios;
    `;

    const [rows] = await legacyConn.query(queryUsers);
    const users = rows as any[];

    if (!users.length) {
        throw new Error(" -> No hay usuarios para migrar.");
    }

    console.log(` -> Usuarios totales a procesar: ${users.length}.`);

    const userMap: Record<number, number> = {};
    const filteredUsers: any[] = [];

    // 1️⃣ Obtener alias para verificar duplicados
    const aliases = users.map(u => u.ALI_USUEMP);

    const [existing] = await conn.query(
        `SELECT COD_USUEMP, ALI_USUEMP FROM users WHERE ALI_USUEMP IN (?) AND FK_COD_EMP = ?`,
        [aliases, newCompanyId]
    );

    const existingMap = new Map<string, number>();
    (existing as any[]).forEach(u => existingMap.set(u.ALI_USUEMP, u.COD_USUEMP));

    // 2️⃣ Separar existentes y nuevos
    for (const u of users) {
        const existingId = existingMap.get(u.ALI_USUEMP);
        if (existingId) userMap[u.COD_USUEMP] = existingId;
        else filteredUsers.push(u);
    }

    console.log(` -> Usuarios ya existentes: ${Object.keys(userMap).length}`);
    console.log(` -> Usuarios nuevos por insertar: ${filteredUsers.length}`);

    // 3️⃣ Insertar nuevos
    if (filteredUsers.length > 0)
        await batchInsertUsers(filteredUsers, newCompanyId, conn, userMap, dataBaseIds);

    const allBranchIds = [...new Set(users.map(u => u.idSucursal).map(id => dataBaseIds.branchMap[id]))];

    const [existingDetails] = await conn.query(
        `SELECT FK_COD_USUC, FK_COD_USU FROM detail_users WHERE FK_COD_USUC IN (?)`,
        [allBranchIds]
    );

    const detailSet = new Set(existingDetails.map((d: any) => `${d.FK_COD_USUC}-${d.FK_COD_USU}`));

   
    const detailValues = [];

    for (const u of users) {
        const branchId = dataBaseIds.branchMap[u.idSucursal];
        const userId = userMap[u.CCOD_USUEMP];

        const key = `${branchId}-${userId}`;
        if (!detailSet.has(key)) {
            detailValues.push([branchId, userId]);
            detailSet.add(key);
        }
    }

    if (detailValues.length > 0) {
        await conn.query(
            `INSERT INTO detail_users (FK_COD_USUC, FK_COD_USU) VALUES ?`,
            [detailValues]
        );
    }

    console.log(` -> Detalle usuarios insertados: ${detailValues.length}`);

    return userMap;
}
