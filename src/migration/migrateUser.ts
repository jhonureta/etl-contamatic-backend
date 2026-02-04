import { UserIdentity } from "./migrationTools";

const permissionByRole = {
    administrador: {
        views: ['*'],
        apis: ['*'],
    },
    gerente: {
        views: ['*'],
        apis: ['*'],
    },
    vendedor: {
        views: ['LISTAR CLIENTES', 'CREAR CLIENTE', 'EDITAR CLIENTE', 'LISTA DOCUMENTOS', 'CREAR VENTA', 'EDITAR VENTA', 'PUNTO DE VENTA', 'LISTA PROFORMA', 'NUEVA PROFORMA MULTIPLE',
            'NUEVA PROFORMA', 'EDITAR PROFORMA', 'LISTAR ORDENES TRABAJO', 'CREAR ORDENES TRABAJO', 'EDITAR ORDENES TRABAJO', 'LISTAR ORDENES VENTA', 'CREAR ORDENES VENTA', 'EDITAR ORDENES VENTA',
            'CREAR NOTA DE CREDITO VENTA', 'EDITAR NOTA DE CREDITO VENTA', 'VER NOTA DE CREDITO', 'VER VENTA'],
        apis: ['CLIENTES / PROVEEDORES', 'VENTA', 'PROFORMA', 'ORDEN TRABAJO', 'ORDEN VENTA', 'NOTA DE CREDITO']
    },
    cajero: {
        views: ['LISTAR CLIENTES', 'CREAR CLIENTE', 'EDITAR CLIENTE', 'LISTA DOCUMENTOS', 'CREAR VENTA', 'EDITAR VENTA', 'PUNTO DE VENTA', 'LISTA PROFORMA', 'NUEVA PROFORMA MULTIPLE',
            'NUEVA PROFORMA', 'EDITAR PROFORMA', 'LISTAR ORDENES TRABAJO', 'CREAR ORDENES TRABAJO', 'EDITAR ORDENES TRABAJO', 'LISTAR ORDENES VENTA', 'CREAR ORDENES VENTA', 'EDITAR ORDENES VENTA',
            'CREAR NOTA DE CREDITO VENTA', 'EDITAR NOTA DE CREDITO VENTA', 'VER NOTA DE CREDITO', 'VER VENTA'],
        apis: ['CLIENTES / PROVEEDORES', 'VENTA', 'PROFORMA', 'ORDEN TRABAJO', 'ORDEN VENTA', 'NOTA DE CREDITO']
    },
}

const invalidRoles = ['seleccione...', 'especial1', 'especial2', 'guardia', 'doctor', 'encomienda', 'boleteria', 'boleteria-encomienda', 'transportista'];

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
        invalidRoles.includes(u.ROL_USUEMP) ? 'vendedor' : u.ROL_USUEMP,
        u.EST_USUEMP, u.estado_usu, u.count_intentos,
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
        const users = [];

        let firstInsertedId = res.insertId;
        for (let i = 0; i < filteredUsers.length; i++) {
            userMap[filteredUsers[i].COD_USUEMP] = firstInsertedId + i;

            let role = invalidRoles.includes(filteredUsers[i]?.ROL_USUEMP) ? 'vendedor' : filteredUsers[i]?.ROL_USUEMP;
            users.push({
                id: firstInsertedId + i,
                role,
                companyId: newCompanyId
            });
        }

        await assingPermissionRole({
            users,
            conn
        });
    } catch (err) {
        console.error("🔴 ERROR: Falló la inserción por lotes de usuarios:", err);
        throw err;
    }
}


export async function migrateUsersForCompany(
    legacyConn: any, conn: any, newCompanyId: number, dataBaseIds: any
): Promise<{ userMap: Record<number, number>, userNameIdMap: Map<string, UserIdentity> }> {

    console.log("Migrando usuarios...");

    const queryUsers = `
        SELECT COD_USUEMP, IDE_USUEMP, NOM_USUEMP, TEL_USUEMP, EMA_USUEMP, CASE WHEN  ALI_USUEMP = 'SoportE9' THEN 'SoportE9' else ALI_USUEMP END AS ALI_USUEMP , PASS_USUEMP, FOTO_USUEMP, 
               LOWER(ROL_USUEMP) AS ROL_USUEMP, EST_USUEMP, ACCESOS_USU, estado_usu, count_intentos, 
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
        `SELECT COD_USUEMP, ALI_USUEMP FROM users WHERE ALI_USUEMP IN (?)`,
        [aliases]
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
    const userNameIdMap = new Map<string, UserIdentity>();
    for (const u of users) {
        const branchId = dataBaseIds.branchMap[u.idSucursal];
        const userId = userMap[u.COD_USUEMP];

        const key = `${branchId}-${userId}`;
        if (!detailSet.has(key)) {
            detailValues.push([branchId, userId]);
            detailSet.add(key);
        }
        userNameIdMap.set(u.NOM_USUEMP?.toUpperCase(), {
            id: userId,
            ci: u.IDE_USUEMP
        });
    }

    if (detailValues.length > 0) {
        await conn.query(
            `INSERT INTO detail_users (FK_COD_USUC, FK_COD_USU) VALUES ?`,
            [detailValues]
        );
    }

    console.log(` -> Detalle usuarios insertados: ${detailValues.length}`);

    return { userMap, userNameIdMap };
}

async function assingPermissionRole({
    users,
    conn
}) {
    const [listViews] = await conn.query(`SELECT * FROM views;`);
    const [listApis] = await conn.query(`SELECT * FROM apis;`);
    for (const user of users) {
        const permissions = permissionByRole[user.role];
        if (!permissions) continue;
        const apiValues = listApis.filter((v: any) => {
            if (permissions.apis.includes('*')) {
                return true;
            }
            return permissions.apis?.includes(v.API_NAME)
        }).map((access: any) => [user.id, user.companyId, access.API_ID, 1, new Date()]);

        if (apiValues.length > 0) {
            const query = `
                INSERT INTO apis_access_permissions (
                FK_USER_ID,
                COD_EMP,
                FK_API_ID,
                APIACC_HAS_ACCESS,
                APIACC_FEC_CRE
                ) VALUES ?;
            `;
            await conn.query(query, [apiValues]);
        }

        const viewValues = listViews
            .filter((v: any) => {
                if (permissions.views.includes('*')) {
                    return true;
                }
                return permissions.views?.includes(v.VIEW_NAME);
            })
            .map((access: any) => [user.id, user.companyId, access.VIEW_ID, 1, new Date()]);
        if (viewValues.length > 0) {
            const query = `
                INSERT INTO views_access_permissions (
                FK_USER_ID,
                COD_EMP,
                FK_VIEW_ID,
                VIEWACC_HAS_ACCESS,
                VIEWACC_FEC_CRE
                ) VALUES ?;
            `;
            await conn.query(query, [viewValues]);
        }
    }
}