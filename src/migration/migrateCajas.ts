import { findFirstDefaultUser } from "./purchaseHelpers";

export async function migrateCajas(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapAccounts: Record<number, number | null>,
    userMap: Record<number, number | null>
): Promise<Record<number, number>> {

    const boxMap: Record<number, number> = {};
    console.log("Migrando cajas...");

    try {
        // 1️⃣ Traer todas las cajas del sistema legado
        const queryCajas = `
            SELECT ID_CAJA, FECREG_CAJA, NOM_CAJA, DESC_CAJA, SALDO_CAJA, EST_CAJA, CIERRE_CAJA, FK_CTA_PLAN, FK_COD_USU
            FROM cajas;
        `;
        const [cajasMigradas] = await legacyConn.query(queryCajas);
        if (!cajasMigradas.length) return boxMap;

        console.log(` -> Batch migrado: ${cajasMigradas.length} cajas`);

        const [defaultUser] = await findFirstDefaultUser({ conn, companyId: newCompanyId });

        let defaultUserId = null;
        if (defaultUser) {
            defaultUserId = defaultUser.COD_USUEMP;
        }

        // 3️⃣ Iterar cajas migradas
        for (const caja of cajasMigradas as any[]) {
            const planCountId = mapAccounts[caja.FK_CTA_PLAN] ?? null;
            console.log(`Migrando cajas...nombre : ${caja.NOM_CAJA}`);
            // Crear registro de caja
            const data = await createBoxReg(conn, {
                nameBox: caja.NOM_CAJA,
                descBox: caja.DESC_CAJA || caja.NOM_CAJA,
                saldBox: caja.SALDO_CAJA,
                estatusBox: caja.EST_CAJA,
                planCountId,
                companyId: newCompanyId
            });

            if (data.affectedRows != 1) {
                throw new Error('Error al insertar los registros de la caja');
            }

            // Insertar detalle solo si hay usuarios
            const firstUserId = userMap[caja.FK_COD_USU] ?? defaultUserId;

            console.log(userMap, caja.FK_COD_USU, defaultUserId);


            const detail = await createBoxDetailReg(conn, {
                userId: firstUserId,
                boxId: data.insertId,
                statusDetail: caja.EST_CAJA == 'ACTIVO' ? 1 : 0
            });
            if (detail.affectedRows != 1) {
                throw new Error('Error al insertar el detalle de la caja');
            }
            boxMap[caja.ID_CAJA] = detail.insertId;
            /* } */
        }

        return boxMap;

    } catch (error) {
        throw new Error(error);
    }
}

async function createBoxReg(conn: any, boxdata: any) {
    try {
        const query = `
            INSERT INTO boxes(
                NOM_CAJA, DESC_CAJA, SALDO_CAJA, EST_CAJA, FK_CTAC_PLAN, FK_COD_EMP
            ) VALUES (?,?,?,?,?,?);
        `;
        const [user] = await conn.query(query, [
            boxdata.nameBox,
            boxdata.descBox,
            boxdata.saldBox,
            boxdata.estatusBox,
            boxdata.planCountId ?? null,
            boxdata.companyId
        ]);
        return user;
    } catch (error) {
        throw new Error(error);
    }
}

async function createBoxDetailReg(conn: any, boxdata: any) {
    try {

        const query = `INSERT INTO box_detail(FK_CODUSER, FK_BOX, ESTADO) VALUES (?,?,?);`;
        const [detail] = await conn.query(query, [
            boxdata.userId,
            boxdata.boxId,
            boxdata.statusDetail
        ]);
        return detail;
    } catch (error) {
        throw new Error(error);
    }
}
