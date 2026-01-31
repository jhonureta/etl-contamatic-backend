import { findFirstDefaultUser } from "./purchaseHelpers";

export async function migrateCashClose(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapAccounts: Record<number, number | null>,
    userMap: Record<number, number | null>,
    boxMapId: Record<number, number | null>,
): Promise<{ mapCloseCash: Record<number, number> }> {

    const mapCloseCash: Record<number, number> = {};
    console.log("Migrando cierre de cajas...");

    try {
        // 1️⃣ Traer todas las cajas del sistema legado
        const queryCierreCajas = `
            SELECT
            COD_ARQ,
            DET_CAJAUSU AS DETALLEFISICO,
            TOTAL_CAJA,
            VENTADIA,
            DEPOSITOS,
            CHEQUES,
            TRANSFERENCIA,
            EFECTIVO,
            EFECTIVO_EGRESO,
            TOTAL,
            FALTANTE,
            SOBRANTE,
            VALORCIERRE,
            ESTADO,
            FECHACIERRE,
            FECHAAPERTURA,
            DESCRIPCION,
            SEC AS SECUENCIA,
            FK_COD_USU AS FK_COD_USU,
            FK_COD_USU AS FK_COD_USU_RESP,
            CODIGO_CAJA AS FK_CAJA,
            TOTAL_APERTURA AS APERTURA,
            DET_APERTURA AS DETALLEAPERTURA,
            NULL AS FK_AUDITOB,
            NULL AS FK_CAJA,
            NULL AS COMPRASDIA,
            NULL AS FECH_REP,
            NULL AS ANULADO,
            NULL AS ANULADODIFERENTE,
            FECHAAPERTURA AS FETCH_ACT,
            NULL AS FKA_COD_EMP
            
        FROM
            arqueo_caja
        WHERE
            1;
        `;
        const [rows] = await legacyConn.query(queryCierreCajas);

        const cierreCaja = rows as any[];



        if (!cierreCaja.length) {
            return { mapCloseCash };
        }
        const BATCH_SIZE = 1000;

        const adaptarItems = (estructuraAntigua: any): any[] => {
            try {
                // 1️⃣ Parsear si viene como string JSON
                let datos = estructuraAntigua;
                if (typeof estructuraAntigua === 'string') {
                    try {
                        datos = JSON.parse(estructuraAntigua);
                    } catch (parseError) {
                        console.error('Error al parsear JSON de items:', parseError);
                        return [];
                    }
                }

                // 2️⃣ Validar que sea un array
                if (!Array.isArray(datos)) {
                    console.warn('La estructura de items no es un arreglo válido');
                    return [];
                }

                // 3️⃣ Mapear y normalizar los items
                const itemsAdaptados = datos
                    .filter((item: any) => item && typeof item === 'object')
                    .map((item: any) => ({
                        name: item.valor ?? null,
                        quantity: Number(item.cantidad) || 0,
                        total: Number(item.total) || 0
                    }));

                return itemsAdaptados;

            } catch (error) {
                console.error('Error en adaptarItems:', error);
                return [];
            }
        };



        for (let i = 0; i < cierreCaja.length; i += BATCH_SIZE) {
            const batch = cierreCaja.slice(i, i + BATCH_SIZE);

            const values = batch.map(c => {
                const idUser = userMap[c.FK_COD_USU] || null;
                const idBox = boxMapId[c.FK_CAJA] || null;

                const detalleFisico = adaptarItems(c.DETALLEFISICO);
                const detalleApertura = adaptarItems(c.DETALLEAPERTURA);

                return [
                    c.COD_ARQ,
                    JSON.stringify(detalleFisico), ,
                    c.TOTAL_CAJA,
                    c.VENTADIA,
                    c.DEPOSITOS,
                    c.CHEQUES,
                    c.TRANSFERENCIA,
                    c.EFECTIVO,
                    c.EFECTIVO_EGRESO,
                    c.TOTAL,
                    c.FALTANTE,
                    c.SOBRANTE,
                    c.VALORCIERRE,
                    c.ESTADO,
                    c.FECHACIERRE,
                    c.FECHAAPERTURA,
                    c.DESCRIPCION,
                    c.SECUENCIA,
                    idUser,
                    idUser,
                    idBox,
                    c.APERTURA,
                    JSON.stringify(detalleApertura),
                    c.FK_AUDITOB,
                    c.FK_CAJA,
                    c.COMPRASDIA,
                    c.FECH_REP,
                    c.ANULADO,
                    c.ANULADODIFERENTE,
                    c.FETCH_ACT,
                    newCompanyId
                ];
            });
            const [res]: any = await conn.query(
                `INSERT INTO cash_count(
            DETALLEFISICO,
            TOTAL_CAJA,
            VENTADIA,
            DEPOSITOS,
            CHEQUES,
            TRANSFERENCIA,
            EFECTIVO,
            EFECTIVO_EGRESO,
            TOTAL,
            FALTANTE,
            SOBRANTE,
            VALORCIERRE,
            ESTADO,
            FECHACIERRE,
            FECHAAPERTURA,
            DESCRIPCION,
            SECUENCIA,
            FK_COD_USU,
            FK_COD_USU_RESP,
            FK_CAJA,
            APERTURA,
            DETALLEAPERTURA,
            FK_AUDITOB,
            COMPRASDIA,
            FECH_REP,
            ANULADO,
            ANULADODIFERENTE,
            FETCH_ACT,
            FKA_COD_EMP
            ) VALUES  ?`,
                [values]
            );
            let newId = res.insertId;
            for (const b of batch) {
                mapCloseCash[b.COD_ARQ] = newId;
                newId++;
            }
            console.log(` -> Batch migrado: ${batch.length} anticipos de proveedores`);
        }

        return { mapCloseCash };

    } catch (error) {
        throw new Error(error);
    }
}