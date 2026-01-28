import { ClientIdentity } from "./migrationTools";
import { findFirstDefaultUser } from "./purchaseHelpers";

//SELECT `COD_TRANS`, `NUM_TRANS` AS TRNF_NUM, `OBS_TRANS` AS TRNF_OBS, `RES_TRANS` AS TRNF_RESP, `FEC_TRANS` AS TRNF_FEC, `FECHA_APRO` AS TRNF_FEC_APRO, `FECHA_ANUL` AS TRNF_FEC_ANU, `EST_TRANS` AS TRNF_EST, `DET_TRANS` AS TRNF_DET, `NUM_COM` AS TRNF_NUM_COM, `ID_COM`, `bodega`, `tipo`, `USER_APRO`, `USER_ANUL`, `estado_transferencia`, NULL AS `FK_COD_EMPRESA`, NULL AS `FK_AUDITRNF` FROM `transferencia` WHERE 1;
export async function migrateProductTransfers(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    storeMap: Record<number, number>,
    userMap: Record<number, number>,
    mapProducts: Record<number, number>,
    userNameIdMap: Map<string, ClientIdentity>
): Promise<Record<number, number>> {
    console.log("Migrando transferencias...");

    const [rows] = await legacyConn.query(`SELECT
                                            COD_TRANS,
                                            NUM_TRANS AS TRNF_NUM,
                                            OBS_TRANS AS TRNF_OBS,
                                            RES_TRANS AS TRNF_RESP,
                                            FEC_TRANS AS TRNF_FEC,
                                            FECHA_APRO AS TRNF_FEC_APRO,
                                            FECHA_ANUL AS TRNF_FEC_ANU,
                                            #EST_TRANS AS TRNF_EST,
                                            DET_TRANS AS TRNF_DET,
                                            NUM_COM AS TRNF_NUM_COM,
                                            bodega,
                                            CASE WHEN NUM_COM='TRANSFERENCIA MASIVA' THEN 'MASIVA'
                                            ELSE 'INDEPENDIENTE' END AS TRNF_TIP,
                                            tipo,
                                            USER_APRO AS TRNF_USE_APRO,
                                            USER_ANUL AS TRNF_USE_ANU,
                                            CASE WHEN estado_transferencia='' THEN 'PENDIENTE'
                                            WHEN estado_transferencia='desaprobado' THEN 'ANULADO'
                                            WHEN estado_transferencia='aprobado' THEN 'APROBADO'
                                            ELSE 'PENDIENTE' END AS TRNF_EST,
                                            NULL AS FK_COD_EMPRESA,
                                            NULL AS FK_AUDITRNF, CASE WHEN ID_COM =0 THEN NULL ELSE ID_COM END AS TRNF_ID_COM
                                            FROM
                                                transferencia
                                            WHERE
                                                1;`);

    const mapTransfers: Record<number, number> = {};
    const mapAuditTransfers: Record<number, number> = {};

    const secuencia_query = `SELECT IFNULL(MAX(CODIGO_AUT+0)+1,1) as codigoAuditoria FROM audit WHERE FK_COD_EMP=?;`;
    const [dataSecuencia] = await conn.query(secuencia_query, [newCompanyId]);
    let codigoAuditoria = dataSecuencia[0]['codigoAuditoria'];

    const transferencias = rows as any[];

    if (!transferencias.length) {
        throw new Error(" -> No hay medidas para migrar.");
    }
    const BATCH_SIZE = 1000;

    function transferToArray(json: any) {
        if (!json) return [];

        let data;

        try {
            // Si viene como string → parseamos
            data = typeof json === 'string'
                ? JSON.parse(json)
                : json;
        } catch (error) {
            console.error('JSON_TRANSFER inválido:', error);
            return [];
        }

        // Validamos que sea un array
        if (!Array.isArray(data)) return [];

        return data
            .filter((item: any) =>
                item?.bodegaDisminuir &&
                item?.productoDisminuir &&
                item?.bodegaAumentar &&
                item?.totalAumentar
            )
            .map((item: any) => ({
                warehouseRemove: Number(storeMap[item.bodegaDisminuir]),
                product: Number(mapProducts[item.productoDisminuir]),
                warehouseAdd: Number(storeMap[item.bodegaAumentar]),
                stock: item.totalAumentar
            }));
    }

    const [defaultUser] = await findFirstDefaultUser({ conn, companyId: newCompanyId });

    let defaultUserId = null;
    if (defaultUser) {
        defaultUserId = defaultUser.COD_USUEMP;
    }

    for (let i = 0; i < transferencias.length; i += BATCH_SIZE) {
        const batch = transferencias.slice(i, i + BATCH_SIZE);

        const auditValues: any[] = [];
        for (let j = 0; j < batch.length; j++) {
            auditValues.push([codigoAuditoria, 'TRF-BODEGA', newCompanyId]);
            codigoAuditoria++;
        }

        const [auditRes]: any = await conn.query(
            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
            [auditValues]
        );

        // El insertId corresponde al primer id insertado; mapearlos por orden
        let firstAuditInsertId = auditRes.insertId;
        for (let j = 0; j < batch.length; j++) {
            const codTrn = batch[j].TRNF_NUM;
            const auditId = firstAuditInsertId + j;
            mapAuditTransfers[codTrn] = auditId;
        }

        const values = batch.map(c => {
            const auditId = mapAuditTransfers[c.TRNF_NUM];

            const detalle = transferToArray(c.TRNF_DET);
            const creador = userNameIdMap.get(c.TRNF_RESP?.toUpperCase())?.id || defaultUserId;
            return [
                c.TRNF_NUM,
                c.TRNF_OBS,
                creador,
                c.TRNF_RESP,
                c.TRNF_FEC,
                c.TRNF_FEC_APRO,
                c.TRNF_FEC_ANU,
                c.TRNF_EST,
                JSON.stringify(detalle),
                c.TRNF_NUM_COM,
                c.TRNF_ID_COM,
                c.TRNF_TIP,
                c.TRNF_USE_APRO,
                c.TRNF_USE_ANU,
                newCompanyId,
                auditId
            ];
        });
        try {
            const [res]: any = await conn.query(`
                INSERT INTO transfers( TRNF_NUM, TRNF_OBS, FK_USER, TRNF_RESP, TRNF_FEC, TRNF_FEC_APRO, TRNF_FEC_ANU, TRNF_EST, TRNF_DET, TRNF_NUM_COM, TRNF_ID_COM, TRNF_TIP, TRNF_USE_APRO, TRNF_USE_ANU, FK_COD_EMPRESA, FK_AUDITRNF) VALUES ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapTransfers[s.TRNF_NUM] = newId;
                newId++;
            }

            console.log(` -> Batch migrado: ${batch.length} transferencias.`);
        } catch (err) {
            throw err;
        }
    }

    return mapTransfers;
}

