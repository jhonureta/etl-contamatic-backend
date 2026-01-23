//SELECT `COD_TRANS`, `NUM_TRANS` AS TRNF_NUM, `OBS_TRANS` AS TRNF_OBS, `RES_TRANS` AS TRNF_RESP, `FEC_TRANS` AS TRNF_FEC, `FECHA_APRO` AS TRNF_FEC_APRO, `FECHA_ANUL` AS TRNF_FEC_ANU, `EST_TRANS` AS TRNF_EST, `DET_TRANS` AS TRNF_DET, `NUM_COM` AS TRNF_NUM_COM, `ID_COM`, `bodega`, `tipo`, `USER_APRO`, `USER_ANUL`, `estado_transferencia`, NULL AS `FK_COD_EMPRESA`, NULL AS `FK_AUDITRNF` FROM `transferencia` WHERE 1;
export async function migratingPhysicalTakeOff(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    storeMap: Record<number, number>,
    userMap: Record<number, number>,
    mapProducts: Record<number, number>,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
): Promise<Record<number, number>> {
    console.log("Migrando toma fisica...");

    const [rows] = await legacyConn.query(`SELECT *FROM (
SELECT
    COD_TOM,
    OBS_TOM AS PHYS_OBS,
    COD_PRO,
    PRE_COS,
    ENT_TOM,
    SAL_TOM,
    fecha AS PHYS_FEC_REG,
    REPLACE(SEC_TOM, ' (REVERSO POR EDICION)', '') as PHYS_NUM,
    sucursal_toma AS FK_WH_ID,
    RES_TOM AS PHYS_RESP,
    FEC_TOM AS PHYS_FEC,
    ABA_TOM,
    STOACT_TOM,
    JSON_HISTORIAL AS PHYS_HIST,
  
    NULL AS PHYS_DET,
    NULL AS FK_COD_EMPRESA,
    NULL AS PHYS_ACCOUNT_ZERO,
    NULL AS PHYS_ACCOUNT_DIFFZERO,
    NULL AS PHYS_AUDIT,
    -- CREAR
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.crear.usuario'
        )
    ) AS crear_usuario,
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.crear.fechaHora'
        )
    ) AS crear_fecha,
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.crear.codigoUser'
        )
    ) AS crear_codigoUser,
    -- EDITADO (primer elemento del array)
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.editados[0].usuario'
        )
    ) AS editado_usuario,
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.editados[0].fechaHora'
        )
    ) AS editado_fecha,
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.editados[0].codigoUser'
        )
    ) AS editado_codigoUser,
    -- ANULAR
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.anular.usuario'
        )
    ) AS TRNF_USE_ANU,
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.anular.fechaHora'
        )
    ) AS PHYS_FEC_ANU,
    JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.anular.codigoUser'
        )
    ) AS anular_codigoUser,
   CASE WHEN  JSON_UNQUOTE(
        JSON_EXTRACT(
            JSON_HISTORIAL,
            '$.anular.codigoUser'
        )
    )IS NULL THEN 'ACTIVO' ELSE 'ANULADO' END AS PHYS_EST
FROM
    tomasfisica GROUP BY SEC_TOM
) AS toma GROUP BY toma.PHYS_NUM;`);

    const mapPhysical: Record<number, number> = {};
    const mapAuditPhysical: Record<number, number> = {};
    const mapAuditDetPhysical: Record<string, any[]> = {};

    const secuencia_query = `SELECT IFNULL(MAX(CODIGO_AUT+0)+1,1) as codigoAuditoria FROM audit WHERE FK_COD_EMP=?;`;
    const [dataSecuencia] = await conn.query(secuencia_query, [newCompanyId]);
    let codigoAuditoria = dataSecuencia[0]['codigoAuditoria'];

    const tomaFisica = rows as any[];

    if (!tomaFisica.length) {
        throw new Error(" -> No hay medidas para migrar.");
    }
    const BATCH_SIZE = 1000;

    function historialToArray(historial: any) {
        if (!historial) return [];

        let data;

        try {
            // Si viene como string → parseamos
            data = typeof historial === 'string'
                ? JSON.parse(historial)
                : historial;
        } catch (error) {
            console.error('JSON_HISTORIAL inválido:', error);
            return [];
        }

        // Validamos que sea objeto
        if (typeof data !== 'object' || Array.isArray(data)) return [];

        return Object.values(data)
            .filter((item: any) => item?.usuario && item?.fechaHora)
            .map((item: any) => ({
                responsible: item.usuario,
                date: item.fechaHora
            }));
    }


    for (let i = 0; i < tomaFisica.length; i += BATCH_SIZE) {
        const batch = tomaFisica.slice(i, i + BATCH_SIZE);

        const auditValues: any[] = [];
        for (let j = 0; j < batch.length; j++) {
            auditValues.push([codigoAuditoria, 'TOMA-FISICA', newCompanyId]);
            codigoAuditoria++;

            const phys_det: any[] = [];

            const searchTake = `SELECT * FROM tomasfisica inner join producto on producto.COD_PRO= tomasfisica.COD_PRO WHERE SEC_TOM  = ?;`;
            const [dataSec] = await legacyConn.query(searchTake, [batch[j].PHYS_NUM]);
            for (let x = 0; x < dataSec.length; x++) {
                phys_det.push({
                    "productId": mapProducts[dataSec[x].COD_PRO],
                    "type": Number(dataSec[x].ENT_TOM) > 0 ? 'INGRESO' : 'SALIDA',
                    "stock": Number(dataSec[x].ENT_TOM) > 0 ? Number(dataSec[x].ENT_TOM) : Number(dataSec[x].SAL_TOM),
                    "cost": dataSec[x].PRE_COS,
                    "name": dataSec[x].NOM_PRO
                })
            }

            mapAuditDetPhysical[batch[j].PHYS_NUM] = phys_det;
        }

        const [auditRes]: any = await conn.query(
            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
            [auditValues]
        );

        // El insertId corresponde al primer id insertado; mapearlos por orden
        let firstAuditInsertId = auditRes.insertId;
        for (let j = 0; j < batch.length; j++) {
            const codToma = batch[j].PHYS_NUM;
            const auditId = firstAuditInsertId + j;
            mapAuditPhysical[codToma] = auditId;
        }

        const values = batch.map(c => {
            const auditId = mapAuditPhysical[c.PHYS_NUM];
            const creador = userMap[c.crear_codigoUser];
            const historial = historialToArray(c.PHYS_HIST);

            console.log(historial);

            const detalle = mapAuditDetPhysical[c.PHYS_NUM];
            return [
                creador,
                c.PHYS_NUM,
                c.PHYS_OBS,
                c.PHYS_RESP,
                c.PHYS_FEC,
                c.PHYS_FEC_ANU,
                c.PHYS_EST,
                JSON.stringify(detalle),
                c.TRNF_USE_ANU,
                JSON.stringify(historial),
                newCompanyId,
                storeMap[c.FK_WH_ID],
                c.PHYS_ACCOUNT_ZERO,
                c.PHYS_ACCOUNT_DIFFZERO,
                auditId,
                c.PHYS_FEC_REG
            ];
        }); /* console.log(values); */
        try {
            const [res]: any = await conn.query(`
                INSERT INTO physical_taking(FK_USER, PHYS_NUM, PHYS_OBS, PHYS_RESP, PHYS_FEC, PHYS_FEC_ANU, PHYS_EST, PHYS_DET, TRNF_USE_ANU, PHYS_HIST, FK_COD_EMPRESA, FK_WH_ID, PHYS_ACCOUNT_ZERO, PHYS_ACCOUNT_DIFFZERO, PHYS_AUDIT, PHYS_FEC_REG) VALUES ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapPhysical[s.PHYS_NUM] = newId;
                newId++;
            }

            console.log(` -> Batch migrado: ${batch.length} tomas fisicas.`);
        } catch (err) {
            throw err;
        }
    }

    return mapPhysical;
}

