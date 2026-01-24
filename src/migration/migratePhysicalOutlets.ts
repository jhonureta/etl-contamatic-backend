
import { upsertTotaledEntry } from "./migrationTools";
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

    const mapEntryAccount = await migrateAccountingEntriesPhysicalTakeOff(
        legacyConn,
        conn,
        newCompanyId,
        mapPeriodo,
        mapAuditPhysical,
    )

    const mapEntryDetailAccount = await migrateDetailedAccountingEntriesPhysicalTakeOff(
        legacyConn,
        conn,
        newCompanyId,
        mapProject,
        mapCenterCost,
        mapAccounts,
        mapEntryAccount.mapEntryAccount
    )


    return mapPhysical;
}


export async function migrateAccountingEntriesPhysicalTakeOff(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapPeriodo: Record<number, number | null>,
    mapAuditPhysical: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {


    console.log("🚀 Migrando encabezado de asiento contables toma fisica..........");
    try {//IMPORTE_GD
        const mapEntryAccount: Record<number, number> = {};
        const [rows]: any[] = await legacyConn.query(`SELECT
    cod_asiento,
    fecha_asiento AS FECHA_ASI,
    descripcion_asiento AS DESCRIP_ASI,
    numero_asiento AS NUM_ASI,
   'TOMA' as  ORG_ASI,
debe_asiento AS TDEBE_ASI,
haber_asiento AS THABER_ASI,
numero_asiento,
contabilidad_asientos.tipo_asiento AS TIP_ASI,
fk_cod_periodo AS FK_PERIODO,
fecha_registro_asiento AS FECHA_REG,
fecha_update_asiento AS FECHA_ACT,
json_asi AS JSON_ASI,
res_asiento AS RES_ASI,
ben_asiento AS BEN_ASI,
NULL AS FK_AUDIT,
NULL AS FK_COD_EMP,
contabilidad_asientos.FK_CODTRAC,
NULL AS COD_TRAC,
CAST(
    REGEXP_REPLACE(
        RIGHT(numero_asiento, 9),
        '[^0-9]',
        ''
    ) AS UNSIGNED
) AS SEC_ASI,
cod_origen,
NULL AS FK_MOV,
contabilidad_asientos.cod_origen AS FK_ANTDET
FROM
    tomasfisica INNER JOIN contabilidad_asientos ON contabilidad_asientos.FK_CODTOM = tomasfisica.COD_TOM;` );

        if (!rows.length) {
            return { mapEntryAccount };
        }


        const BATCH_SIZE = 1000;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {
                const periodoId = mapPeriodo[o.FK_PERIODO]
                const idAuditTr = mapAuditPhysical[o.cod_origen];
                const idMovimiento = null;

                insertValues.push([
                    o.FECHA_ASI,
                    o.DESCRIP_ASI,
                    o.NUM_ASI,
                    o.ORG_ASI,
                    o.TDEBE_ASI,
                    o.THABER_ASI,
                    o.TIP_ASI,
                    periodoId,
                    o.FECHA_REG,
                    o.FECHA_ACT,
                    o.JSON_ASI,
                    o.RES_ASI,
                    o.BEN_ASI,
                    idAuditTr,
                    newCompanyId,
                    o.SEC_ASI,
                    null,
                    idMovimiento
                ]);


            }

            const [res]: any = await conn.query(`INSERT INTO accounting_movements(
                    FECHA_ASI,
                    DESCRIP_ASI,
                    NUM_ASI,
                    ORG_ASI,
                    TDEBE_ASI,
                    THABER_ASI,
                    TIP_ASI,
                    FK_PERIODO,
                    FECHA_REG,
                    FECHA_ACT,
                    JSON_ASI,
                    RES_ASI,
                    BEN_ASI,
                    FK_AUDIT,
                    FK_COD_EMP,
                    SEC_ASI,
                    FK_MOVTRAC,
                    FK_MOV) VALUES ?`, [insertValues]);

            let newId = res.insertId;
            for (const o of batch) {
                mapEntryAccount[o.cod_asiento] = newId++;
            }
        }
        console.log("✅ Migración asiento contable toma fisica completada correctamente");
        return { mapEntryAccount };
    } catch (err) {
        console.error("❌ Error en migración de asiento contable:", err);
        throw err;
    }
}
export async function migrateDetailedAccountingEntriesPhysicalTakeOff(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {
    console.log("🚀 Cuentas contables");
    console.log("🚀 Iniciando migración de detalles de asientos contables toma fisica..........");

    const mapAccountDetail: Record<number, number> = {};

    const [rows]: any[] = await legacyConn.query(`SELECT
    d.cod_detalle_asiento,
    contabilidad_asientos.fecha_asiento,
    contabilidad_asientos.cod_asiento AS FK_COD_ASIENTO,
    d.debe_detalle_asiento AS DEBE_DET,
    d.haber_detalle_asiento AS HABER_DET,
    d.fk_cod_plan AS FK_CTAC_PLAN,
    d.fkProyectoCosto AS FK_COD_PROJECT,
    d.fkCentroCosto AS FK_COD_COST
FROM
    tomasfisica
INNER JOIN contabilidad_asientos ON contabilidad_asientos.FK_CODTOM = tomasfisica.COD_TOM
INNER JOIN contabilidad_detalle_asiento d ON
    d.fk_cod_asiento = contabilidad_asientos.cod_asiento;`);

    if (!rows.length) {
        console.log("⚠️ No hay registros para migrar");
        return { mapAccountDetail };
    } //console.log(rows);

    const BATCH_SIZE = 1000;
    console.log(`📦 Total registros a migrar: ${rows.length}`);
    let totalDebe = 0;
    let totalHaber = 0;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        console.log(`➡️ Procesando batch ${i / BATCH_SIZE + 1}`);

        try {
            const insertValues: any[] = [];
            const totalsMap = new Map<string, any>();

            for (const o of batch) {
                const idPlan = mapAccounts[o.FK_CTAC_PLAN];
                const idProyecto = mapProject[o.FK_COD_PROJECT] ?? null;
                const idCentroCosto = mapCenterCost[o.FK_COD_COST] ?? null;
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO] ?? null;

                if (!idPlan || !idCodAsiento) continue;

                const debe = Number(o.DEBE_DET) || 0;
                const haber = Number(o.HABER_DET) || 0;

                insertValues.push([
                    idCodAsiento,
                    debe,
                    haber,
                    idPlan,
                    idProyecto,
                    idCentroCosto
                ]);

                const key = `${newCompanyId}-${idPlan}-${o.fecha_asiento}`;
                if (!totalsMap.has(key)) {
                    totalsMap.set(key, {
                        id_plan: idPlan,
                        fecha: o.fecha_asiento,
                        debe: 0,
                        haber: 0,
                        total: 0,
                        operacion: "suma"
                    });
                }

                const acc = totalsMap.get(key);
                acc.debe += debe;
                acc.haber += haber;
                acc.total++;

                totalHaber += haber;
                totalDebe += debe;
            }

            if (!insertValues.length) {
                console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
                continue;
            }

            const [res]: any = await conn.query(`
                INSERT INTO accounting_movements_det (
                    FK_COD_ASIENTO,
                    DEBE_DET,
                    HABER_DET,
                    FK_CTAC_PLAN,
                    FK_COD_PROJECT,
                    FK_COD_COST
                ) VALUES ?
            `, [insertValues]);

            let newId = res.insertId;

            for (const o of batch) {
                const idPlan = mapAccounts[o.FK_CTAC_PLAN];
                console.log(`➡️ Procesando detalle de asiento toma fisica ${idPlan}`);
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

                if (!idPlan || !idCodAsiento) continue;

                mapAccountDetail[o.cod_detalle_asiento] = newId++;
            }

            for (const t of totalsMap.values()) {
                await upsertTotaledEntry(conn, t, newCompanyId);
            }

            console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado toma fisica correctamente.`);

        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }

    console.log("🎉 Migración  detalles contables completada toma fisica");
    return { mapAccountDetail };
}