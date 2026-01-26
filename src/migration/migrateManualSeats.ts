import { upsertTotaledEntry } from "./migrationTools";

export async function migrateManualSeats(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapPeriodo: Record<number, number | null>,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
): Promise<{
    mapEntryAccount: Record<number, number>
}> {



        console.log("🚀 Iniciando migracion de asiento contables (MANUALES)..........");
        try {

            const secuencia_query = `SELECT IFNULL(MAX(CODIGO_AUT+0)+1,1) as codigoAuditoria FROM audit WHERE FK_COD_EMP=?;`;
            const [dataSecuencia] = await conn.query(secuencia_query, [newCompanyId]);
            let codigoAuditoria = dataSecuencia[0]['codigoAuditoria'];


            //IMPORTE_GD
            const mapEntryAccount: Record<number, number> = { };
const [rows]: any[] = await legacyConn.query(`SELECT 

 cod_asiento,
    fecha_asiento AS FECHA_ASI,
    descripcion_asiento AS DESCRIP_ASI,
    numero_asiento AS NUM_ASI,
   'CONTABILIDAD' AS ORG_ASI,
abs(debe_asiento) AS TDEBE_ASI,
abs(haber_asiento) AS THABER_ASI,
numero_asiento,
'CONTABILIDAD' as TIP_ASI,
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
FROM contabilidad_asientos WHERE tipo_asiento ='CONTABILIDAD'  ORDER BY cod_asiento DESC;` );

if (!rows.length) {
    return { mapEntryAccount };
}

const mapAuditSeats: Record<number, number> = {};
const BATCH_SIZE = 1000;

for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const insertValues: any[] = [];


    const auditValues: any[] = [];
    for (let j = 0; j < batch.length; j++) {
        auditValues.push([codigoAuditoria, 'CONTABILIDAD', newCompanyId]);
        codigoAuditoria++;
    }

    const [auditRes]: any = await conn.query(
        `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
        [auditValues]
    );

    // El insertId corresponde al primer id insertado; mapearlos por orden
    let firstAuditInsertId = auditRes.insertId;
    for (let j = 0; j < batch.length; j++) {
        const cod_asiento = batch[j].cod_asiento;
        const auditId = firstAuditInsertId + j;
        mapAuditSeats[cod_asiento] = auditId;
    }

    for (const o of batch) {
        const periodoId = mapPeriodo[o.FK_PERIODO]
        const idAuditTr = mapAuditSeats[o.cod_asiento];
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
const mapDetailAsiento = await migrateDetailedAccountingEntriesCustomerObligations(
    legacyConn,
    conn,
    newCompanyId,
    mapProject,
    mapCenterCost,
    mapAccounts,
    mapEntryAccount,
)

console.log("✅ Migración asientos manuales completada correctamente");
return { mapEntryAccount };
    } catch (err) {
    console.error("❌ Error en migración de asiento contable:", err);
    throw err;
}
}

export async function migrateDetailedAccountingEntriesCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapProject: Record<number, number | null>,
    mapCenterCost: Record<number, number | null>,
    mapAccounts: Record<number, number | null>,
    mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {
    console.log("🚀 Cuentas contables");
    console.log("🚀 Iniciando migración de detalles de asientos contables manuales..........");

    const mapAccountDetail: Record<number, number> = {};

    const [rows]: any[] = await legacyConn.query(`SELECT
            d.cod_detalle_asiento,
            contabilidad_asientos.fecha_asiento,
            contabilidad_asientos.cod_asiento AS FK_COD_ASIENTO,
            abs(d.debe_detalle_asiento) AS DEBE_DET,
            abs(d.haber_detalle_asiento) AS HABER_DET,
            d.fk_cod_plan AS FK_CTAC_PLAN,
            d.fkProyectoCosto AS FK_COD_PROJECT,
            d.fkCentroCosto AS FK_COD_COST
FROM
contabilidad_asientos
INNER JOIN contabilidad_detalle_asiento d ON d.fk_cod_asiento = contabilidad_asientos.cod_asiento
WHERE tipo_asiento ='CONTABILIDAD';`);

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
                console.log(`➡️ Procesando detalle de asientos manuales ${idPlan}`);
                const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

                if (!idPlan || !idCodAsiento) continue;

                mapAccountDetail[o.cod_detalle_asiento] = newId++;
            }

            for (const t of totalsMap.values()) {
                await upsertTotaledEntry(conn, t, newCompanyId);
            }

            console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado asientos manuales`);

        } catch (err) {
            console.error("❌ Error en batch:", err);
            throw err;
        }
    }

    console.log("🎉 Migración  detalles contables completada");
    return { mapAccountDetail };
}