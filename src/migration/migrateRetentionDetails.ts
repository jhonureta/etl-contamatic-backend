import { RetentionCodeValue } from "./purchaseHelpers";

type BuildResult = {
  mapRetentions: Record<number, number>;
  oldRetentionCodeMap: Map<string, RetentionCodeValue>;
  newRetentionIdMap: Record<number, number>;
};

export async function migrateRetentions(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapAccounts: Record<number, number | null>
): Promise<BuildResult> {

    const mapRetentions: Record<number, number> = {};
    const oldRetentionCodeMap = new Map<string, RetentionCodeValue>();
    const newRetentionIdMap: Record<number, number> = {};
    try {
        // 1. Obtener retenciones existentes
        const costQuery = `
            SELECT
                ce.CODIGOTR_ID AS COSTEXPENSEID,
                ce.NOMBRE_RETEN AS NOMBRE_COSTEXP,
                ced.FK_COD_ACTIVO AS FK_CTA_COSTS,
                ced.FK_COD_PASIVO AS FK_CTA_EXPE,
                ced.ESTADO AS ESTADOCOST,
                ced.FK_COD_EMP
            FROM tax_codes AS ce
            LEFT JOIN detail_tax AS ced 
                ON ce.CODIGOTR_ID = ced.CODIGOTR_IDFK
                AND (ced.FK_COD_EMP IS NULL OR ced.FK_COD_EMP = ?)
            WHERE (ce.FK_COD_EMP IS NULL OR ce.FK_COD_EMP = ?);
        `;
        const [costsData] = await conn.query(costQuery, [newCompanyId, newCompanyId]);
        if (!costsData.length) throw new Error('No existen retenciones base para migrar.');

        const values: Record<string, any> = {};
        for (const row of costsData) {
            values[row.NOMBRE_COSTEXP.toUpperCase()] = row;
        }

        // 2. Obtener retenciones del sistema legado
        const legacyQuery = `
           SELECT 
                id_Retencion AS id,
                concepto_Retencion AS nombre_CG,
                fk_cuenta_activo AS FK_COSTO_CUENTA,
                fk_cuenta_pasivo AS FK_GASTO_CUENTA,
                estado AS estado_CG,
                porcentaje_Retencion AS porcentaje,
                codigo_Retencion AS codigoRetencion,
                renta_iva AS tipoRetencion,
                fecha_inicial AS fechaInicio,
                CASE WHEN  fecha_vigencia ='0000-00-00' THEN '2028-12-31' ELSE fecha_vigencia END AS fechaFin,
                estado_actual_sri,
                ultima_actualizacion
            FROM retenciones;
        `;
        const [businessExpenses] = await legacyConn.query(legacyQuery);
        if (!businessExpenses.length) throw new Error('No se encontraron retenciones en el sistema legado.');

        // 3. Procesar cada retención
        for (const item of businessExpenses as any[]) {
            const nombreUpper = item.nombre_CG.toUpperCase();

            const codIdPlanCosto = mapAccounts[item.FK_COSTO_CUENTA] ?? null;
            const codIdPlanGasto = mapAccounts[item.FK_GASTO_CUENTA] ?? null;

            if (!codIdPlanCosto && !codIdPlanGasto) {
                console.warn(`⚠️ Cuentas no encontradas para ${item.nombre_CG}`);
                continue;
            }
            console.warn(` MIGRANDO RETENCIONES ${item.nombre_CG}`);
            const existing = values[nombreUpper];

            if (existing) {
                // Revisar si existe detalle
                const [detail] = await conn.query(
                    `SELECT * FROM detail_tax WHERE CODIGOTR_IDFK = ? AND FK_COD_EMP = ?`,
                    [existing.COSTEXPENSEID, newCompanyId]
                );

                if (!detail.length) {
                    const [insertDetail] = await conn.query(
                        `INSERT INTO detail_tax(CODIGOTR_IDFK, FK_COD_EMP, FK_COD_ACTIVO, FK_COD_PASIVO, ESTADO)
                         VALUES (?, ?, ?, ?, ?);`,
                        [
                            existing.COSTEXPENSEID,
                            newCompanyId,
                            codIdPlanCosto,
                            codIdPlanGasto,
                            item.estado_CG
                        ]
                    );
                    mapRetentions[item.id] = insertDetail.insertId;
                    newRetentionIdMap[item.id] = existing.COSTEXPENSEID;
                    oldRetentionCodeMap.set(
                        `${item.codigoRetencion}:${item.tipoRetencion}`,
                        {
                            id: existing.COSTEXPENSEID,
                            name: item.nombre_CG
                        }
                    );
                } else {
                    const row = detail[0];
                    await conn.query(
                        `UPDATE detail_tax 
                         SET FK_COD_ACTIVO=?, FK_COD_PASIVO=?, ESTADO=?
                         WHERE ID_DET=? AND FK_COD_EMP=?;`,
                        [
                            codIdPlanCosto,
                            codIdPlanGasto,
                            item.estado_CG,
                            row.ID_DET,
                            newCompanyId
                        ]
                    );
                    mapRetentions[item.id] = row.ID_DET;
                    newRetentionIdMap[item.id] = existing.COSTEXPENSEID;
                    oldRetentionCodeMap.set(
                        `${item.codigoRetencion}:${item.tipoRetencion}`,
                        {
                            id: existing.COSTEXPENSEID,
                            name: item.nombre_CG
                        }
                    )
                }
            } else {
                // Insertar retención completa
                const { detail, tax } = await insertNewRetentionCodeMdl(conn, {
                    companyId: newCompanyId,
                    ...item,
                    planCountActivoId: codIdPlanCosto,
                    planCountPasivoId: codIdPlanGasto,
                    statusTax: item.estado_CG,
                });
                mapRetentions[item.id] = detail.insertId;
                newRetentionIdMap[item.id] = tax.insertId;
                oldRetentionCodeMap.set(
                    `${item.codigoRetencion}:${item.tipoRetencion}`,
                     {
                        id: tax.insertId,
                        name: item.nombre_CG
                    }
                )
                console.log(`✔ Insertada nueva retención ${item.nombre_CG}`);
            }
        }
    } catch (error: any) {
        console.error('❌ Error en migrateExpensesDetails:', error.message);
        throw error;
    }

    return { mapRetentions, oldRetentionCodeMap, newRetentionIdMap};
}

// -----------------------------
// Insertar nueva retención
// -----------------------------
async function insertNewRetentionCodeMdl(conn: any, taxData: any) {
    try { 
        const [tax] = await conn.query(
            `INSERT INTO tax_codes(
                NOMBRE_RETEN,DESC_RETEN, PORC_RETEN, COD_RET, TIPO_RETEN,
                ESTADO_SRI, FECHA_INITSRI, FECHA_FINSRI,
                FK_COD_EMP, FECHA_ACTUALIZACION
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
            [
                taxData.nombre_CG,
                taxData.nombre_CG,
                taxData.porcentaje,
                taxData.codigoRetencion,
                taxData.tipoRetencion,
                taxData.estado_actual_sri,
                taxData.fechaInicio,
                taxData.fechaFin,
                taxData.companyId,
                taxData.ultima_actualizacion
            ]
        );

        const [detail] = await conn.query(
            `INSERT INTO detail_tax(
                CODIGOTR_IDFK, FK_COD_EMP, FK_COD_ACTIVO, FK_COD_PASIVO, ESTADO
            ) VALUES (?, ?, ?, ?, ?);`,
            [
                tax.insertId,
                taxData.companyId,
                taxData.planCountActivoId,
                taxData.planCountPasivoId,
                taxData.statusTax
            ]
        );

        return { detail, tax };
    } catch (error) {
        console.error('❌ Error insertando retención:', error);
        throw error;
    }
}
