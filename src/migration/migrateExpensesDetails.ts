export async function migrateExpensesDetails(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapAccounts: Record<number, number | null>
): Promise<Record<number, number>> {

    const costeExpenseMap: Record<number, number> = {};

    try {
        console.log("Migrando costos y gastos...");
        // 1. Obtener datos de costos ya migrados
        const [rows] = await conn.query(`
            SELECT
                ce.COSTEXPENSEID,
                ce.NOMBRE_COSTEXP,
                ced.FK_CTA_COSTS,
                ced.FK_CTA_EXPE,
                ced.ESTADOCOST,
                ced.FK_COD_EMP
            FROM costs_expenses AS ce
            LEFT JOIN costs_expenses_details AS ced 
                ON ce.COSTEXPENSEID = ced.FK_COSTEXPENSEID
                AND (ced.FK_COD_EMP IS NULL OR ced.FK_COD_EMP = ?)
            WHERE (ce.FK_COD_EMP IS NULL OR ce.FK_COD_EMP = ?);
        `, [newCompanyId, newCompanyId]);

        const dataExpenses = rows as any[];
        if (dataExpenses.length === 0) {
            throw new Error(" -> No hay costos y gastos base en la nueva empresa.");

        }

        // Mapa { NOMBRE_COSTEXP: fila }
        const expensesMap = Object.fromEntries(
            dataExpenses.map(e => [e.NOMBRE_COSTEXP.toUpperCase(), e])
        );

        // 2. Obtener datos del sistema legado
        const [businessExpenses] = await legacyConn.query(`
            SELECT 
                ID_CG1 as id,
                nombre_CG,
                FK_COSTO_CUENTA,
                FK_GASTO_CUENTA,
                estado_CG
            FROM costos_gastos;
        `);

        if (!businessExpenses.length) {
            throw new Error("No existen cuentas de costos/gastos en la empresa antigua.");

        }
        console.log(`Costos y gastos a migrar... ${businessExpenses.length}`);
        // 3. Procesar migración
        for (const exp of businessExpenses) {

            const {
                id,
                nombre_CG,
                FK_COSTO_CUENTA,
                FK_GASTO_CUENTA,
                estado_CG
            } = exp;
            console.log(`Gastos ${nombre_CG}`);
            // No tiene cuentas contables → ignorar
            if (!FK_COSTO_CUENTA && !FK_GASTO_CUENTA) {
                console.warn(`⚠️ Ambas cuentas son NULL para ${nombre_CG}`);
                continue;
            }

            const codCosto = mapAccounts[FK_COSTO_CUENTA] ?? null;
            const codGasto = mapAccounts[FK_GASTO_CUENTA] ?? null;

            if (codCosto === null && codGasto === null) {
                // Ninguna cuenta del legacy fue migrada
                continue;
            }

            const existing = expensesMap[nombre_CG.toUpperCase()];

            // 4. Ya existe → actualizar o insertar detalles
            if (existing) {

                const [details] = await conn.query(
                    `SELECT * FROM costs_expenses_details 
                     WHERE FK_COSTEXPENSEID = ? AND FK_COD_EMP = ?;`,
                    [existing.COSTEXPENSEID, newCompanyId]
                );

                const dbDetail = details[0];

                if (!dbDetail) {
                    // Insertar detalle
                    const insertParams = [
                        existing.COSTEXPENSEID,
                        codCosto,
                        codGasto,
                        estado_CG,
                        newCompanyId
                    ];

                    const [res] = await conn.query(`
                        INSERT INTO costs_expenses_details
                        (FK_COSTEXPENSEID, FK_CTA_COSTS, FK_CTA_EXPE, ESTADOCOST, FK_COD_EMP)
                        VALUES (?, ?, ?, ?, ?);
                    `, insertParams);

                    costeExpenseMap[id] = res.insertId;

                } else {
                    // Actualizar detalle
                    const updateParams = [
                        existing.COSTEXPENSEID,
                        codCosto,
                        codGasto,
                        estado_CG,
                        dbDetail.COSTEXPENSEDETAILID,
                        newCompanyId
                    ];

                    const [res] = await conn.query(`
                        UPDATE costs_expenses_details 
                        SET FK_COSTEXPENSEID=?, FK_CTA_COSTS=?, FK_CTA_EXPE=?, ESTADOCOST=?
                        WHERE COSTEXPENSEDETAILID = ? AND FK_COD_EMP = ?;
                    `, updateParams);

                    costeExpenseMap[id] = dbDetail.COSTEXPENSEDETAILID;
                }

            } else {
                // 5. Insertar un nuevo costo porque no existe
                const { insertId } = await insertNewCostMdl(conn, {
                    nombreCostoGasto: nombre_CG,
                    statusTax: estado_CG,
                    planCountActivoId: codCosto,
                    planCountPasivoId: codGasto,
                    companyId: newCompanyId,
                    codMig: id
                });

                costeExpenseMap[id] = insertId;
            }
        }

    } catch (error) {
        throw new Error(`Error en migrateExpensesDetails:${error}`);
    }

    return costeExpenseMap;
}


// =========================================
// Insertar nuevo costo + detalle
// =========================================
async function insertNewCostMdl(conn: any, costsData: any) {
    try {
        const [tax] = await conn.query(`
            INSERT INTO costs_expenses
            (NOMBRE_COSTEXP, DESC_COSTEXP, ESTADOCOSTEXP, FK_COD_EMP, FECHA_REG)
            VALUES (?, ?, ?, ?, NOW());
        `, [
            costsData.nombreCostoGasto,
            costsData.nombreCostoGasto,
            costsData.statusTax,
            costsData.companyId
        ]);

        if (tax.affectedRows === 0) {
            throw new Error('❌ Error al insertar costo/gasto.');
        }
        const [taxDetail] = await conn.query(`
            INSERT INTO costs_expenses_details
            (FK_COSTEXPENSEID, FK_COD_EMP, FK_CTA_COSTS, FK_CTA_EXPE, ESTADOCOST)
            VALUES (?, ?, ?, ?, ?);
        `, [
            tax.insertId,
            costsData.companyId,
            costsData.planCountActivoId,
            costsData.planCountPasivoId,
            costsData.statusTax
        ]);

        if (taxDetail.affectedRows === 0) {
            throw new Error('❌ Error al insertar detalle del costo.');

        }

        return taxDetail;

    } catch (error) {
        console.log("Error insertNewCostMdl:", error);
        throw error;
    }
}
