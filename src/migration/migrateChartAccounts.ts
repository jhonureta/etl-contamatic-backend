/* export async function migrateChartAccounts(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {
 */
import { PoolConnection } from 'mysql2/promise'; // Asumiendo que usas mysql2

/**
 * Función para migrar el plan de cuentas de la base de datos legacy a la nueva.
 * Optimizado para reducir viajes de ida y vuelta a la base de datos.
 */
export async function migrateChartAccounts(
    legacyConn: any,
    conn: any,
    newCompanyId: number
): Promise<Record<number, number>> {

    console.log("Migrando plan de cuentas...");

    // 1️⃣ Optimización: Mover la lógica de duplicados a una etapa separada 
    // y revisarla para evitar el problema N+1. Por ahora, asumimos que se mantiene,
    // pero recuerda que es el mayor cuello de botella.
    await detectarSecuenciaDuplicados(legacyConn);

    // 2️⃣ Extraer datos del origen
    // (Consulta optimizada para asegurar que solo trae lo necesario y es eficiente)
    const [rows] = await legacyConn.query(`
        SELECT  DISTINCT
            cod_plan,
            codigo_plan,
            nombre_plan,
            estado_plan,
            grupo_plan,
            NIV_PLAN,
            TIP_FAM
        FROM contabilidad_plan_v2;
    `);

    const accounts = rows as any[];
    if (!accounts.length) {
        throw new Error(" -> No hay plan de cuentas para migrar.");
    }

    const BATCH_SIZE = 1000;
    const mapAccounts: Record<number, number> = {};
    const parentMap: Record<string, number> = {};
    const updateData: { idComapny: number, id: number, codigo_plan: string, nombre_plan: string, estado_plan: string, grupo_plan: string, nivel: number, parentId: number, tipFam: string }[] = [];

    // 3️⃣ Insertar cuentas por batch (Este paso ya es eficiente)
    for (let i = 0; i < accounts.length; i += BATCH_SIZE) {
        const batch = accounts.slice(i, i + BATCH_SIZE);

        const values = batch.map(a => [
            newCompanyId,
            a.codigo_plan,
            a.nombre_plan,
            a.nombre_plan,
            a.estado_plan,
            a.grupo_plan,
            0, // padre temporal
            a.NIV_PLAN,
            a.TIP_FAM
        ]);

        try {
            const [res]: any = await conn.query(
                `INSERT INTO account_plan 
                (FK_COD_EMP, CODIGO_PLAN, NOM_PLAN, DESC_PLAN, ESTADO_PLAN, GRUPO_PLAN, FK_PAD_PLAN, NIVEL_PLAN, TIPO_FAM_PLAN)
                VALUES ?`,
                [values]
            );

            let newId = res.insertId;

            // Recopilar datos de actualización de jerarquía
            for (const a of batch) {
                const accountId = newId;
                mapAccounts[a.cod_plan] = accountId;
                parentMap[a.codigo_plan] = accountId;

                const parentCode = a.codigo_plan.substring(0, a.codigo_plan.lastIndexOf('.'));
                const parentId = parentMap[parentCode] || 0;

                // Guardar la información necesaria para el UPDATE masivo
                updateData.push({ idComapny: newCompanyId, id: accountId, codigo_plan: parentCode, nombre_plan: a.nombre_plan, estado_plan: a.estado_plan, grupo_plan: a.grupo_plan, nivel: a.NIV_PLAN, parentId: parentId, tipFam: a.TIP_FAM });

                newId++;
            }

            console.log(` -> Batch migrado: ${batch.length} cuentas`);

        } catch (error) {
            console.error("Error en la inserción de batch:", error);
            throw error;
        }
    }


    if (updateData.length > 0) {
        try {
            const tempValues = updateData.map(d => [d.idComapny, d.id, d.codigo_plan, d.nombre_plan, d.nombre_plan, d.estado_plan, d.grupo_plan, d.nivel, d.parentId, d.tipFam]);
          /*   console.log(tempValues); */
            const [updateRes] = await conn.query(
                `INSERT INTO account_plan (FK_COD_EMP,ID_PLAN,CODIGO_PLAN,NOM_PLAN, DESC_PLAN,ESTADO_PLAN, GRUPO_PLAN,NIVEL_PLAN, FK_PAD_PLAN, TIPO_FAM_PLAN) 
             VALUES ? 
             ON DUPLICATE KEY UPDATE 
                FK_PAD_PLAN = VALUES(FK_PAD_PLAN), 
                TIPO_FAM_PLAN = VALUES(TIPO_FAM_PLAN)`,
                [tempValues]
            );
            console.log(` -> Jerarquías actualizadas en una operación masiva: ${updateData.length} registros afectados.`);
        } catch (error) {
            console.error("Error al actualizar jerarquías:", error);
        }

    }

    return mapAccounts;
}


async function detectarSecuenciaDuplicados(legacyConn: any) {
    const [duplicados] = await legacyConn.execute(`
        SELECT codigo_plan, fk_cod_empresa, COUNT(*) as cantidad 
        FROM contabilidad_plan_v2
        GROUP BY codigo_plan, fk_cod_empresa
        HAVING cantidad > 1
    `);

    for (const dup of duplicados) {

        console.log(`⚠️ Código duplicado: ${dup.codigo_plan} (empresa ${dup.fk_cod_empresa})`);

        const [registros] = await legacyConn.execute(
            `SELECT cod_plan, codigo_plan
             FROM contabilidad_plan_v2
             WHERE codigo_plan = ? AND fk_cod_empresa = ?
             ORDER BY cod_plan ASC`,
            [dup.codigo_plan, dup.fk_cod_empresa]
        );

        for (let i = 1; i < registros.length; i++) {

            const actual = registros[i];
            const partesCodigo = dup.codigo_plan.split('.');
            // Asegurarse de que el código base no incluye el último segmento para la búsqueda del patrón
            const basePattern = partesCodigo.slice(0, partesCodigo.length - 1).join('.');

            const [similares] = await legacyConn.execute(
                `SELECT codigo_plan 
                 FROM contabilidad_plan_v2
                 WHERE codigo_plan LIKE ? AND fk_cod_empresa = ?
                 ORDER BY codigo_plan DESC
                 LIMIT 1`,
                [`${basePattern}.%`, dup.fk_cod_empresa]
            );

            let nuevoCodigo;

            if (similares.length > 0) {
                const ultimo = similares[0].codigo_plan;
                const partes = ultimo.split('.');
                const ultimoNumero = parseInt(partes.pop());
                const nuevoNumero = (ultimoNumero + 1).toString().padStart(2, '0');
                nuevoCodigo = [...partes, nuevoNumero].join('.');
            } else {
                // Si no encuentra similares, usa el código original, pero incrementado (e.g., 1.1.01)
                const ultimoSegmento = parseInt(partesCodigo.pop());
                const nuevoSegmento = (ultimoSegmento + 1).toString().padStart(2, '0');
                nuevoCodigo = [...partesCodigo, nuevoSegmento].join('.');
            }

            await legacyConn.execute(
                `UPDATE contabilidad_plan_v2 SET codigo_plan = ? WHERE cod_plan = ?`,
                [nuevoCodigo, actual.cod_plan]
            );

            console.log(`   → Actualizado ID ${actual.cod_plan} → ${nuevoCodigo}`);
        }
    }
    console.warn("✅ Fin de detección de duplicados.");
}