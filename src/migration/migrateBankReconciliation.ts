export async function migrateBankReconciliation(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    bankMap: Record<number, number | null>,
): Promise<Record<number, number>> {
    const mapConciliation: Record<number, number> = {};
    console.log('Migrando conciliaciones bancarias...');

    try {
        const queryConciliacion = `
            SELECT ID_CONCI, FECHA_CORTE, SALDO_BANCARIO, SALDO_CONTABLE, 
                   DIFERENCIA_CONC, DESCRIP_CONC, ESTADO, ACTUALIZACION, 
                   RESPONSABLE, FK_COD_BANC_CONC
            FROM conciliacion_bancaria;
        `;

        const [dataConciliacion]: any = await legacyConn.query(queryConciliacion);
        if (!dataConciliacion || !dataConciliacion.length) return mapConciliation;

        // 1. Obtener el punto de partida para CODIGO_AUT
        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit
             FROM audit WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let currentAuditSeq = nextAudit;
        const BATCH_SIZE = 1000; // Reducido un poco para estabilidad
        let secuencia = 1;
        for (let i = 0; i < dataConciliacion.length; i += BATCH_SIZE) {
            const batch = dataConciliacion.slice(i, i + BATCH_SIZE);

            // --- PASO A: Insertar Auditoría en Bloque ---
            const auditValues = batch.map(() => [
                currentAuditSeq++,
                'CONCILIACION_BANCARIA',
                newCompanyId
            ]);

            const [resAudit]: any = await conn.query(
                `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
                [auditValues]
            );

            // El primer ID generado por este batch de auditoría
            let firstAuditId = resAudit.insertId;

            // --- PASO B: Preparar datos de Conciliación ---
            const reconciliationValues = batch.map((c: any, index: number) => {
                const idBanco = bankMap[c.FK_COD_BANC_CONC] ?? null;
                return [
                    c.FECHA_CORTE ?? null,
                    c.SALDO_BANCARIO ?? null,
                    c.SALDO_CONTABLE ?? null,
                    c.DIFERENCIA_CONC ?? null,
                    c.DESCRIP_CONC ?? null,
                    c.ESTADO ?? null,
                    c.ACTUALIZACION ?? null,
                    c.RESPONSABLE ?? null,
                    idBanco,
                    newCompanyId,
                    firstAuditId + index,
                    secuencia++
                    // Asignamos el ID de auditoría correspondiente
                ];
            });

            // --- PASO C: Insertar Conciliaciones en Bloque ---
            const [resConcil]: any = await conn.query(
                `INSERT INTO bank_reconciliation(
                    FECHA_CORTE, SALDO_BANCARIO, SALDO_CONTABLE, DIFERENCIA_CONC, DESCRIP_CONC,
                    ESTADO, ACTUALIZACION, RESPONSABLE, FK_COD_BANC_CONC, FK_COD_EMP, FK_AUDITCONC, SECU_CONC
                ) VALUES ?`,
                [reconciliationValues]
            );

            // --- PASO D: Mapear IDs para el retorno ---
            let currentNewId = resConcil.insertId;
            for (const item of batch) {
                mapConciliation[item.ID_CONCI] = currentNewId++;
            }

            console.log(` -> Batch migrado: ${i + batch.length} / ${dataConciliacion.length}`);
        }

        return mapConciliation;
    } catch (error: any) {
        console.error('Error migrando conciliaciones bancarias:', error);
        throw error;
    }
}