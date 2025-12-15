export async function migrateCustomerObligations(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapSales: Record<number, number | null>,
    mapAuditSales: Record<number, number | null>,
    mapClients: Record<number, number | null>
): Promise<{
    mapObligationsCustomers: Record<number, number>,
    mapObligationsAudit: Record<number, number>
}> {

    console.log("🚀 Migrando obligaciones clientes");

    const mapObligationsCustomers: Record<number, number> = {};
    const mapObligationsAudit: Record<number, number> = {};

    try {

        const [[{ nextAudit }]]: any = await conn.query(
            `SELECT IFNULL(MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1, 1) AS nextAudit
             FROM audit
             WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        let auditSeq: number = nextAudit;

        const [rows]: any[] = await legacyConn.query(`
            SELECT
                cod_cp AS old_id,
                fk_cod_cli_cp AS fk_persona_old,
                FK_TRAC_CUENTA AS fk_cod_trans_old,
                OBL_ASI AS obl_asi_group,
                Tipo_cxp AS tipo_obl,
                fecha_emision_cxp AS fech_emision,
                fecha_vence_cxp AS fech_vencimiento,
                tipo_documento AS tip_doc,
                estado_cxp AS estado,
                saldo_cxp AS saldo,
                valor_cxp AS total,
                referencia_cxp AS ref_secuencia,
                TIPO_CUENTA AS tipo_cuenta,
                FK_SERVICIO AS fk_id_infcon,
                TIPO_ESTADO_CUENTA AS tipo_estado_cuenta
            FROM cuentascp
            WHERE Tipo_cxp = 'CXC'
            ORDER BY cod_cp
        `);

        if (!rows.length) {
            return { mapObligationsCustomers, mapObligationsAudit };
        }

        const oblAsiToAuditId: Record<number, number> = {};
        let importadoAuditId: number | null = null;

        const BATCH_SIZE = 500;

        for (let i = 0; i < rows.length; i += BATCH_SIZE) {
            const batch = rows.slice(i, i + BATCH_SIZE);
            const insertValues: any[] = [];

            for (const o of batch) {

                let auditId: number | null = null;

   
                if (o.fk_cod_trans_old && mapAuditSales[o.fk_cod_trans_old]) {
                    auditId = mapAuditSales[o.fk_cod_trans_old]!;
                }


                else if (o.obl_asi_group !== null) {
                    if (!oblAsiToAuditId[o.obl_asi_group]) {
                        const codigoAut = auditSeq++;

                        const [resAudit]: any = await conn.query(
                            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP)
                             VALUES (?, 'VENTAS', ?)`,
                            [codigoAut, newCompanyId]
                        );

                        oblAsiToAuditId[o.obl_asi_group] = resAudit.insertId;
                    }

                    auditId = oblAsiToAuditId[o.obl_asi_group];
                }

                else if (o.tipo_cuenta === 'Importado') {
                    if (!importadoAuditId) {
                        const codigoAut = auditSeq++;

                        const [resAudit]: any = await conn.query(
                            `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP)
                             VALUES (?, 'VENTAS', ?)`,
                            [codigoAut, newCompanyId]
                        );

                        importadoAuditId = resAudit.insertId;
                    }

                    auditId = importadoAuditId;
                }

                if (!auditId) {
                    throw new Error(`No se pudo resolver AUDIT para obligación ${o.old_id}`);
                }

                mapObligationsAudit[o.old_id] = auditId;

                const customerId = mapClients[o.fk_persona_old];
                if (!customerId) {
                    throw new Error(`Cliente no mapeado: ${o.fk_persona_old}`);
                }

                insertValues.push([
                    customerId,
                    o.tipo_obl,
                    o.fech_emision,
                    o.fech_vencimiento,
                    o.tip_doc,
                    o.estado,
                    o.saldo,
                    o.total,
                    o.ref_secuencia,
                    mapSales[o.fk_cod_trans_old] ?? null,
                    o.tipo_cuenta,
                    o.fk_id_infcon,
                    o.tipo_estado_cuenta,
                    auditId,
                    new Date(),
                    newCompanyId
                ]);
            }

            const [res]: any = await conn.query(`
                INSERT INTO cuentas_obl (
                    FK_PERSONA, TIPO_OBL, FECH_EMISION, FECH_VENCIMIENTO, TIP_DOC,
                    ESTADO, SALDO, TOTAL, REF_SECUENCIA, FK_COD_TRANS,
                    TIPO_CUENTA, FK_ID_INFCON, TIPO_ESTADO_CUENTA, FK_AUDITOB,
                    OBLG_FEC_REG, FK_COD_EMP
                ) VALUES ?
            `, [insertValues]);

            let newId = res.insertId;
            for (const o of batch) {
                mapObligationsCustomers[o.old_id] = newId++;
            }
        }

        console.log("✅ Migración completada correctamente");
        return { mapObligationsCustomers, mapObligationsAudit };

    } catch (err) {
        console.error("❌ Error en migración de obligaciones:", err);
        throw err;
    }
}
