



export async function migrateDocRetentions(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    purchaseLiquidationAuditIdMap: Record<number, number>,
    mapAuditSales: Record<number, number>
): Promise<Record<string, number>> {

    console.log("Migrando Documentos recibidos SRI...");

    // Obtiene docRetentions únicas normalizadas
    const [rows] = await legacyConn.query(`SELECT
    cod_retsri,
    f_emision AS D_ISSUE_RET,
    f_autorizacion AS D_AUTHORIZATION_RET,
    tipo_emision AS TYPE_ISSUE_RET,
    documento_relacionado AS DOC_RELATION_RET,
    tipo_comprobante AS TYPE_INVOICE_RET,
    CASE WHEN tipo_comprobante='Comprobante de Retencion' THEN '07'
    WHEN tipo_comprobante='Factura' THEN '01'
    WHEN tipo_comprobante='Notas de Credito' THEN '04' ELSE NULL END  AS CODE_DOC_RET,
    serie_retencion AS SERIE_RET,
    ruc_emisor AS TRANSMITTER_RUC_RET,
    razon_social AS RAZSOC_EMP_RET,
    ident_receptor AS IDENT_RECEIVER_RET,
    clave_acceso AS ACCESS_KEY_RET,
    num_autorizacion AS N_AUTHORIZATION_RET,
    importe_total AS TOTAL_AMOUNT_RET,
    
    CASE WHEN estado='Anulado' THEN 'ANULADO'
    WHEN estado='No registrado' THEN 'NO_REGISTRADO'
    WHEN estado='Registrado' THEN 'REGISTRADO' ELSE NULL END  AS STATE_RET,
    fk_cod_trac,
    confirm_compra,
    NULL AS FK_AUDIT,
    NULL AS STATE_FILE_RET FROM  retenciones_sri WHERE 1;`);

    const docRetentions = rows as any[];
    const mapRetentionsSri: Record<string, number> = {};
    if (!docRetentions.length) {
        return mapRetentionsSri
    }

    const BATCH_SIZE = 1000;


    for (let i = 0; i < docRetentions.length; i += BATCH_SIZE) {
        const batch = docRetentions.slice(i, i + BATCH_SIZE);

        const values = batch.map(p => {

            let idAudit = null;
            if (p.CODE_DOC_RET === '01') {
                idAudit = purchaseLiquidationAuditIdMap[p.fk_cod_trac] ?? null;
                p.STATE_FILE_RET = idAudit != null ? 1 : 0

            }

            if (p.CODE_DOC_RET === '07') {
                idAudit = mapAuditSales[p.fk_cod_trac] ?? null;
                p.STATE_FILE_RET = idAudit != null ? 1 : 0
            }

            return [
                p.COD_RET,
                p.D_ISSUE_RET,
                p.D_AUTHORIZATION_RET,
                p.TYPE_ISSUE_RET,
                p.DOC_RELATION_RET,
                p.TYPE_INVOICE_RET,
                p.CODE_DOC_RET,
                p.SERIE_RET,
                p.TRANSMITTER_RUC_RET,
                p.RAZSOC_EMP_RET,
                p.IDENT_RECEIVER_RET,
                p.ACCESS_KEY_RET,
                p.N_AUTHORIZATION_RET,
                p.TOTAL_AMOUNT_RET,
                p.STATE_RET,
                p.STATE_FILE_RET,
                newCompanyId,
                idAudit
            ];
        });

        const [res]: any = await conn.query(
            `INSERT INTO docRetentions (COD_RET, D_ISSUE_RET, D_AUTHORIZATION_RET, TYPE_ISSUE_RET, DOC_RELATION_RET, TYPE_INVOICE_RET, CODE_DOC_RET, SERIE_RET, TRANSMITTER_RUC_RET, RAZSOC_EMP_RET, IDENT_RECEIVER_RET, ACCESS_KEY_RET, N_AUTHORIZATION_RET, TOTAL_AMOUNT_RET, STATE_RET, STATE_FILE_RET, FK_COD_EMP, FK_AUDIT) VALUES ?`,
            [values]
        );

        let newId = res.insertId;

        for (const b of batch) {
            mapRetentionsSri[b.ID_PROJECT] = newId;
            newId++;
        }

        console.log(` -> Batch migrado: ${batch.length} Documentos recibidos SRI`);
    }
    return mapRetentionsSri;
}


