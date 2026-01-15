export interface oldProductCodeMap {
    id: number;
    precioProducto: number;
}

export async function batchInsertProducts(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapAccounts: Record<number, number | null>,
    mapCategories: Record<number, number | null>,
    mapMeasures: Record<number, number | null>,
    mapBrand
): Promise<{ mapProducts: Record<number, number>; oldProductCodeMap: Map<string, oldProductCodeMap> }> {


    console.log("Migrando productos...");

    const [rows] = await legacyConn.query(`SELECT
                                                COD_PRO AS PROD_ID,
                                                EST_PRO AS PROD_STATUS,
                                                CODN_PRO AS PROD_COD,
                                                CODA_PRO AS PROD_CODAUX,
                                                COD_AUX_CONST AS PROD_CODAUX_CONST,
                                                CODBAR_PRO AS PROD_CODBAR,
                                                NOM_PRO AS PROD_NAME,
                                                FK_COD_CAT AS PROD_CATE,
                                                FK_COD_MED AS PROD_UNIT,
                                                MARSUB_PRO AS PROD_BRAND,
                                                COMADI_PRO AS PROD_COMENAD,
                                                DES_PRO AS PROD_DESCR,
                                                FECCAD_PRO AS PROD_FEC_EXPI,
                                                TIP_PRO AS PROD_TYPE,
                                                IMG_PRO AS PROD_IMG,
                                                FECCREA_PRO AS PROD_FEC_REG,
                                                PRECOS_PREC AS PROD_PRICE_COST,
                                                PREPUB_PREC AS PROD_PRICE_PUB,
                                                PREDIS_PREC AS PROD_PRICE_DISTR,
                                                PREDISDES_PREC AS PROD_DESC_DISTR,
                                                PREMAY_PREC AS PROD_PRICE_MAY,
                                                PREMAYDES_PREC AS PROD_DESC_MAY,
                                                PRELIQ_PREC AS PROD_PRICE_CARD,
                                                PRELIQDES_PREC AS PROD_DESC_CARD,
                                                PREE_PREC AS PROD_PRICE_E,
                                                PREEDES_PREC AS PROD_DESC_E,
                                                PREF_PREC AS PROD_PRICE_F,
                                                PREFDES_PREC AS PROD_DESC_F,
                                                PREMAN_PRO AS PROD_PRICE_MAN,
                                                FK_CTA_PLAN_VENTA AS FK_CTA_VENTA,
                                                FK_CTA_PLAN_COMPRA AS FK_CTA_COMPRA,
                                                FK_CTA_PLAN_COSTOS AS FK_CTA_COSTO,
                                                FK_IMPUESTO AS FK_IMP,
                                                HIST_PREC_PROD AS PROD_HISTORY_PRICE,
                                                0 AS PROD_PROCESS,
                                                0 AS PROD_DETINV,
                                                OMIT_AUX_TRANS
                                            FROM
                                                producto
                                            WHERE
                                                1;`);
    const productos = rows as any[];

    if (!productos.length) {
        throw new Error(" -> No hay productos para migrar.");
    }
    const BATCH_SIZE = 1000;
    const mapProducts: Record<number, number> = {};
    const oldProductCodeMap = new Map<string, oldProductCodeMap>();

    for (let i = 0; i < productos.length; i += BATCH_SIZE) {
        const batch = productos.slice(i, i + BATCH_SIZE);

        const values = batch.map(p => {

            const catId = mapCategories[p.PROD_CATE];

            const medId = mapMeasures[p.PROD_UNIT];
            let marcaId = null;
            if (p.MARSUB_PRO == null || p.MARSUB_PRO == '') {
                marcaId = mapBrand['SIN MARCA'];
            } else {
                marcaId = mapBrand[p.MARSUB_PRO];
            }

            const fk_venta = mapAccounts[p.FK_CTA_VENTA] ?? null;
            const fk_compra = mapAccounts[p.FK_CTA_COMPRA] ?? null;
            const fk_costo = mapAccounts[p.FK_CTA_COSTO] ?? null;

            return [
                p.PROD_STATUS == 1 ? 'ACTIVO' : 'DESACTIVO',
                p.PROD_COD,
                p.PROD_CODAUX,
                p.PROD_CODAUX_CONST,
                p.PROD_CODBAR,
                p.PROD_NAME,
                catId,
                medId,
                marcaId,
                p.PROD_COMENAD,
                p.PROD_DESCR,
                p.PROD_FEC_EXPI,
                p.PROD_TYPE?.toUpperCase() || 'PRODUCTO SIMPLE',
                p.PROD_IMG,
                p.PROD_FEC_REG,
                p.PROD_PRICE_COST,
                p.PROD_PRICE_PUB,
                p.PROD_PRICE_DISTR,
                p.PROD_DESC_DISTR,
                p.PROD_PRICE_MAY,
                p.PROD_DESC_MAY,
                p.PROD_PRICE_CARD,
                p.PROD_DESC_CARD,
                p.PROD_PRICE_E,
                p.PROD_DESC_E,
                p.PROD_PRICE_F,
                p.PROD_DESC_F,
                p.PROD_PRICE_MAN,
                fk_venta,
                fk_compra,
                fk_costo,
                p.FK_IMP,
                p.PROD_HISTORY_PRICE,
                p.PROD_PROCESS,
                p.PROD_DETINV,
                p.OMIT_AUX_TRANS,
                newCompanyId
            ];
        });


        //PROD_ID
        try {
            const [res]: any = await conn.query(`
            INSERT INTO products(  
                                               PROD_STATUS,
                                               PROD_COD,
                                                PROD_CODAUX,
                                                PROD_CODAUX_CONST,
                                                PROD_CODBAR,
                                                PROD_NAME,
                                                PROD_CATE,
                                                PROD_UNIT,
                                                PROD_BRAND,
                                                PROD_COMENAD,
                                                PROD_DESCR,
                                                PROD_FEC_EXPI,
                                                PROD_TYPE,
                                                PROD_IMG,
                                                PROD_FEC_REG,
                                                PROD_PRICE_COST,
                                                PROD_PRICE_PUB,
                                                PROD_PRICE_DISTR,
                                                PROD_DESC_DISTR,
                                                PROD_PRICE_MAY,
                                                PROD_DESC_MAY,
                                                PROD_PRICE_CARD,
                                                PROD_DESC_CARD,
                                                PROD_PRICE_E,
                                                PROD_DESC_E,
                                                PROD_PRICE_F,
                                                PROD_DESC_F,
                                                PROD_PRICE_MAN,
                                                FK_CTA_VENTA,
                                                FK_CTA_COMPRA,
                                                FK_CTA_COSTO,
                                                FK_IMP,
                                                PROD_HISTORY_PRICE,
                                                PROD_PROCESS,
                                                PROD_DETINV,
                                                OMIT_AUX_TRANS, FK_COD_EMP) VALUES  ?`,
                [values]
            );

            let newId = res.insertId;
            for (const s of batch) {
                mapProducts[s.PROD_ID] = newId;

                oldProductCodeMap.set(
                    `${s.PROD_COD}`,
                    {
                        id: newId,
                        precioProducto: s.PROD_PRICE_PUB
                    }
                )

                newId++;
            }
            console.log(` -> Batch migrado: ${batch.length} productos`);
        } catch (err) {
            throw err;
        }
    }

    return { mapProducts, oldProductCodeMap };
}
