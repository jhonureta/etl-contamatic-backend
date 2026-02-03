/* import { erpPool } from '../config/db';
 */
export async function migrateSales(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    branchMap: any,
    userMap: any,
    mapClients: any,
    mapProducts: any,
    mapRetentions: any,
    oldRetentionCodeMap: any,
    newRetentionIdMap: any,
    storeMap: any
): Promise<{ mapSales: Record<number, number>; mapAuditSales: Record<number, number> }> {
    console.log("Migrando ventas...");

    const [rows] = await legacyConn.query(`SELECT
    COD_TRAC AS COD_TRANS,
    CASE WHEN TIP_TRAC = 'Fisica' THEN SUBSTRING(NUM_TRACFIC, 1, 7) WHEN TIP_TRAC = 'Electronica' THEN SUBSTRING(NUM_TRAC, 1, 7) ELSE SUBSTRING(NUM_TRACCI, 1, 7)
END AS PUNTO_EMISION_DOC,
CASE WHEN TIP_TRAC = 'Fisica' THEN SUBSTRING(NUM_TRACFIC, 9, 9) WHEN TIP_TRAC = 'Electronica' THEN SUBSTRING(NUM_TRAC, 9, 9) ELSE SUBSTRING(NUM_TRACCI, 9, 9)
END AS SECUENCIA_DOC,
SUBSTRING(secuencialFacturaRet, 9, 9) AS SECUENCIA_REL_DOC,
claveFactura AS CLAVE_TRANS,
transacciones.claveFacturaRet AS CLAVE_REL_TRANS,
NULL AS TIP_DET_DOC,
YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
MONTH(FEC_TRAC) AS FEC_MES_TRAC,
FEC_TRAC AS FEC_TRAC,
fechaFacturaRet AS FEC_REL_TRAC,
FEC_MERC_TRAC AS FEC_MERC_TRAC,
CASE WHEN METPAG_TRAC = 'MULTIPLES' THEN METPAG_TRAC ELSE METPAG_TRAC
END AS MET_PAG_TRAC,
OBS_TRAC,
FK_COD_USU AS FK_USER,
FK_USU_CAJA AS FK_USER_VEND,
FK_COD_CLI AS FK_PERSON,
estado AS ESTADO,
estado_compra AS ESTADO_REL,
documentoAnulado AS RUTA_DOC_ANULADO,
SUB_BASE_5 AS SUB_BASE_5,
SUB_BASE_8 AS SUB_BASE_8,
SUB_BASE_12 AS SUB_BASE_12,
SUB_BASE_13 AS SUB_BASE_13,
SUB_BASE_14 AS SUB_BASE_14,
SUB_BASE_15 AS SUB_BASE_15,
SUB12_TRAC AS SUB_12_TRAC,
SUB0_TRAC AS SUB_0_TRAC,
SUBNOBJETO_TRAC AS SUB_N_OBJETO_TRAC,
SUBEXENTO_TRAC AS SUB_EXENTO_TRAC,
SUB_TRAC AS SUB_TRAC,
IVA_TRAC AS IVA_TRAC,
IVA_TRAC,
TOT_TRAC AS TOT_TRAC,
TOTRET_TRAC AS TOT_RET_TRAC,
TOTPAG_TRAC AS TOT_PAG_TRAC,
PROPINA_TRAC,
OTRA_PER,
'01' AS COD_COMPROBANTE,
'07' AS COD_COMPROBANTE_REL,
NULL AS COD_DOCSUS_TRIB,
NULL AS FK_COD_EMP,
firmado AS FRIMADO,
enviado AS ENVIADO,
autorizado AS AUTORIZADO,
enviadoCliente AS ENVIADO_CLIEMAIL,
fechaAutorizado AS FECHA_AUTORIZACION,
NULL AS FECHA_AUTORIZACION_REL,
CASE WHEN secuencialFacturaRet <> '' THEN 'retencion-venta' ELSE NULL
END AS TIP_DOC_REL,
DES_TRAC AS DSTO_TRAC,
NULL AS FK_CODSUCURSAL,
NULL AS FK_AUDITTR,
CASE WHEN TIP_TRAC = 'Electronica' THEN 'factura' WHEN TIP_TRAC = 'Fisica' THEN 'fisica' ELSE 'comprobante-ingreso'
END AS TIP_TRAC,
fecha AS FECHA_REG,
DET_TRAC AS DOCUMENT_DETAIL,
NULL AS PUNTO_EMISION_REC,
fechaAnulacion AS FECHA_ANULACION,
CASE WHEN TIP_TRAC = 'Electronica' THEN 'factura' WHEN TIP_TRAC = 'Fisica' THEN 'fisica' ELSE 'comprobante-ingreso'
END AS TIP_DOC,
NULL AS SRI_PAY_CODE,
IP_CLIENTE AS CLIENT_IP,
NULL AS FK_AUDIT_REL,
CASE WHEN TIP_TRAC = 'Electronica' THEN NUM_TRAC WHEN TIP_TRAC = 'Fisica' THEN NUM_TRACFIC ELSE NUM_TRACCI end NUM_TRANS,
secuencialFacturaRet AS NUM_REL_DOC,
serial_producto AS DIV_PAY_YEAR,
cabecera_compra AS DOCUMENT_REL_DETAIL,
RESPU_SRI AS RESP_SRI,
INFO_ADIC AS INFO_ADIC,
DET_REMBOLSO AS DET_EXP_REEMBOLSO,
METPAG_JSON_TRAC AS JSON_METODO,
INFO_ADIC AS ITEMS_PROF,
OBS_AUXILIAR AS OBS_AUXILIAR,
OBS_ORD AS OBS_ORDEN
FROM
    transacciones
WHERE
    transacciones.TIP_TRAC IN(
        'Electronica',
        'Fisica',
        'comprobante-ingreso'
    )
ORDER BY
    COD_TRAC
DESC;`);
    const ventas = rows as any[];

    if (!ventas.length) {
        throw new Error(" -> No hay ventas para migrar.");
    }

    //Ultima secuencia de auditoria
    const secuenciaSucursal = `SELECT
    COD_SURC,
    SUBSTRING(secuencial, 1, 7) AS ELECTRONICA,
    SUBSTRING(secuencialFisica, 1, 7) AS FISICA,
    SUBSTRING(SURC_SEC_COMPINGR, 1, 7) AS COMPINGRESO
FROM
    sucursales;`;
    const [dataSucursal] = await legacyConn.query(secuenciaSucursal, [newCompanyId]);

    const listElectronica = [];
    const listFisica = [];
    const listComprobante = [];

    for (let x = 0; x < dataSucursal.length; x++) {

        listElectronica[dataSucursal[x].ELECTRONICA] = dataSucursal[x].COD_SURC;
        listFisica[dataSucursal[x].FISICA] = dataSucursal[x].COD_SURC;
        listComprobante[dataSucursal[x].COMPINGRESO] = dataSucursal[x].COD_SURC;
    }

    const secuencia_query = `SELECT IFNULL(MAX(CODIGO_AUT+0)+1,1) as codigoAuditoria FROM audit WHERE FK_COD_EMP=?;`;
    const [dataSecuencia] = await conn.query(secuencia_query, [newCompanyId]);
    let codigoAuditoria = dataSecuencia[0]['codigoAuditoria'];


    const BATCH_SIZE = 1000;
    const mapSales: Record<number, number> = {};
    const mapAuditSales: Record<number, number> = {};

    const transformarProductos = (productosOriginales: any) => {
        // Parsear si es string JSON
        let productos = productosOriginales;
        if (typeof productosOriginales === 'string') {
            try {
                productos = JSON.parse(productosOriginales);
            } catch (e) {
                console.error("Error al parsear JSON:", e);
                return [];
            }
        }

        if (!Array.isArray(productos)) {
            console.error("El input debe ser un array.", typeof productos, productos);
            return [];
        }
        return productos.map(producto => {
            const precioSinIvaNum = parseFloat(producto.precioSinIva);
            const impuestoPorcentaje = parseFloat(producto.impuesto);
            const valorIva = (precioSinIvaNum * impuestoPorcentaje) / 100;
            const preciosAdaptados = (producto.valores || []).map((valorItem: any, index: number) => ({
                name: valorItem.nombre,
                price: String(valorItem.valor) || "0",
                discount: valorItem.descuento || 0,
                select: index === 0
            }));
            return {
                idProducto: parseInt(mapProducts[producto.idProducto]) || null,
                idBodega: parseInt(storeMap[producto.idBodega]) || null,
                codigo: producto.codigo,
                nombre: producto.nombre,
                stock: parseInt(producto.stock) || 0,
                cantidad: parseFloat(producto.cantidad) || 0,
                impuesto: impuestoPorcentaje,
                codigoImpuesto: parseInt(producto.codigoimpuesto) || 0,
                nombreImpuesto: producto.nombreImpuesto,
                esCombo: false,
                costo: 0,
                marca: producto.unidad || "SIN MARCA",
                precioProducto: producto.precioSinIva,
                precioSinIva: producto.precioSinIva,
                precioPlusIva: valorIva.toFixed(6),
                porcentajeDescuento: parseFloat(producto.porcentajeDescuento) || 0,
                valorDescuento: parseFloat(producto.valorDescuento) || 0,
                total: String(producto.tota),
                tota: String(producto.tota),
                preciomanual: parseInt(producto.preciomanual) || 1,
                codigoAuxiliar: producto.codigoAuxiliar || producto.codigo,
                precios: preciosAdaptados
            };
        });
    };


    const formatDecimal = (value: any, decimals: number = 2): string => {
        const num = typeof value === 'number' ? value : parseFloat(value || 0);
        return isNaN(num) ? '0.00' : num.toFixed(decimals);
    };

    const parseInteger = (value: any): number | null => {
        const num = parseInt(value, 10);
        return isNaN(num) ? null : num;
    };

    const adaptarEstructuraMultiple = (estructuraAntigua: any): any[] => {
        try {
            // Parsear si es string JSON
            let datos = estructuraAntigua;
            if (typeof estructuraAntigua === 'string') {
                try {
                    datos = JSON.parse(estructuraAntigua);
                } catch (parseError) {
                    console.error('Error al parsear JSON de retencion:', parseError);
                    return [];
                }
            }

            // Normalizar: si viene como array, tomar el primer elemento; si es objeto, usarlo directamente
            const datosOriginales = Array.isArray(datos) ? datos[0] : datos;

            if (!datosOriginales || typeof datosOriginales !== 'object') {
                console.warn('Estructura de retención vacía o inválida');
                return [];
            }

            // Mapear retenciones por renta (Retención en la Fuente)
            const nuevasRetencionesRenta = (datosOriginales.listadoRetenciones || [])
                .filter((ret: any) => ret && ret.renta) // Filtrar nulos/undefined y renta vacío
                .map((ret: any) => ({
                    codigoRenta: ret.renta || null,
                    idRetRenta: newRetentionIdMap[ret.idRetencionRenta],
                    nombreRenta: ret.nombreRetencionFuente || null,
                    porcentajeRenta: formatDecimal(ret.porcentaje),
                    subtotalBase0: formatDecimal(ret.subtotalBase0),
                    subtotalDiferente: formatDecimal(ret.subtotalDiferente),
                    valorRetenido: formatDecimal(ret.valorRetenido)
                }));

            // Mapear retenciones por IVA
            const nuevasRetencionesIva = (datosOriginales.listadoRetencionesIva || [])
                .filter((ret: any) => ret && ret.rentaIva)
                .map((ret: any) => {
                    // Filtrar impuestos activos con tarifa > 0
                    const impuestosActivos = (ret.arraryImpuestos || [])
                        .filter((imp: any) => imp && imp.impuestoActivoIva === 1 && parseFloat(imp.tarifa) > 0)
                        .map((imp: any) => ({
                            codigo: parseInteger(imp.codigo),
                            tarifa: parseInteger(imp.tarifa),
                            total: formatDecimal(imp.total)
                        }));

                    return {
                        codigoIva: ret.rentaIva || null,
                        idRetIva: newRetentionIdMap[ret.idRetencionIva],
                        nombreIva: ret.nombreRetencionIva || null,
                        porcentajeIva: formatDecimal(ret.porcentajeIva),
                        subtotalDiferenteIva: formatDecimal(ret.subtotalDiferenteIva),
                        valorRetenido: formatDecimal(ret.valorRetenidoIva),
                        impuestos: impuestosActivos
                    };
                });

            return [
                {
                    listadoRetenciones: nuevasRetencionesRenta,
                    listadoRetencionesIva: nuevasRetencionesIva
                }
            ];
        } catch (error) {
            console.error('Error en adaptarEstructuraMultiple:', error);
            return [];
        }
    };



    for (let i = 0; i < ventas.length; i += BATCH_SIZE) {
        const batch = ventas.slice(i, i + BATCH_SIZE);
        try {
            // Inicio de transacción por batch: agrupar audit + insert customers

            // Preparar inserción en lote para la tabla `audit`
            const auditValues: any[] = [];
            for (let j = 0; j < batch.length; j++) {
                auditValues.push([codigoAuditoria, 'VENTAS', newCompanyId]);
                codigoAuditoria++;
            }

            const [auditRes]: any = await conn.query(
                `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
                [auditValues]
            );

            // El insertId corresponde al primer id insertado; mapearlos por orden
            let firstAuditInsertId = auditRes.insertId;
            for (let j = 0; j < batch.length; j++) {
                const codTrans = batch[j].COD_TRANS;
                const auditId = firstAuditInsertId + j;
                mapAuditSales[codTrans] = auditId;
            }




            // Preparar valores para INSERT en batch
            const values = batch.map((t, index) => {
                console.log(`transformando y normalizando ${t.NUM_TRANS}`);
                const jsonAdaptado = transformarProductos(t.DOCUMENT_DETAIL);
                const auditId = mapAuditSales[t.COD_TRANS];
                const vendedor = userMap[t.FK_USER_VEND];
                const creador = userMap[t.FK_USER];
                const cliente = mapClients[t.FK_PERSON];

                let idSucursal = null;
                if (t.TIP_TRAC == 'comprobante-ingreso') {

                    idSucursal = branchMap[listComprobante[t.PUNTO_EMISION_DOC]];
                }
                if (t.TIP_TRAC == 'fisica') {
                    idSucursal = branchMap[listFisica[t.PUNTO_EMISION_DOC]];
                }
                if (t.TIP_TRAC == 'factura') {
                    idSucursal = branchMap[listElectronica[t.PUNTO_EMISION_DOC]];
                }

                function safeJson(input: any) {
                    try {
                        if (typeof input === "string") {
                            JSON.parse(input);        // verificar validez
                            return input;
                        }
                        return JSON.stringify(input ?? {});
                    } catch {
                        return "{}"; // fallback JSON válido
                    }
                }



                const retencionVentaNueva = adaptarEstructuraMultiple(t.DOCUMENT_REL_DETAIL);

                return [
                    t.PUNTO_EMISION_DOC,
                    t.SECUENCIA_DOC,
                    t.SECUENCIA_REL_DOC,
                    t.CLAVE_TRANS,
                    t.CLAVE_REL_TRANS,
                    t.TIP_DET_DOC,
                    t.FEC_PERIODO_TRAC,
                    t.FEC_MES_TRAC,
                    t.FEC_TRAC,
                    t.FEC_REL_TRAC,
                    t.FEC_MERC_TRAC,
                    t.MET_PAG_TRAC,
                    t.OBS_TRAC,
                    creador,
                    vendedor,
                    cliente,
                    t.ESTADO,
                    t.ESTADO_REL,
                    t.RUTA_DOC_ANULADO,
                    t.SUB_BASE_5,
                    t.SUB_BASE_8,
                    t.SUB_BASE_12,
                    t.SUB_BASE_13,
                    t.SUB_BASE_14,
                    t.SUB_BASE_15,
                    t.SUB_12_TRAC,
                    t.SUB_0_TRAC,
                    t.SUB_N_OBJETO_TRAC,
                    t.SUB_EXENTO_TRAC,
                    t.SUB_TRAC,
                    t.IVA_TRAC,
                    t.TOT_TRAC,
                    t.TOT_RET_TRAC,
                    t.TOT_PAG_TRAC,
                    t.PROPINA_TRAC ?? 0,
                    t.OTRA_PER,
                    t.COD_COMPROBANTE,
                    t.COD_COMPROBANTE_REL,
                    t.COD_DOCSUS_TRIB,
                    newCompanyId,
                    t.FRIMADO,
                    t.ENVIADO,
                    t.AUTORIZADO,
                    t.ENVIADO_CLIEMAIL,
                    t.FECHA_AUTORIZACION,
                    t.FECHA_AUTORIZACION_REL,
                    t.TIP_DOC_REL,
                    t.DSTO_TRAC,
                    idSucursal,
                    auditId,
                    t.TIP_TRAC,
                    t.FECHA_REG,
                    JSON.stringify(jsonAdaptado),
                    t.PUNTO_EMISION_REC,
                    t.FECHA_ANULACION,
                    t.TIP_DOC,
                    t.SRI_PAY_CODE,
                    t.CLIENT_IP ?? '0.0.0.0',
                    t.FK_AUDIT_REL,
                    t.NUM_TRANS,
                    t.NUM_REL_DOC,
                    t.DIV_PAY_YEAR,
                    JSON.stringify(retencionVentaNueva),
                    safeJson(t.RESP_SRI),
                    t.INFO_ADIC,
                    t.DET_EXP_REEMBOLSO,
                    safeJson(t.JSON_METODO),
                    t.ITEMS_PROF,
                    t.OBS_AUXILIAR,
                    t.OBS_ORDEN
                ];
            });

            // Insertar clientes en batch
            const [res]: any = await conn.query(`
                INSERT INTO transactions (PUNTO_EMISION_DOC,
                    SECUENCIA_DOC,
                    SECUENCIA_REL_DOC,
                    CLAVE_TRANS,
                    CLAVE_REL_TRANS,
                    TIP_DET_DOC,
                    FEC_PERIODO_TRAC,
                    FEC_MES_TRAC,
                    FEC_TRAC,
                    FEC_REL_TRAC,
                    FEC_MERC_TRAC,
                    MET_PAG_TRAC,
                    OBS_TRAC,
                    FK_USER,
                    FK_USER_VEND,
                    FK_PERSON,
                    ESTADO,
                    ESTADO_REL,
                    RUTA_DOC_ANULADO,
                    SUB_BASE_5,
                    SUB_BASE_8,
                    SUB_BASE_12,
                    SUB_BASE_13,
                    SUB_BASE_14,
                    SUB_BASE_15,
                    SUB_12_TRAC,
                    SUB_0_TRAC,
                    SUB_N_OBJETO_TRAC,
                    SUB_EXENTO_TRAC,
                    SUB_TRAC,
                    IVA_TRAC,
                    TOT_TRAC,
                    TOT_RET_TRAC,
                    TOT_PAG_TRAC,
                    PROPINA_TRAC,
                    OTRA_PER,
                    COD_COMPROBANTE,
                    COD_COMPROBANTE_REL,
                    COD_DOCSUS_TRIB,
                    FK_COD_EMP,
                    FRIMADO,
                    ENVIADO,
                    AUTORIZADO,
                    ENVIADO_CLIEMAIL,
                    FECHA_AUTORIZACION,
                    FECHA_AUTORIZACION_REL,
                    TIP_DOC_REL,
                    DSTO_TRAC,
                    FK_CODSUCURSAL,
                    FK_AUDITTR,
                    TIP_TRAC,
                    FECHA_REG,
                    DOCUMENT_DETAIL,
                    PUNTO_EMISION_REC,
                    FECHA_ANULACION,
                    TIP_DOC,
                    SRI_PAY_CODE,
                    CLIENT_IP,
                    FK_AUDIT_REL,
                    NUM_TRANS,
                    NUM_REL_DOC,
                    DIV_PAY_YEAR,
                    DOCUMENT_REL_DETAIL,
                    RESP_SRI,
                    INFO_ADIC,
                    DET_EXP_REEMBOLSO,
                    JSON_METODO,
                    ITEMS_PROF,
                    OBS_AUXILIAR,
                    OBS_ORDEN
                ) VALUES ?
            `, [values]);
            // Mapear ventas
            let newId = res.insertId;
            for (const s of batch) {
                mapSales[s.COD_TRANS] = newId++;
            }
            console.log(` -> Batch migrado: ${batch.length} ventas`);
        } catch (err) {

            throw err;
        }
    }

    return { mapSales, mapAuditSales };
}
