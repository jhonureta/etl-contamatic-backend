export async function migratePreconfiguredAccounts(
    legacyConn: any,
    conn: any,
    newCompanyId: number, mapAccounts: any
): Promise<Record<number, number>> {
    console.log("Migrando preconfigurado...");

    // 1. Obtener configuración de cierre
    const queryClosure = `SELECT * FROM contabilidad_cierre_configuracion;`;
    const [businessClosure] = await legacyConn.query(queryClosure);

    if (!businessClosure.length) {
        console.log("Cuentas de cierre no encontradas");
        return {};
    } 

    // 2. Obtener plan de cuentas
    const accountPlan = await findParametersAccounting(newCompanyId, conn);
    
    if (!accountPlan.length) {
        console.log("Plan de cuentas vacio en esta empresa");
        return {};
    }

    // 3. Mapear los nombres para evitar bucles anidados
    const closureMap = {
        'RESULTADOS INTEGRALES ACUMULADOS (CUENTA PATRIMONIAL CODIGO 3)': 'Resultados Integrales Acumulados - Cuenta Patrimonial (Código 3)',
        'CUENTA DE RESULTADOS INTEGRALES (CUENTA DE PERDIDA O GANANCIA CODIGO 5)': 'Cuenta de Resultados Integrales - Pérdidas o Ganancias (Código 5)',
        'RESULTADOS INTEGRAL DEL EJERCICIO (CUENTA DE PERDIDA O GANANCIA DEL EJERCICIO ACTUAL CUENTA PATRIMONIAL 3)': 'Resultados Integrales Acumulados - Pérdidas o Ganancias del Ejercicio (Cuenta Patrimonial Código 3)',
    };

    // 4. Procesar cuentas de manera más eficiente
    for (const closure of businessClosure) {
        const matchingAccount = accountPlan.find(acc => acc.NOMBRE === closureMap[closure.RES_CONTCICONF]);
        if (!matchingAccount) continue;

        const codIdPlan = mapAccounts[closure.CUENT_CONTCICONF]??null;
        const dataInsert = await findParametersInsertAccounting({
            newCompanyId,
            parameterId: matchingAccount.COD_PARAMETRO,
            codIdPlan,
        }, conn);
        if (!dataInsert.affectedRows) {
            console.log(`Error al migrar cuenta de resultados: ${closure.RES_CONTCICONF}`);
            return {};

        }
        console.log(`Cuenta de resultados ${closure.RES_CONTCICONF} - Cuenta: ${codIdPlan}`);
    }

    const queryPreconfiguradoBussinnes = `
                                                    SELECT 
                                                        contabilidad_asiento_preconfigurado.DET_CONFIG,
                                                        tipo_asiento.TIP_ASI_NOM,
                                                        tipo_asiento.TIP_ASI_DET,
                                                        tipo_asiento.CODCONT
                                                    FROM tipo_asiento
                                                    INNER JOIN contabilidad_asiento_preconfigurado 
                                                        ON contabilidad_asiento_preconfigurado.FK_ID_TIP_ASI = tipo_asiento.ID_TIP_ASI;
                                                `;
    const [businesPreConf] = await legacyConn.query(queryPreconfiguradoBussinnes);
    if (!businesPreConf.length) {
        console.log(`Preconfigurado no encontrado`);
        return {};
    }
    // Configuración centralizada: cada CODCONT y sus detalles con los parámetros asociados
    const configMap = {
        CTAR: { 'TARJETA': [38] },
        RETASUMIDA: { 'Retención asumida': [2, 3] },
        CT: { 'Cuenta transitoria': [1] },
        VEN: {
            'VENTAS DIFERENTES 0%': [20],
            'VENTAS 0%': [21, 22, 23],
            'IMPUESTOS': [32],
        },
        IVAC0: {
            'INVENTARIO DIFERENTE 0%': [12],
            'INVENTARIO 0%': [14, 16, 18],
            'IMPUESTO': [33],
        },
        CVENTA: {
            'Costo de ventas diferente de 0': [13],
            'Costo de ventas 0': [15, 17, 19],
        },
        ANTCN: { 'ANTICIPOS NACIONALES': [8] },
        ANTCE: { 'ANTICIPO EXTERIOR': [9] },
        ANTPN: { 'Anticipo de Proveedores nacionales': [10] },
        ANTPE: { 'Anticipo de Proveedores': [11] },
        CXCN: { 'Clientes nacionales': [4, 41] },
        CXCE: { 'Clientes exterior': [5] },
        CXPN: { 'CxP Proveedores Nacionales': [6, 42] },
        CXPE: { 'Proveedores exterior': [7] },
        NDC: {
            'VENTA DIFERENTE 0%': [24],
            'VENTA 0%': [25, 26, 27],
        },
        NDC1: {
            'VENTA DIFERENTE 0%': [28],
            'VENTA 0%': [29, 30, 31],
        },
        NDC2: {
            'COMPRA DIFERENTE 0%': [44],
            'COMPRA 0%': [45, 46, 47],
        },
        NDC3: {
            'COMPRA DIFERENTE 0%': [48],
            'COMPRA 0%': [49, 50, 51],
        },
        INVENT: {
            'PATRIMONIO CON IVA': [37],
            'PATRIMONIO SIN IVA': [54],
        },
        IVAC: {
            'IMPUESTO': [52],
        },
        //
    };

    // 🔁 Función genérica para insertar cuentas
    const processCuenta = async ({ detalle, codigo, parameterIds, newCompanyId }, conn) => {
        const cuentaData = await consultarCuentaCodigoContable(codigo, newCompanyId, conn);
        if (!cuentaData.length) {
            console.log(`Cuenta contable no encontrada: ${codigo}`);
            return {};
        }
        const codIdPlan = cuentaData[0].ID_PLAN;
        for (const parameterId of parameterIds) {
            const dataInsert = await findParametersInsertAccounting({
                newCompanyId,
                parameterId,
                codIdPlan,
            }, conn);
            if (!dataInsert.affectedRows) {
                console.log(`Error al migrar cuenta de resultados: ${detalle}`);
                return {};
            }
            //console.log(`✅ Cuenta de resultados ${detalle} - Cuenta: ${codIdPlan}`);
        }
    };

    for (const configData of businesPreConf) {
        const { CODCONT, DET_CONFIG } = configData;
        const jsonConfig = JSON.parse(DET_CONFIG);

        // Si el código no está configurado, se ignora
        const detalles = configMap[CODCONT];
        if (!detalles) continue;

        for (const json of jsonConfig) {
            const parameterIds = detalles[json.detalle];
            if (parameterIds) {
                await processCuenta({
                    detalle: json.detalle,
                    codigo: json.codigo,
                    parameterIds,
                    newCompanyId,
                }, conn);
            }
        }
    }

    return { 1: 1 };
}

async function findParametersAccounting(newCompanyId, conn) {
    try {
        const query = `SELECT * 
                            FROM accounting_parameters
                            LEFT JOIN company_acct_params 
                            ON company_acct_params.COD_PARAMETROID = accounting_parameters.COD_PARAMETRO  
                            AND company_acct_params.FK_COD_EMP =?
                            LEFT JOIN (
                            SELECT * 
                            FROM account_plan  
                            WHERE account_plan.FK_COD_EMP = ?
                            ) as tbPlan 
                            ON company_acct_params.FK_CODIGOPLAN = tbPlan.ID_PLAN 
                            WHERE 1;`;
        const [company] = await conn.query(query, [newCompanyId, newCompanyId]);
        return company;

    } catch (error) {
        console.log(` -> Error generado en la exepcion`);
        return {};
    }
}

async function findParametersInsertAccounting(parameter, conn) {
    try {
        const query = `SELECT * FROM company_acct_params WHERE FK_COD_EMP =? and COD_PARAMETROID =? ;`;
        const [company] = await conn.query(query, [parameter.newCompanyId, parameter.parameterId]);
        if (company.length > 0) {
            const detParamId = company[0].ACCT_PARAMCOD;
            const query = `UPDATE company_acct_params SET FK_CODIGOPLAN=? WHERE ACCT_PARAMCOD=? AND COD_PARAMETROID=? AND FK_COD_EMP=?;`;
            const [param] = await conn.query(query, [parameter.codIdPlan, detParamId, parameter.parameterId, parameter.newCompanyId]);
            return param;
        } else {
            const query = `INSERT INTO company_acct_params (COD_PARAMETROID, FK_COD_EMP, FK_CODIGOPLAN ) VALUES (?,?,?);`;
            const [param] = await conn.query(query, [parameter.parameterId, parameter.newCompanyId, parameter.codIdPlan]);
            return param;
        }
    } catch (error) {
        console.log(` -> Error en actualizar informacion de los parametros contables`);
        return {};
    }
}

async function consultarCuentaCodigoContable(codigoPlan, companyId, conn) {

    try {
        const queryUsuser = "SELECT *FROM account_plan  WHERE account_plan.CODIGO_PLAN=? AND account_plan.FK_COD_EMP =? limit 1";
        const [account_plan] = await conn.query(queryUsuser, [codigoPlan, companyId]);

        return account_plan;

    } catch (error) {
        console.log(` -> Error en actualizar informacion de los parametros contables Process`);
        return {};
    }
}