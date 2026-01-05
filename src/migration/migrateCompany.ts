import { systemworkPool, erpPool, createLegacyConnection } from '../config/db';
import CryptoService from './encrypt.handler';
import { migrateAccountingPeriod } from './migrateAccountingPeriod';
import { migrateBancos } from './migrateBancos';
import { migrateBranchesForCompany } from './migrateBranch';
import { migrateBrand } from './migrateBrand';
import { migrateCajas } from './migrateCajas';
import { migrateCategories } from './migrateCategories';
import { migrateChartAccounts } from './migrateChartAccounts';
import { migrateClientsForCompany } from './migrateClients';
import { migrateCostCenter } from './migrateCostCenter';
import { migrateExpensesDetails } from './migrateExpensesDetails';
import { migrateMeasures } from './migrateMeasures';
import { migratePreconfiguredAccounts } from './migratePreconfiguredAccounts';
import { batchInsertProducts } from './migrateProducts';
import { migrateProjects } from './migrateProjects';
import { migrateRetentions } from './migrateRetentionDetails';
import { migrateSales } from './migrateSales';
import { migrateSuppliersForCompany } from './migrateSuppliers';
import { migrateUsersForCompany } from './migrateUser';
import { migrateWarehouseDetails } from './migrateWarehouseDetails';
import { migrateBankReconciliation } from './migrateBankReconciliation';
import { migrateMovementDetail0bligations } from './migrateMovementDetail0bligations';
import { migrateCustomerAccounting } from './migrateCustomerObligations';
import { migrateProformaInvoices } from './migrateProformaInvoices';
export async function migrateCompany(codEmp: number) {
  const [rows] = await systemworkPool.query(
    `SELECT * FROM empresas WHERE COD_EMPSYS = ?`,
    [codEmp],
  );

  const empresas = rows as any[];

  if (!empresas.length) {
    throw new Error(`No se encontró empresa con COD_EMPSYS=${codEmp}`);
  }

  const empresa = empresas[0];

  console.log(
    `Migrando empresa: ${empresa.NOM_EMPSYS} (BD: ${empresa.BASE_EMP})`,
  );

  const legacyConn = await createLegacyConnection({
    user: empresa.USER_BASE_EMP,
    password: empresa.PASS_BASE_EMP,
    database: empresa.BASE_EMP,
  });


  const conn = await erpPool.getConnection();
  try {
    const [rowsEmpresaLegacy] = await legacyConn.query(
      `SELECT *, 1 AS FK_COD_COUNT,'ACTIVO' AS EST_EMP, 'CORPNEWBEST' as CONF_FIRM_PROV, NOMFIRELEC_EMP as CONF_FIRM, CLAVFIRELEC_EMP as CONF_FIRM_PASS,  FECEMISION_FIR AS CONF_FIRM_EMI, FECVENCE_FIR AS CONF_FIRM_EXP,TIPAMB_EMP AS CONF_TIPAMB,
            EMA_EMP AS CONF_EMAIL_DEFAULT, 1 AS CONF_DESC_VENTA, INVE_EMP as CONF_STOCK  , INVEN_DETALLADO as CONF_INVDET,SELECCION_COSTEO as CONF_COST, DESCUENTO_AUTOMATICO as CONF_DESCAUTO, eliminar_transaccion AS CONF_DELDOCS, pedir_clave as CONF_CLAVE, SELECCION_VENDEDOR as CONF_SELECT_VEND,SELECCION_FECHA as CONF_SELECT_FECHA,
            PBX_EMP as CONF_PRECIVA, IMP_SURC_EMP as CONF_INFOSUC,IMP_LOGO  AS CONF_LOGOTICK, PARAM_EMP , '#000000' AS CONF_COLOR_FORMT, 0 as CONF_COLOR_FOND , SEC_EXTR_EMP AS CONF_SECEXT, LEGEND_EMP AS CONF_LEYEND,  TIT_LEGEND_EMP AS CONF_TIT_LEYEND ,
            LEGEND_EMP as CONF_CONT_LEYEND,'#000000' as COLOR_LEYENDA, '#1b6898' as CONF_COLOR_LETR, DATOS_EXTR_EMP ,'[]' as CONF_ITEMS_ADIC, 0 AS CONF_TICKET_AUTOM, 2 AS CONF_BARCODE_NUM, 0 AS CONF_BARCODE_COLOR,  CIERRE_CAJA as CLOSING_BOX, 1 as CONF_TYPE_POINTSALES,NULL AS FECHA_CREACION FROM empresa limit 1`,
    );
    const e = (rowsEmpresaLegacy as any[])[0];

    if (!e) {
      throw new Error(
        `No se encontró registro en tabla 'empresa' de BD ${empresa.BASE_EMP}`,
      );
    }
    const vacio = 'NULL';
    const resultInsert = [
      e.CODGEN_EMP,
      e.RAZSOC_EMP,
      e.RUC_EMP,
      e.NOMCOM_EMP,
      e.NOMFIN_EMP,
      e.CIUD_EMP,
      e.DIR_EMP,
      e.EMA_EMP,
      e.TEL_EMP,
      e.TEL2_EMP,
      e.TEL3_EMP,
      e.CODART_EMP,
      e.CONT_EMP,
      e.CONTESP_EMP,
      e.LOGO_EMP,
      e.FK_COD_USU,
      e.IDENREPLEG_EMP,
      e.NOMREPLEG_EMP,
      e.IDENCONT_EMP,
      e.NOMCONT_EMP,
      e.TIPAMB_EMP == 'prueba' ? 'pruebas'.toLowerCase() : 'produccion'.toLowerCase(),
      e.CODNUMACCES_EMP,
      e.secuencial,
      e.estado_secuencial,
      vacio,
      e.INVE_EMP,
      e.regimen,
      e.resolucion,
      e.eliminar_transaccion,
      e.pedir_clave,
      vacio,
      e.DETTICKET_EMP,
      e.JSON_IMPUESTO,
      e.INVEN_DETALLADO,
      e.SELECCION_VENDEDOR,
      e.SELECCION_FECHA,
      e.SELECCION_COSTEO,
      e.EDIT_VENDEDOR,
      e.DESCUENTO_AUTOMATICO,
      e.TIPEMP_EMP == 'prueba' ? 'pruebas' : 'produccion',
      '[]',
      e.LEGEND_EMP,
      e.TIT_LEGEND_EMP,
      e.ITEMS_AUT,
      e.CLAVRUC_EMP,
      e.SEC_EXTR_EMP,
      '[]',
      e.ACT_DESCRIPCION,
      e.IMP_LOGO,
      e.IMP_SURC_EMP,
      e.COLOR_LEYENDA,
      e.FK_COD_COUNT,
      e.FECHA_CREACION,
      e.EST_EMP.toUpperCase() ?? 'ACTIVO',
      e.CONTADOR ?? 1
    ]

    await conn.beginTransaction();

    const [resCompany]: any = await conn.query(
      `INSERT INTO companies (CODGEN_EMP, RAZSOC_EMP,
     RUC_EMP, NOMCOM_EMP, NOMFIN_EMP, CIUD_EMP, DIR_EMP, EMA_EMP,
     TEL_EMP, TEL2_EMP, TEL3_EMP, CODART_EMP, CONT_EMP, CONTESP_EMP, 
     LOGO_EMP, FK_COD_USU, IDENREPLEG_EMP, NOMREPLEG_EMP, IDENCONT_EMP, 
     NOMCONT_EMP, TIPAMB_EMP, CODNUMACCES_EMP, secuencial, 
     estado_secuencial, SUB_EMP, INVE_EMP, regimen, resolucion, 
     eliminar_transaccion, pedir_clave, PARAM_EMP, DETTICKET_EMP, 
     JSON_IMPUESTO, INVEN_DETALLADO, SELECCION_VENDEDOR, SELECCION_FECHA, 
     SELECCION_COSTEO, EDIT_VENDEDOR, DESCUENTO_AUTOMATICO, TIPEMP_EMP, 
     JSON_ESTAB, LEGEND_EMP, TIT_LEGEND_EMP, ITEMS_AUT, CLAVRUC_EMP, 
     SEC_EXTR_EMP, DATOS_EXTR_EMP, ACT_DESCRIPCION, IMP_LOGO, IMP_SURC_EMP, COLOR_LEYENDA, 
     FK_COD_COUNT, FECCREACION_EMP, EST_EMP, CONTADOR)
    VALUES (?, ?, ?, ?, ?, ?, ?,
       ?, ?, ?, ?, ?, ?, ?, 
       ?, ?, ?, ?, ?, ?, ?, 
       ?, ?, ?, ?, ?, ?, ?, 
       ?, ?, ?, ?, ?, ?, ?, 
       ?, ?, ?, ?, ?, ?, ?, 
       ?, ?, ?, ?, ?, ?, ?, 
       ?, ?, ?, ?, ?, ?
    )
    `,
      resultInsert
    );


    const newCompanyId: number = resCompany.insertId;
    console.log(`-> Empresa migrada con ID = ${newCompanyId}`);

    console.log(`-> Migrando configuracion de empresa`);

    /*   const datosExtras = JSON.parse(e.DATOS_EXTR_EMP) || [];
      const requiredExtras = ['RESPONSABLE', 'BODEGA', 'VENDEDOR'];
      const CONF_DATOS_RESP = datosExtras.some(d => d.toUpperCase() === 'RESPONSABLE') ? 1 : 0;
      const CONF_DATOS_BOD = datosExtras.some(d => d.toUpperCase() === 'BODEGA') ? 1 : 0;
      const CONF_DATOS_VEND = datosExtras.some(d => d.toUpperCase() === 'VENDEDOR') ? 1 : 0;
      const faltantesExtras = requiredExtras.filter(k => !datosExtras.map(d => d.toUpperCase()).includes(k));
      if (faltantesExtras.length > 0) {
        throw new Error(
          `Faltan datos extra obligatorios: ${faltantesExtras.join(', ')}`,
        );
      }
   */

    // 1. Intentar parsear de forma segura para que no rompa si e.DATOS_EXTR_EMP es null o mal formado
    let datosExtras = [];
    try {
      datosExtras = e.DATOS_EXTR_EMP ? JSON.parse(e.DATOS_EXTR_EMP) : [];
      if (!Array.isArray(datosExtras)) datosExtras = []; // Asegurar que sea array
    } catch (error) {
      datosExtras = []; // Si el JSON está mal, por defecto dejamos array vacío
    }

    // 2. Normalizamos a mayúsculas una sola vez para mejorar el rendimiento
    const datosUpper = datosExtras.map(d => String(d).toUpperCase());

    // 3. Asignación de 1 o 0 (si no existe, por defecto será 0)
    const CONF_DATOS_RESP = datosUpper.includes('RESPONSABLE') ? 1 : 0;
    const CONF_DATOS_BOD = datosUpper.includes('BODEGA') ? 1 : 0;
    const CONF_DATOS_VEND = datosUpper.includes('VENDEDOR') ? 1 : 0;

    // 4. Eliminamos el "throw new Error" y simplemente validamos si quieres registrar los faltantes
    const requiredExtras = ['RESPONSABLE', 'BODEGA', 'VENDEDOR'];
    const faltantesExtras = requiredExtras.filter(k => !datosUpper.includes(k));

    if (faltantesExtras.length > 0) {
      console.warn(`Aviso: Faltan datos extra (${faltantesExtras.join(', ')}). Se asignó 0 por defecto.`);
    }

    let formatProforma, numFormat;
    try {
      const format = JSON.parse(e.PARAM_EMP) || [];
      if (!Array.isArray(format) || format.length === 0) throw new Error('PARAM_EMP debe ser array no vacío');
      formatProforma = format[0];
      numFormat = formatProforma.tipo_formato_orden;
    } catch (err) {

      throw new Error(
        `PARAM_EMP inválido`,
      );
    }

    let CONF_DATOS_SUCUR = '[]';
    try {
      CONF_DATOS_SUCUR = JSON.parse(e.JSON_ESTAB) || '[]';
    } catch (err) {

      throw new Error(
        `JSON_ESTAB inválido`,
      );

    }

    const cryptoService = new CryptoService()
    const claveFirma = cryptoService.encrypt(e.CONF_FIRM_PASS);
    const resultInsertConfig = [
      newCompanyId,
      e.CONF_FIRM,
      e.CONF_FIRM_PROV,
      claveFirma,
      e.CONF_FIRM_EMI,
      e.CONF_FIRM_EXP,
      e.CONF_TIPAMB.toLowerCase(),
      e.CONF_EMAIL_DEFAULT,
      e.CONF_DESC_VENTA,
      e.EDIT_VENDEDOR == 'NO' ? 0 : 1,
      e.CONF_STOCK,
      e.CONF_INVDET,
      e.CONF_COST,
      e.CONF_DESCAUTO == 'SI' ? 1 : 0,
      e.CONF_DELDOCS == 'si' ? 1 : 0,
      e.CONF_CLAVE,
      e.CONF_SELECT_VEND,
      e.CONF_SELECT_FECHA,
      e.CONF_PRECIVA == 'si' ? 1 : 0,
      e.CONF_INFOSUC == '1' ? 1 : 0,
      e.CONF_LOGOTICK,
      numFormat,
      e.CONF_COLOR_FORMT,
      e.CONF_COLOR_FOND,
      e.CONF_SECEXT,
      e.CONF_LEYEND != '' ? 1 : 0,
      e.CONF_TIT_LEYEND,
      e.CONF_CONT_LEYEND,
      e.CONF_COLOR_LETR,
      CONF_DATOS_RESP,
      CONF_DATOS_BOD,
      CONF_DATOS_VEND,
      CONF_DATOS_SUCUR,
      e.CONF_ITEMS_ADIC,
      e.CONF_TICKET_AUTOM,
      e.CONF_BARCODE_NUM,
      e.CONF_BARCODE_COLOR,
      e.CLOSING_BOX,
      e.CONF_TYPE_POINTSALES
    ]
    const [company_config]: any = await conn.query(
      `INSERT INTO company_configuration ( COD_EMP, CONF_FIRM, CONF_FIRM_PROV, CONF_FIRM_PASS, 
            CONF_FIRM_EMI, CONF_FIRM_EXP, CONF_TIPAMB,  CONF_EMAIL_DEFAULT, CONF_DESC_VENTA, 
            CONF_EDIT_VENDEDOR, CONF_STOCK, CONF_INVDET, CONF_COST, CONF_DESCAUTO, 
            CONF_DELDOCS, CONF_CLAVE, CONF_SELECT_VEND, CONF_SELECT_FECHA, CONF_PRECIVA, 
            CONF_INFOSUC, CONF_LOGOTICK, CONF_FORMAT_PROF, CONF_COLOR_FORMT, CONF_COLOR_FOND, 
            CONF_SECEXT, CONF_LEYEND, CONF_TIT_LEYEND,CONF_CONT_LEYEND, CONF_COLOR_LETR, 
            CONF_DATOS_RESP, CONF_DATOS_BOD, CONF_DATOS_VEND, CONF_DATOS_SUCUR, CONF_ITEMS_ADIC, 
            CONF_TICKET_AUTOM, CONF_BARCODE_NUM, CONF_BARCODE_COLOR, CLOSING_BOX, CONF_TYPE_POINTSALES) 
            VALUES (?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?, ?, ?, ?, 
                    ?, ?, ?, ?);
    `,
      resultInsertConfig
    );

    if (company_config.affectedRows == 0) {
      throw new Error(`Error al migrar la configuración de la empresa=${codEmp}`);
    }

    console.log(` ->  Configuración de empresa migrado correctamente`);

    const [rowsplans] = await systemworkPool.query(
      `SELECT  FK_COD_EMP as FK_COD_EMP,CONTADOR as DOC_UTI, DOC_TOTAL as DOC_TOTAL, 
      FEC_INICIO as FEC_INICIO,ADDDATE(FEC_INICIO, INTERVAL 1 YEAR)  as FEC_FIN, 
      FEC_VENCE,RENOVACION, FK_COD_PLAN as PLAN_ID, FEC_REG as FEC_REG,ESTADO as EST_PLAN, 
      FEC_MOD as FEC_MOD  FROM detalle_plan WHERE FK_COD_EMP =?;`,
      [codEmp],
    );

    const plans = rowsplans as any[];

    if (!plans.length) {
      throw new Error(`No se encontró plan de la empresa con COD_EMPSYS=${codEmp}`);
    }

    const p = plans[0];
    const planParameter = [
      newCompanyId,
      p.DOC_UTI,
      p.DOC_TOTAL,
      p.FEC_INICIO,
      p.FEC_FIN,
      p.FEC_VENCE,
      p.RENOVACION || 0,
      p.PLAN_ID,
      p.FEC_REG,
      p.EST_PLAN.toUpperCase(),
      p.FEC_MOD
    ]

    //MIGRAR PLAN DE EMPRESA
    console.log(` -> Migrar plan de empresa`);
    const [companyPlan]: any = await conn.query(`INSERT INTO detalle_plan (FK_COD_EMP,DOC_UTI,DOC_TOTAL,
    FEC_INICIO,FEC_FIN,FEC_VENC,RENOVACION,
    PLAN_ID,FEC_REG,EST_PLAN,FEC_MOD ) 
    VALUES (?,?,?,?,
            ?,?,?,?,
            ?,?,?);`,
      planParameter);

    if (companyPlan.affectedRows == 0) {
      throw new Error(`Error al migrar plan de empresa =${codEmp}`);
    }
    console.log(` -> Plan de la empresa migrado correctamente`);
    const branchMap = await migrateBranchesForCompany(
      legacyConn,
      conn,
      newCompanyId
    );

    const dataBaseIds = {
      branchMap,
    }

    const userMap = await migrateUsersForCompany(
      legacyConn,
      conn,
      newCompanyId,
      dataBaseIds
    );

    const mapClients = await migrateClientsForCompany(
      legacyConn,
      conn,
      newCompanyId
    );

    const mapSuppliers = await migrateSuppliersForCompany(
      legacyConn,
      conn,
      newCompanyId
    );

    const mapCenterCost = await migrateCostCenter(
      legacyConn,
      conn,
      newCompanyId
    );


    const mapProject = await migrateProjects(
      legacyConn,
      conn,
      newCompanyId
    );


    const mapPeriodo = await migrateAccountingPeriod(
      legacyConn,
      conn,
      newCompanyId
    );

    const mapAccounts = await migrateChartAccounts(
      legacyConn,
      conn,
      newCompanyId
    );

    const mapParamameters = await migratePreconfiguredAccounts(
      legacyConn,
      conn,
      newCompanyId,
      mapAccounts
    );

    const mapCostExpenses = await migrateExpensesDetails(
      legacyConn,
      conn,
      newCompanyId,
      mapAccounts
    );

    const mapRetentions = await migrateRetentions(
      legacyConn,
      conn,
      newCompanyId,
      mapAccounts
    );

    const bankMap = await migrateBancos(
      legacyConn,
      conn,
      newCompanyId,
      mapAccounts
    );

    const boxMap = await migrateCajas(
      legacyConn,
      conn,
      newCompanyId,
      mapAccounts,
      userMap
    );
    const mapCategories = await migrateCategories(
      legacyConn,
      conn,
      newCompanyId,
    );

    const mapMeasures = await migrateMeasures(
      legacyConn,
      conn,
      newCompanyId
    );


    const mapBrand = await migrateBrand(
      legacyConn,
      conn,
      newCompanyId
    );


    const mapProducts = await batchInsertProducts(
      legacyConn,
      conn,
      newCompanyId,
      mapAccounts,
      mapCategories,
      mapMeasures,
      mapBrand
    );

    const mapDetWare = await migrateWarehouseDetails(
      legacyConn,
      conn,
      branchMap,
      mapProducts
    );

    const mapsSales = await migrateSales(
      legacyConn,
      conn,
      newCompanyId, branchMap, userMap, mapClients, mapProducts, mapRetentions);

    //MIGRACION DE CONCILIACION BANCARIA

    const mapConciliation = await migrateBankReconciliation(
      legacyConn,
      conn,
      newCompanyId,
      bankMap
    );


    /* MIGRARCION MOVIMIENTOS DE VENTAS   */
    const mapObligationsCustomers = await migrateCustomerAccounting(
      legacyConn,
      conn,
      newCompanyId,
      mapsSales.mapSales,
      mapsSales.mapAuditSales,
      mapClients,
      bankMap,
      boxMap,
      userMap,
      mapPeriodo,
      mapProject,
      mapCenterCost,
      mapAccounts,
      mapConciliation
    )

    const map = await migrateMovementDetail0bligations(
      legacyConn,
      conn,
      newCompanyId,
      mapsSales.mapSales,
      bankMap,
      boxMap,
      userMap,
      mapConciliation,
      mapObligationsCustomers.mapObligationsCustomers,
      mapPeriodo,
      mapProject,
      mapCenterCost,
      mapAccounts
    )

    await migrateProformaInvoices({
      legacyConn,
      conn,
      newCompanyId,
      userMap,
      mapClients,
      mapProducts,
      branchMap
    })

    /*   const [rows] = await conn.query(`SELECT *FROM products WHERE FK_COD_EMP=${newCompanyId}`);
      const accounts = rows as any[]; console.log(rows);
      if (!accounts.length) {
        console.log(" -> No hay plan de cuentas para migrar.");
        return {};
      } */

    //console.log(mapAccounts);

    /* const [rows] = await conn.query(`SELECT *FROM account_plan WHERE FK_COD_EMP=${newCompanyId}`);
    const accounts = rows as any[]; console.log(rows);
    if (!accounts.length) {
      console.log(" -> No hay plan de cuentas para migrar.");
      return {};
    }
 */
    await conn.rollback();
    console.log("MAPEO DE SUCURSALES MIGRADAS:", Object.keys(branchMap).length);
    console.log("MAPEO DE PROYECTOS MIGRADOS:", Object.keys(mapProject).length);
    console.log("MAPEO DE CENTRO DE COSTOS MIGRADOS:", Object.keys(mapCenterCost).length);
    console.log("MAPEO DE USUARIOS MIGRADAS:", Object.keys(userMap).length);
    console.log("MAPEO DE CLIENTES MIGRADAS:", Object.keys(mapClients).length);
    console.log("MAPEO DE PROVEEDORES MIGRADAS:", Object.keys(mapSuppliers).length);
    console.log("MAPEO DE PERIODOS CONTABLES MIGRADAS:", Object.keys(mapPeriodo).length);
    console.log("MAPEO PLAN DE CUENTAS:", Object.keys(mapAccounts).length);
    console.log("MAPEO COSTOS Y GASTOS:", Object.keys(mapCostExpenses).length);
    console.log("MAPEO RETENCIONES:", Object.keys(mapRetentions).length);
    console.log("MAPEO BANCOS MIGRADOS:", Object.keys(bankMap).length);
    console.log("MAPEO CAJAS MIGRADAS:", Object.keys(boxMap).length);
    console.log("MAPEO CATEGORIAS MIGRADOS:", Object.keys(mapCategories).length);
    console.log("MAPEO MEDIDAS MIGRADAS:", Object.keys(mapMeasures).length);
    console.log("MAPEO MARCAS MIGRADAS:", Object.keys(mapBrand).length);
    console.log("MAPEO PRODUCTOS MIGRADOS:", Object.keys(mapProducts).length);
    console.log("DETALLE DE BODEGA MIGRADOS:", Object.keys(mapDetWare).length);
    console.log("VENTAS MIGRADAS:", Object.keys(mapsSales.mapSales).length);
    console.log("CONCILIACION MIGRADA :", Object.keys(mapConciliation).length);
    console.log("AUDITORIA DE VENTAS MIGRADAS:", Object.keys(mapsSales.mapAuditSales).length);
    console.log("OBLIGACIONES MIGRADAS:", Object.keys(mapObligationsCustomers.mapObligationsCustomers).length);
    console.log("OBLIGACIONES AUDITORIA:", Object.keys(mapObligationsCustomers.mapObligationsAudit).length);




    return newCompanyId;
  } catch (error) {
    console.error(error);
    await conn.rollback();
    throw new Error(error.message);
  } finally {
    conn.release();
    await legacyConn.end();
  }
}
