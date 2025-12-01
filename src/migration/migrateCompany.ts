import { systemworkPool, erpPool, createLegacyConnection } from '../config/db';
import { migrateBranchesForCompany } from './migrateBranch';
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

  try {
    const [rowsEmpresaLegacy] = await legacyConn.query(
      'SELECT * FROM empresa LIMIT 1',
    );
    const e = (rowsEmpresaLegacy as any[])[0];

    if (!e) {
      throw new Error(
        `No se encontró registro en tabla 'empresa' de BD ${empresa.BASE_EMP}`,
      );
    }

    const resultInsert =     [
        e.CODGEN_EMP,                   
        e.RAZSOC_EMP,                   
        e.RUC_EMP,                      
        e.NOMCOM_EMP,                   
        e.NOMFIN_EMP,                   
        e.CIUD_EMP,                     
        e.DIR_EMP,                      
        e.DIRMAT_EMP,                   
        e.FAX_EMP,                      
        e.PBX_EMP,              
        e.EMA_EMP,              
        e.TEL_EMP,              
        e.TEL2_EMP,             
        e.TEL3_EMP,             
        e.CODART_EMP,           
        e.CONT_EMP,             
        e.NUMEST_EMP,           
        e.NUMSUC_EMP,           
        e.PUNEMI_EMP,           
        e.CONTESP_EMP,          
        e.LOGO_EMP,             
        e.FK_COD_USU,           
        e.IDENREPLEG_EMP,       
        e.NOMREPLEG_EMP,        
        e.IDENCONT_EMP,         
        e.NOMCONT_EMP,          
        e.TIPAMB_EMP,           
        e.CODNUMACCES_EMP,      
        e.secuencial,           
        e.estado_secuencial,    
        e.SUB_EMP,              
        e.INVE_EMP,             
        e.regimen,              
        e.resolucion,           
        e.eliminar_transaccion, 
        e.pedir_clave,          
        e.PARAM_EMP,            
        e.DETTICKET_EMP,        
        e.JSON_IMPUESTO,        
        e.INVEN_DETALLADO,      
        e.SELECCION_VENDEDOR,   
        e.SELECCION_FECHA ?? "",      
        e.SELECCION_COSTEO,     
        e.EDIT_VENDEDOR,        
        e.DESCUENTO_AUTOMATICO, 
        e.TIPEMP_EMP,           
        e.JSON_ESTAB,           
        e.LEGEND_EMP,           
        e.TIT_LEGEND_EMP,       
        e.ITEMS_AUT,            
        e.CLAVRUC_EMP,          
        e.SEC_EXTR_EMP,         
        e.DATOS_EXTR_EMP,       
        e.ACT_DESCRIPCION,      
        e.IMP_LOGO,             
        e.IMP_SURC_EMP,         
        e.COLOR_LEYENDA,            
        null,
        '', 
        0,
        null,
        0, 
        'ACTIVO',
        null,
        null
      ]

    const [resCompany]: any = await erpPool.query(
    `
    INSERT INTO companies (
        CODGEN_EMP, RAZSOC_EMP, RUC_EMP, NOMCOM_EMP, NOMFIN_EMP, CIUD_EMP,
        DIR_EMP, DIRMAT_EMP, FAX_EMP, PBX_EMP, EMA_EMP, TEL_EMP, TEL2_EMP,
        TEL3_EMP, CODART_EMP, CONT_EMP, NUMEST_EMP, NUMSUC_EMP, PUNEMI_EMP,
        CONTESP_EMP, LOGO_EMP, FK_COD_USU, IDENREPLEG_EMP, NOMREPLEG_EMP,
        IDENCONT_EMP, NOMCONT_EMP, TIPAMB_EMP, CODNUMACCES_EMP, secuencial,
        estado_secuencial, SUB_EMP, INVE_EMP, regimen, resolucion,
        eliminar_transaccion, pedir_clave, PARAM_EMP, DETTICKET_EMP,
        JSON_IMPUESTO, INVEN_DETALLADO, SELECCION_VENDEDOR, SELECCION_FECHA,
        SELECCION_COSTEO, EDIT_VENDEDOR, DESCUENTO_AUTOMATICO, TIPEMP_EMP,
        JSON_ESTAB, LEGEND_EMP, TIT_LEGEND_EMP, ITEMS_AUT, CLAVRUC_EMP,
        SEC_EXTR_EMP, DATOS_EXTR_EMP, ACT_DESCRIPCION, IMP_LOGO, IMP_SURC_EMP,
        COLOR_LEYENDA, FK_COD_COUNT, WHATS_EMP, STATUS_WHATS_EMP, JSON_USER,
        STATUS_SOL, EST_EMP, MOTINAC_EMP, CONTADOR
    )
    VALUES (
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?, ?, ?, ?, 
        ?, ?, ?
    )
    `,
    resultInsert
    );


    const newCompanyId: number = resCompany.insertId;
    console.log(` -> Empresa migrada con ID = ${newCompanyId}`);
    
    const branchMap = await migrateBranchesForCompany(
    legacyConn,
    newCompanyId
    );

    console.log("MAPEO DE SUCURSALES MIGRADAS:", branchMap);

    return newCompanyId;
  } finally {
    await legacyConn.end();
  }
}
