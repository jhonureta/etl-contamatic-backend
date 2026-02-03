import { restructureCreditNoteDetail } from "./creditNoteHelpers";
import { upsertTotaledEntry } from "./migrationTools";
import { normalizarProducto } from "./normalizador";
import { findNextAuditCode, toJSONArray, toNumber } from "./purchaseHelpers";

interface BranchSequentialData {
  COD_SURC: number;
  ELECTRONICA: string;
  FISICA: string;
  COMPINGRESO: string;
}

export async function migrateCreditNotesPurchases({
  legacyConn,
  conn,
  newCompanyId,
  branchMap,
  userMap,
  mapSuppliers,
  mapProducts,
  oldProductCodeMap,
  purchaseLiquidationIdMap,
  purchaseLiquidationAuditIdMap,
  bankMap,
  boxMap,
  mapConciliation,
  purchaseLiquidationObligationIdMap,
  mapPeriodo,
  mapProject,
  mapCenterCost,
  mapAccounts,
  storeMap,
  idFirstBranch
}): Promise<{ creditNotesPurchasesIdMap: Record<number, number>; creditNotesPurchasesAuditIdMap: Record<number, number> }> {
  try {
    console.log("Migrando notas de credito de compras...");
    const creditNotesPurchasesIdMap: Record<number, number> = {};
    const creditNotesPurchasesAuditIdMap: Record<number, number> = {};
    const [rows] = await legacyConn.query(`
    SELECT
        tf.COD_TRAC AS COD_TRANS,
        CONCAT(
            SUBSTRING(tf.numeroNotaProve, 1, 3),
            '-',
            SUBSTRING(tf.numeroNotaProve, 4, 3)
        ) AS PUNTO_EMISION_DOC,
        SUBSTR(tf.numeroNotaProve, 7) AS SECUENCIA_DOC,
        SUBSTR(tf.secuencialFacturaRet, 7) AS SECUENCIA_REL_DOC,
        tf.claveNotaProve AS CLAVE_TRANS,
        tn.claveFacturaRet AS CLAVE_REL_TRANS,
        CASE WHEN tf.tipo_nota = 'descuento' THEN 'descuento' ELSE 'devolucion'
    END AS TIP_DET_DOC,
    YEAR(tf.FEC_TRAC) AS FEC_PERIODO_TRAC,
    MONTH(tf.FEC_TRAC) AS FEC_MES_TRAC,
    tf.FEC_TRAC AS FEC_TRAC,
    tn.FEC_TRAC AS FEC_REL_TRAC,
    tf.FEC_MERC_TRAC AS FEC_MERC_TRAC,
    tf.METPAG_TRAC AS MET_PAG_TRAC,
    tf.OBS_TRAC,
    tf.FK_COD_USU AS FK_USER,
    tf.FK_COD_USU AS FK_USER_VEND,
    tf.FK_COD_PROVE AS FK_PERSON,
    tf.estado AS ESTADO,
    'activo' AS ESTADO_REL,
    tf.documentoAnulado AS RUTA_DOC_ANULADO,
    tf.SUB_BASE_5 AS SUB_BASE_5,
    tf.SUB_BASE_8 AS SUB_BASE_8,
    tf.SUB_BASE_12 AS SUB_BASE_12,
    tf.SUB_BASE_13 AS SUB_BASE_13,
    tf.SUB_BASE_14 AS SUB_BASE_14,
    tf.SUB_BASE_15 AS SUB_BASE_15,
    tf.SUB12_TRAC AS SUB_12_TRAC,
    tf.SUB0_TRAC AS SUB_0_TRAC,
    tf.SUBNOBJETO_TRAC AS SUB_N_OBJETO_TRAC,
    tf.SUBEXENTO_TRAC AS SUB_EXENTO_TRAC,
    tf.SUB_TRAC AS SUB_TRAC,
    tf.IVA_TRAC AS IVA_TRAC,
    tf.IVA_TRAC,
    tf.TOT_TRAC AS TOT_TRAC,
    tf.TOTRET_TRAC AS TOT_RET_TRAC,
    tf.TOTPAG_TRAC AS TOT_PAG_TRAC,
    tf.PROPINA_TRAC,
    tf.OTRA_PER,
    '04' AS COD_COMPROBANTE,
    '01' AS COD_COMPROBANTE_REL,
    tn.DOCSUCCOM_TRAC AS COD_DOCSUS_TRIB,
    NULL AS FK_COD_EMP,
    NULL AS COD_ASIENTO,
    tf.firmado AS FRIMADO,
    tf.enviado AS ENVIADO,
    tf.autorizado AS AUTORIZADO,
    tf.enviadoCliente AS ENVIADO_CLIEMAIL,
    tf.fechaAutorizado AS FECHA_AUTORIZACION,
    NULL AS FECHA_AUTORIZACION_REL,
    'electronica' AS TIP_DOC_REL,
    tf.DES_TRAC AS DSTO_TRAC,
    NULL AS FK_CODSUCURSAL,
    NULL AS FK_AUDITTR,
    'nota-compra' TIP_TRAC,
    tf.fecha AS FECHA_REG,
    tf.DET_TRAC AS DOCUMENT_DETAIL,
    CONCAT(
        SUBSTRING(tf.secuencialFacturaRet, 1, 3),
        '-',
        SUBSTRING(tf.secuencialFacturaRet, 4, 3)
    ) AS PUNTO_EMISION_REC,
    tf.COD_TRAC_FACT AS FK_DOC_REL,
    tf.fechaAnulacion AS FECHA_ANULACION,
    'electronica' AS TIP_DOC,
    NULL AS SRI_PAY_CODE,
    tf.IP_CLIENTE AS CLIENT_IP,
    NULL AS FK_AUDIT_REL,
    CONCAT(
        SUBSTRING(tf.numeroNotaProve, 1, 3), '-',
        SUBSTRING(tf.numeroNotaProve, 4, 3), '-',
        SUBSTRING(tf.numeroNotaProve, 7)
    ) AS NUM_TRANS,
    CONCAT(
        SUBSTRING(tf.secuencialFacturaRet, 1, 3), '-',
        SUBSTRING(tf.secuencialFacturaRet, 4, 3), '-',
        SUBSTRING(tf.secuencialFacturaRet, 7)
    ) AS NUM_REL_DOC,
    tf.serial_producto AS DIV_PAY_YEAR,
    tf.cabecera_compra AS DOCUMENT_REL_DETAIL,
    tf.RESPU_SRI AS RESP_SRI,
    tf.INFO_ADIC AS INFO_ADIC,
    tf.DET_REMBOLSO AS DET_EXP_REEMBOLSO,
    tf.METPAG_JSON_TRAC AS JSON_METODO,
    tf.INFO_ADIC AS ITEMS_PROF,
    tf.OBS_AUXILIAR AS OBS_AUXILIAR,
    tf.OBS_ORD AS OBS_ORDEN,
    tf.COD_TRAC_FACT,
    tf.NUM_TRACNOTCOM,
    SUBSTRING(tf.NUM_TRACNOTCOM, 1, 7) AS PUNTO_EMI_NOTCOM
    FROM
        transacciones tf
    INNER JOIN transacciones tn ON
        tf.COD_TRAC_FACT = tn.COD_TRAC
    WHERE
        tf.TIP_TRAC IN(
            'nota-compra',
            'nota-compra-devuelto'
        )
    ORDER BY
        tf.COD_TRAC
    DESC;
  `);
    const creditNotesPurchases = rows as any[];

    if (creditNotesPurchases.length === 0) {
      return { creditNotesPurchasesIdMap, creditNotesPurchasesAuditIdMap };
    }

    const branchSequenseQuery: string = `
	SELECT
			COD_SURC,
			SUBSTRING(secuencial, 1, 7) AS ELECTRONICA,
			SUBSTRING(secuencialFisica, 1, 7) AS FISICA,
			SUBSTRING(SURC_SEC_COMPINGR, 1, 7) AS COMPINGRESO
	FROM
			sucursales;`;

    const resultSequentialQuery = await legacyConn.query(branchSequenseQuery, [newCompanyId]);
    const [sequentialBranches]: any[] = resultSequentialQuery as Array<any>;

    const electronicSequences = new Map<string, number>();
    sequentialBranches.forEach((branch: BranchSequentialData, index: number) => {
      electronicSequences.set(branch.ELECTRONICA, branch.COD_SURC);
    });

    const auditId = await findNextAuditCode({ conn, companyId: newCompanyId });

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

    const BATCH_SIZE = 1000;

    for (let i = 0; i < creditNotesPurchases.length; i += BATCH_SIZE) {
      const batchCreditNotes = creditNotesPurchases.slice(i, i + BATCH_SIZE);

      const auditValues = batchCreditNotes.map((purchase: any, index: number) => {
        const auditIdIsert = auditId + index;
        return [
          auditIdIsert,
          "NCCOMPRA",
          newCompanyId
        ]
      });

      const [resultCreateAudit] = await conn.query(
        `INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
        [auditValues]
      );

      const firstAuditId = resultCreateAudit.insertId;

      const values = batchCreditNotes.map((t, index) => {
        console.log(`transformando y normalizando ${t.NUM_TRANS}`);

        const auditId = firstAuditId + index;
        creditNotesPurchasesAuditIdMap[t.COD_TRANS] = auditId;

        const previusDetailProd = toJSONArray(t.DOCUMENT_DETAIL);

        const { detailTransformed, branchId: branchIdProduct } = restructureCreditNoteDetail({
          inputDetail: previusDetailProd,
          mapProducts,
          storeMap,
          idFirstBranch,
          creditNoteType: t.TIP_DET_DOC,
          oldProductCodeMap
        });

        let branchId: number = branchIdProduct;
        if (electronicSequences.has(t.PUNTO_EMI_NOTCOM)) {
          let oldBranchId = electronicSequences.get(t.PUNTO_EMI_NOTCOM);
          branchId = branchMap[oldBranchId] || idFirstBranch;
        }

        const vendedor = userMap[t.FK_USER_VEND];
        const creador = userMap[t.FK_USER];
        const cliente = mapSuppliers[t.FK_PERSON];
        const relDocId = purchaseLiquidationIdMap[t.COD_TRAC_FACT];
        const idAuditRelDoc = purchaseLiquidationAuditIdMap[t.COD_TRAC_FACT] || null;

        const paymentMethod = toJSONArray(t.JSON_METODO);

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
          t.PROPINA_TRAC,
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
          branchId,
          auditId,
          t.TIP_TRAC,
          t.FECHA_REG,
          JSON.stringify(detailTransformed),
          t.PUNTO_EMISION_REC,
          relDocId,
          t.FECHA_ANULACION,
          t.TIP_DOC,
          t.SRI_PAY_CODE,
          t.CLIENT_IP,
          idAuditRelDoc,
          t.NUM_TRANS,
          t.NUM_REL_DOC,
          t.DIV_PAY_YEAR,
          null,
          safeJson(t.RESP_SRI),
          t.INFO_ADIC,
          t.DET_EXP_REEMBOLSO,
          JSON.stringify(paymentMethod),
          t.ITEMS_PROF,
          t.OBS_AUXILIAR,
          t.OBS_ORDEN
        ];
      });

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
            FK_DOC_REL,
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
      for (const s of batchCreditNotes) {
        creditNotesPurchasesIdMap[s.COD_TRANS] = newId++;
      }
      console.log(` -> Batch migrado: ${batchCreditNotes.length} notas de credito`);
    }

    const { mapNoteMovements } = await migrateMovementeAdvancesNote(
      legacyConn,
      conn,
      newCompanyId,
      userMap,
      bankMap,
      boxMap,
      mapConciliation,
      purchaseLiquidationObligationIdMap,
      mapPeriodo,
      mapProject,
      mapCenterCost,
      mapAccounts,
      creditNotesPurchasesIdMap,
      creditNotesPurchasesAuditIdMap
    )

    return { creditNotesPurchasesIdMap, creditNotesPurchasesAuditIdMap };
  } catch (error) {
    console.error('Error al migrar notas de credito:', error);
    throw error;
  }
}

export async function migrateMovementeAdvancesNote(
  legacyConn: any,
  conn: any,
  newCompanyId: number,
  userMap: any,
  bankMap: Record<number, number | null>,
  boxMap: Record<number, number | null>,
  mapConciliation: Record<number, number | null>,
  mapObligationsCustomers: Record<number, number | null>,
  mapPeriodo: Record<number, number | null>,
  mapProject: Record<number, number | null>,
  mapCenterCost: Record<number, number | null>,
  mapAccounts: Record<number, number | null>,
  mapCreditNote: Record<number, number | null>,
  mapAuditCreditNote: Record<number, number | null>
): Promise<{ mapNoteMovements: Record<number, number> }> {
  console.log("Migrando movimientos notas de credito en compras...");
  const mapNoteMovements: Record<number, number> = {};
  const [movements] = await legacyConn.query(`
    SELECT
        COD_TRAC,
        CONCAT(
            SUBSTRING(
                transacciones.numeroNotaProve,
                1,
                3
            ),
            '-',
            SUBSTRING(
                transacciones.numeroNotaProve,
                4,
                3
            ),
            '-',
            SUBSTRING(
                transacciones.numeroNotaProve,
                7
            )
        ) AS NUM_TRANS,
        COD_TRAC AS FK_COD_TRAN,
        m.FK_COD_BANCO_MOVI AS FKBANCO,
        m.FK_CONCILIADO AS FK_CONCILIADO,
        IFNULL(
            m.FK_USER_EMP_MOVI,
            transacciones.FK_COD_USU
        ) AS FK_USER,
        IFNULL(
            m.FECHA_MOVI,
            transacciones.fecha
        ) AS FECHA_MOVI,
        IFNULL(
            FECHA_MANUAL,
            transacciones.FEC_TRAC
        ) AS FECHA_MANUAL,
        CASE WHEN COALESCE(ID_DET_ANT, '') <> '' THEN 'CREDITO' WHEN m.TIP_MOVI IS NULL THEN 'CREDITO' ELSE m.TIP_MOVI
    END AS TIP_MOVI,
    IFNULL(
        m.ORIGEN_MOVI,
        'NOTA CREDITO COMPRA'
    ) AS ORIGEN_MOVI,
    'CREDITO' AS TIPO_MOVI,
    IFNULL(CONCEP_MOVI, OBS_DET_ANT) AS REF_MOVI,
    CONCAT(
        SUBSTRING(
            transacciones.numeroNotaProve,
            1,
            3
        ),
        '-',
        SUBSTRING(
            transacciones.numeroNotaProve,
            4,
            3
        ),
        '-',
        SUBSTRING(
            transacciones.numeroNotaProve,
            7
        )
    ) AS CONCEP_MOVI,
    NULL AS NUM_VOUCHER,
    NULL AS NUM_LOTE,
    IFNULL(CAUSA_MOVI, 'EGRESO') AS CAUSA_MOVI,
    'NCCOMPRA' AS MODULO,
    NULL AS SECU_MOVI,
    IFNULL(m.IMPOR_MOVI, TOTPAG_TRAC) AS IMPOR_MOVI,
    CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 1
    END AS ESTADO_MOVI,
    IFNULL(
        m.PER_BENE_MOVI,
        proveedores.NOM_PROV
    ) AS PER_BENE_MOVI,
    NULL AS FK_COD_EMP,
    m.FK_CONCILIADO,
    m.CONCILIADO,
    FK_COD_CAJAS_MOVI AS IDCAJA,
    IFNULL(
        transacciones.OBS_TRAC,
        m.CONCEP_MOVI
    ) AS OBS_MOVI,
    IFNULL(m.IMPOR_MOVI, TOTPAG_TRAC) AS IMPOR_MOVITOTAL,
    NULL AS FK_ASIENTO,
    NULL AS FK_AUDITMV,
    NULL AS FK_ARQUEO,
    NULL AS ID_TARJETA,
    NULL AS RECIBO_CAJA,
    NULL AS FK_CTAM_PLAN,
    NULL AS NUMERO_UNIDAD,
    NULL AS JSON_PAGOS,
    SUBSTRING(secuencialFacturaRet, 9, 9) AS SECUENCIA_REL_DOC,
    transacciones.claveFacturaRet AS CLAVE_REL_TRANS,
    YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
    MONTH(FEC_TRAC) AS FEC_MES_TRAC,
    FEC_TRAC AS FEC_TRAC,
    fechaFacturaRet AS FEC_REL_TRAC,
    FEC_MERC_TRAC AS FEC_MERC_TRAC,
    METPAG_TRAC,
    cabecera_compra AS DOCUMENT_REL_DETAIL,
    OBS_TRAC,
    FK_USU_CAJA AS FK_USER_VEND,
    FK_COD_PROVE AS FK_PERSON,
    estado_compra AS ESTADO_REL,
    SUB_TRAC AS SUB_TRAC,
    IVA_TRAC AS IVA_TRAC,
    SUB0_TRAC,
    SUB12_TRAC,
    TOTRET_TRAC AS TOT_RET_TRAC,
    TOTPAG_TRAC AS TOT_PAG_TRAC,
    NULL AS FECHA_AUTORIZACION_REL,
    NULL AS TIP_DOC_REL,
    NULL AS FK_AUDITTR,
    fecha AS FECHA_REG,
    NULL AS PUNTO_EMISION_REC,
    fechaAnulacion AS FECHA_ANULACION,
    NULL AS FK_AUDIT_REL,
    CONCAT(
        SUBSTRING(
            transacciones.numeroNotaProve,
            1,
            3
        ),
        '-',
        SUBSTRING(
            transacciones.numeroNotaProve,
            4,
            3
        ),
        '-',
        SUBSTRING(
            transacciones.numeroNotaProve,
            7
        )
    ) AS NUM_REL_DOC,
    detalle_anticipos.ID_DET_ANT,
    SECU_DET_ANT,
    FDP_DET_ANT,
    OBS_DET_ANT,
    IMPOR_DET_ANT,
    m.ID_MOVI,
    detalle_anticipos.ID_DET_ANT,
    transacciones.COD_TRAC_FACT
    FROM
        transacciones
    LEFT JOIN detalle_anticipos ON detalle_anticipos.FK_COD_TRAC = transacciones.COD_TRAC
    LEFT JOIN proveedores ON transacciones.FK_COD_PROVE = proveedores.ID_PROV
    LEFT JOIN movimientos m ON
        m.FK_TRAC_MOVI = transacciones.COD_TRAC
    WHERE
        transacciones.TIP_TRAC IN(
            'nota-compra',
            'nota-compra-devuelto'
        ) AND(
            detalle_anticipos.ID_DET_ANT IS NULL OR detalle_anticipos.ID_DET_ANT = ''
        ) AND(
            m.ID_MOVI IS NULL OR m.ID_MOVI = ''
        )
    ORDER BY
        COD_TRAC
    DESC
        ;
  `);

  if (movements.length === 0) {
    return { mapNoteMovements };
  }
  const BATCH_SIZE = 1000;
  const oldDetailAcountCodeMap = [];

  const movementSequenceQuery = await conn.query(`SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS SECU_MOVI FROM movements WHERE MODULO = 'NCCOMPRA' AND  FK_COD_EMP = ?`,
    [newCompanyId]
  );
  const [movementData] = movementSequenceQuery as Array<any>;
  let secuenciaMovimiento = movementData[0]?.SECU_MOVI ?? 1;

  for (let i = 0; i < movements.length; i += BATCH_SIZE) {
    const batch = movements.slice(i, i + BATCH_SIZE);
    const values = batch.map((o: any, index: number) => {
      console.log(`transformando y normalizando movimientos nota de credito ${o.NUM_TRANS}`);
      let idPlanCuenta = null;
      const currentAuditId = mapAuditCreditNote[o.FK_COD_TRAN];
      o.REF_MOVI = `NOTA CREDITO COMPRA N°:${o.NUM_TRANS}`;
      return [
        bankMap[o.FKBANCO] ?? null,
        mapCreditNote[o.COD_TRAC] ?? null,
        mapConciliation[o.FK_CONCILIADO] ?? null,
        userMap[o.FK_USER] ?? null,
        o.FECHA_MOVI,
        o.FECHA_MANUAL,
        o.TIP_MOVI,
        o.ORIGEN_MOVI,
        o.TIPO_MOVI,
        o.REF_MOVI,
        o.CONCEP_MOVI,
        o.NUM_VOUCHER,
        o.NUM_LOTE,
        o.CAUSA_MOVI,
        o.MODULO,
        secuenciaMovimiento++,
        o.IMPOR_MOVI,
        o.ESTADO_MOVI,
        o.PER_BENE_MOVI,
        o.CONCILIADO ?? null,
        newCompanyId,
        boxMap[o.IDCAJA] ?? null,
        o.OBS_MOVI,
        o.IMPOR_MOVITOTAL,
        null, // FK_ASIENTO
        currentAuditId,
        null, // FK_ARQUEO
        null,
        null, // RECIBO_CAJA
        idPlanCuenta,
        null, // NUM_UNIDAD
        '[]'  // JSON_PAGOS
      ];

    });

    const [resMov]: any = await conn.query(
      `INSERT INTO movements (
                  FKBANCO, FK_COD_TRAN, FK_CONCILIADO, FK_USER, FECHA_MOVI, FECHA_MANUAL,
                  TIP_MOVI, ORIGEN_MOVI, TIPO_MOVI, REF_MOVI, CONCEP_MOVI, NUM_VOUCHER,
                  NUM_LOTE, CAUSA_MOVI, MODULO, SECU_MOVI, IMPOR_MOVI, ESTADO_MOVI,
                  PER_BENE_MOVI, CONCILIADO, FK_COD_EMP, IDDET_BOX, OBS_MOVI,
                  IMPOR_MOVITOTAL, FK_ASIENTO, FK_AUDITMV, FK_ARQUEO, ID_TARJETA,
                  RECIBO_CAJA, FK_CTAM_PLAN, NUMERO_UNIDAD, JSON_PAGOS
              ) VALUES ?`,
      [values]
    );


    let currentMovId = resMov.insertId;
    batch.forEach(o => {
      mapNoteMovements[o.COD_TRAC] = currentMovId++;
      oldDetailAcountCodeMap.push({
        idFactura: o.COD_TRAC_FACT,
        importe: o.IMPOR_MOVI,
        fecha: o.FECHA_MANUAL,
        idTrn: o.COD_TRAC,
        audit: mapNoteMovements[o.COD_TRAC]
      });
    });
    console.log(` -> Batch migrado: ${batch.length} notas de credito`);

  }


  const { mapNoteMovementsFull } = await migrateRetentionsCredit(
    legacyConn,
    conn,
    newCompanyId,
    userMap,
    bankMap,
    boxMap,
    mapConciliation,
    mapCreditNote,
    mapAuditCreditNote,
    mapNoteMovements
  );

  const mapObligations = await migratePaymentDetails(
    legacyConn,
    conn,
    mapObligationsCustomers,
    mapNoteMovementsFull,
    oldDetailAcountCodeMap
  );

  const mapEntryAccount = await migrateAccountingEntriesCustomerObligations(
    legacyConn,
    conn,
    newCompanyId,
    mapNoteMovementsFull,
    mapPeriodo,
    mapAuditCreditNote
  );

  const mapEntryDetailAccount = await migrateDetailedAccountingEntriesCustomerObligations(
    legacyConn,
    conn,
    newCompanyId,
    mapProject,
    mapCenterCost,
    mapAccounts,
    mapEntryAccount.mapEntryAccount
  )


  return { mapNoteMovements };
}

export async function migrateRetentionsCredit(
  legacyConn: any,
  conn: any,
  newCompanyId: number,
  userMap: any,
  bankMap: Record<number, number | null>,
  boxMap: Record<number, number | null>,
  mapConciliation: Record<number, number | null>,
  mapCreditNote: Record<number, number | null>,
  mapAuditCreditNote: Record<number, number | null>,
  mapNoteMovements: Record<number, number | null>,

): Promise<{ mapNoteMovementsFull: Record<number, number> }> {
  try {
    console.log("Migrando movimientos de notas de credito compras anticipo...");
    const mapNoteMovementsFull: Record<number, number> = { ...mapNoteMovements };

    const [rows] = await legacyConn.query(`
    SELECT
        COD_TRAC,
        CONCAT(
            SUBSTRING(
                transacciones.numeroNotaProve,
                1,
                3
            ),
            '-',
            SUBSTRING(
                transacciones.numeroNotaProve,
                4,
                3
            ),
            '-',
            SUBSTRING(
                transacciones.numeroNotaProve,
                7
            )
        ) AS NUM_TRANS,
        COD_TRAC AS FK_COD_TRAN,
        m.FK_COD_BANCO_MOVI AS FKBANCO,
        m.FK_CONCILIADO AS FK_CONCILIADO,
        IFNULL(
            m.FK_USER_EMP_MOVI,
            transacciones.FK_COD_USU
        ) AS FK_USER,
        IFNULL(
            m.FECHA_MOVI,
            transacciones.fecha
        ) AS FECHA_MOVI,
        IFNULL(
            FECHA_MANUAL,
            transacciones.FEC_TRAC
        ) AS FECHA_MANUAL,
        'ANTICIPO' AS TIP_MOVI,
        IFNULL(
            m.ORIGEN_MOVI,
            'NOTA CREDITO COMPRA'
        ) AS ORIGEN_MOVI,
        'ANTICIPO' AS TIPO_MOVI,
        IFNULL(CONCEP_MOVI, OBS_DET_ANT) AS REF_MOVI,
        IFNULL(
            CONCAT(
                SUBSTRING(
                    transacciones.numeroNotaProve,
                    1,
                    3
                ),
                '-',
                SUBSTRING(
                    transacciones.numeroNotaProve,
                    4,
                    3
                ),
                '-',
                SUBSTRING(
                    transacciones.numeroNotaProve,
                    7
                )
            ),
            OBS_DET_ANT
        ) AS CONCEP_MOVI,
        NULL AS NUM_VOUCHER,
        NULL AS NUM_LOTE,
        IFNULL(CAUSA_MOVI, 'EGRESO') AS CAUSA_MOVI,
        'NCCOMPRA' AS MODULO,
        detalle_anticipos.SECU_DET_ANT AS SECU_MOVI,
        IFNULL(m.IMPOR_MOVI, TOTPAG_TRAC) AS IMPOR_MOVI,
        CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 1
    END AS ESTADO_MOVI,
    IFNULL(
        m.PER_BENE_MOVI,
        proveedores.NOM_PROV
    ) AS PER_BENE_MOVI,
    NULL AS FK_COD_EMP,
    m.FK_CONCILIADO,
    m.CONCILIADO,
    FK_COD_CAJAS_MOVI AS IDCAJA,
    IFNULL(
        transacciones.OBS_TRAC,
        m.CONCEP_MOVI
    ) AS OBS_MOVI,
    IFNULL(m.IMPOR_MOVI, TOTPAG_TRAC) AS IMPOR_MOVITOTAL,
    NULL AS FK_ASIENTO,
    NULL AS FK_AUDITMV,
    NULL AS FK_ARQUEO,
    NULL AS ID_TARJETA,
    NULL AS RECIBO_CAJA,
    NULL AS FK_CTAM_PLAN,
    NULL AS NUMERO_UNIDAD,
    NULL AS JSON_PAGOS,
    SUBSTR(numeroNotaProve, 7) AS SECUENCIA_REL_DOC,
    transacciones.claveFacturaRet AS CLAVE_REL_TRANS,
    YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
    MONTH(FEC_TRAC) AS FEC_MES_TRAC,
    FEC_TRAC AS FEC_TRAC,
    fechaFacturaRet AS FEC_REL_TRAC,
    FEC_MERC_TRAC AS FEC_MERC_TRAC,
    METPAG_TRAC,
    cabecera_compra AS DOCUMENT_REL_DETAIL,
    OBS_TRAC,
    FK_USU_CAJA AS FK_USER_VEND,
    FK_COD_PROVE AS FK_PERSON,
    estado_compra AS ESTADO_REL,
    SUB_TRAC AS SUB_TRAC,
    IVA_TRAC AS IVA_TRAC,
    SUB0_TRAC,
    SUB12_TRAC,
    TOTRET_TRAC AS TOT_RET_TRAC,
    TOTPAG_TRAC AS TOT_PAG_TRAC,
    NULL AS FECHA_AUTORIZACION_REL,
    NULL AS TIP_DOC_REL,
    NULL AS FK_AUDITTR,
    fecha AS FECHA_REG,
    NULL AS PUNTO_EMISION_REC,
    fechaAnulacion AS FECHA_ANULACION,
    NULL AS FK_AUDIT_REL,
    CONCAT(
        SUBSTRING(
            transacciones.numeroNotaProve,
            1,
            3
        ),
        '-',
        SUBSTRING(
            transacciones.numeroNotaProve,
            4,
            3
        ),
        '-',
        SUBSTRING(
            transacciones.numeroNotaProve,
            7
        )
    ) AS NUM_REL_DOC,
    detalle_anticipos.ID_DET_ANT,
    SECU_DET_ANT,
    FDP_DET_ANT,
    OBS_DET_ANT,
    IMPOR_DET_ANT,
    m.ID_MOVI,
    detalle_anticipos.ID_DET_ANT
    FROM
        transacciones
    LEFT JOIN detalle_anticipos ON detalle_anticipos.FK_COD_TRAC = transacciones.COD_TRAC
    LEFT JOIN proveedores ON transacciones.FK_COD_PROVE = proveedores.ID_PROV
    LEFT JOIN movimientos m ON
        m.FK_TRAC_MOVI = transacciones.COD_TRAC
    WHERE
        transacciones.TIP_TRAC IN(
            'nota-compra',
            'nota-compra-devuelto'
        ) AND(
            (
                detalle_anticipos.ID_DET_ANT IS NOT NULL AND detalle_anticipos.ID_DET_ANT <> ''
            ) OR(
                m.ID_MOVI IS NOT NULL AND m.ID_MOVI <> ''
            )
        )
    ORDER BY
        COD_TRAC
    DESC
        ;`);

    const creditNotes = rows as any[];
    if (!creditNotes.length) {
      return { mapNoteMovementsFull };
    }

    const BATCH_SIZE = 1000;

    const movementSequenceQuery = await conn.query(`SELECT IFNULL(MAX(SECU_MOVI) + 1, 1) AS SECU_MOVI FROM movements WHERE MODULO = 'NCCOMPRA' AND FK_COD_EMP = ?`,
      [newCompanyId]
    );
    const [movementData] = movementSequenceQuery as Array<any>;
    let secuenciaMovimiento = movementData[0]?.SECU_MOVI ?? 1;


    for (let i = 0; i < creditNotes.length; i += BATCH_SIZE) {
      const batchCreditNotes = creditNotes.slice(i, i + BATCH_SIZE);

      const values = batchCreditNotes.map((o: any, index: number) => {
        console.log(`transformando y normalizando movimientos de NC a credito en compras anticipo ${o.NUM_TRANS}`);
        // Lógica de Negocio
        let idPlanCuenta = null;
        const currentAuditId = mapAuditCreditNote[o.COD_TRAC];
        const reference = `NOTA CREDITO COMPRA N°:${o.NUM_TRANS}`;

        return [
          bankMap[o.FKBANCO] ?? null,
          mapCreditNote[o.COD_TRAC] ?? null,
          mapConciliation[o.FK_CONCILIADO] ?? null,
          userMap[o.FK_USER] ?? null,
          o.FECHA_MOVI,
          o.FECHA_MANUAL,
          o.TIP_MOVI,
          o.ORIGEN_MOVI,
          o.TIPO_MOVI,
          reference,
          o.CONCEP_MOVI,
          o.NUM_VOUCHER,
          o.NUM_LOTE,
          o.CAUSA_MOVI,
          o.MODULO,
          secuenciaMovimiento++,
          o.IMPOR_MOVI,
          o.ESTADO_MOVI,
          o.PER_BENE_MOVI,
          o.CONCILIADO ?? null,
          newCompanyId,
          boxMap[o.IDCAJA] ?? null,
          o.OBS_MOVI,
          o.IMPOR_MOVITOTAL,
          null, // FK_ASIENTO
          currentAuditId,
          null, // FK_ARQUEO
          null,
          null, // RECIBO_CAJA
          idPlanCuenta,
          null, // NUM_UNIDAD
          '[]'  // JSON_PAGOS
        ];

      });

      const [resMov]: any = await conn.query(
        `INSERT INTO movements (
                  FKBANCO, FK_COD_TRAN, FK_CONCILIADO, FK_USER, FECHA_MOVI, FECHA_MANUAL,
                  TIP_MOVI, ORIGEN_MOVI, TIPO_MOVI, REF_MOVI, CONCEP_MOVI, NUM_VOUCHER,
                  NUM_LOTE, CAUSA_MOVI, MODULO, SECU_MOVI, IMPOR_MOVI, ESTADO_MOVI,
                  PER_BENE_MOVI, CONCILIADO, FK_COD_EMP, IDDET_BOX, OBS_MOVI,
                  IMPOR_MOVITOTAL, FK_ASIENTO, FK_AUDITMV, FK_ARQUEO, ID_TARJETA,
                  RECIBO_CAJA, FK_CTAM_PLAN, NUMERO_UNIDAD, JSON_PAGOS
              ) VALUES ?`,
        [values]
      );

      let currentMovId = resMov.insertId;
      batchCreditNotes.forEach(o => {
        mapNoteMovementsFull[o.COD_TRAC] = currentMovId++;
      });
      console.log(` -> Batch migrado: ${batchCreditNotes.length} movimientos de NC COMPRA  anticipos`);
    }
    return { mapNoteMovementsFull };
  } catch (error) {
    throw error;
  }
}


export async function migratePaymentDetails(
  legacyConn: any,
  conn: any,
  mapObligationsCustomers: Record<number, number | null>,
  mapNoteMovementsFull: Record<number, number | null>,
  oldDetailAcountCodeMap: any
): Promise<{
  mapDetailObligationsAplicate: Record<number, number>
}> {

  console.log("🚀 Migrando detalle de cuentas de NC DE COMPRAS");
  const mapDetailObligationsAplicate: Record<number, number> = {};

  try {

    const [rows]: any[] = await legacyConn.query(`
      SELECT
          detalles_cuentas.fk_cod_cuenta,
          detalles_cuentas.FK_COD_GD,
          detalles_cuentas.fecha,
          detalles_cuentas.importe,
          detalles_cuentas.saldo,
          detalles_cuentas.nuevo_saldo,
          transacciones.COD_TRAC,
          transacciones.TIP_TRAC,
          detalles_cuentas.observacion_cp,
          detalles_cuentas.forma_pago_cp
      FROM
          cuentascp
      INNER JOIN transacciones ON transacciones.COD_TRAC = cuentascp.FK_TRAC_CUENTA
      INNER JOIN detalles_cuentas ON cuentascp.cod_cp = detalles_cuentas.fk_cod_cuenta
      INNER JOIN grupo_detalles_t ON detalles_cuentas.FK_COD_GD = grupo_detalles_t.ID_GD
      LEFT JOIN movimientos ON movimientos.FK_COD_CX = grupo_detalles_t.ID_GD
      WHERE
          cuentascp.Tipo_cxp = 'CXP' AND detalles_cuentas.forma_pago_cp IN('17')
      ORDER BY
          cod_detalle
      DESC
          ;
  `);

    if (rows.length === 0) return { mapDetailObligationsAplicate };

    const BATCH_SIZE = 1000;

    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const movementValues = batch.map((o, index: number) => {
        const idCuenta = mapObligationsCustomers[o.fk_cod_cuenta];


        let idFactNote = null;
        for (let i = 0; i < oldDetailAcountCodeMap.length; i++) {

          const importeOld = Number(oldDetailAcountCodeMap[i].importe).toFixed(2);
          const importeNew = Number(o.importe).toFixed(2);
          console.log(Number(oldDetailAcountCodeMap[i].idFactura), Number(o.COD_TRAC), importeOld, importeNew);
          if (Number(oldDetailAcountCodeMap[i].idFactura) === Number(o.COD_TRAC) && importeOld === importeNew) {

            idFactNote = oldDetailAcountCodeMap[i].idTrn;
            break;
          }

        }
        const idMovimiento = idFactNote != null ? mapNoteMovementsFull[idFactNote] : null;

        return [
          idCuenta,
          idMovimiento,
          o.fecha,
          o.importe,
          o.saldo,
          o.nuevo_saldo
        ];
      });

      const [resMov]: any = await conn.query(
        `INSERT INTO account_detail (
                  FK_COD_CUENTA, FK_ID_MOVI, FECHA_REG, IMPORTE, SALDO, NEW_SALDO
              ) VALUES ?`,
        [movementValues]
      );

      let currentMovId = resMov.insertId;
      batch.forEach(o => {
        mapDetailObligationsAplicate[o.fk_cod_cuenta] = currentMovId++;
      });
    }

    console.log("✅ Migración completada de detalle de cuentas notas de credito");
    return { mapDetailObligationsAplicate };

  } catch (err) {
    console.error("❌ Error:", err);
    throw err;
  }
}

export async function migrateAccountingEntriesCustomerObligations(
  legacyConn: any,
  conn: any,
  newCompanyId: number,
  mapNoteMovementsFull: Record<number, number | null>,
  mapPeriodo: Record<number, number | null>,
  mapAuditCreditNote: Record<number, number | null>,
): Promise<{
  mapEntryAccount: Record<number, number>
}> {
  try {
    console.log("🚀 Migrando encabezado de asiento contables nota de credito en compras");
    const mapEntryAccount: Record<number, number> = {};
    const [rows]: any[] = await legacyConn.query(`
      SELECT
          cod_asiento,
          fecha_asiento AS FECHA_ASI,
          descripcion_asiento AS DESCRIP_ASI,
          numero_asiento AS NUM_ASI,
          'NOTA CREDITO DEVOLUCION COMPRA' AS ORG_ASI,
          debe_asiento AS TDEBE_ASI,
          haber_asiento AS THABER_ASI,
          numero_asiento,
          CASE WHEN transacciones.tipo_nota = 'descuento' THEN 'NC-COM-DEV' ELSE 'NC-COM-DESC'
      END AS TIP_ASI,
      fk_cod_periodo AS FK_PERIODO,
      fecha_registro_asiento AS FECHA_REG,
      fecha_update_asiento AS FECHA_ACT,
      json_asi AS JSON_ASI,
      res_asiento AS RES_ASI,
      ben_asiento AS BEN_ASI,
      NULL AS FK_AUDIT,
      NULL AS FK_COD_EMP,
      contabilidad_asientos.FK_CODTRAC,
      transacciones.COD_TRAC,
      CAST(
          REGEXP_REPLACE(
              RIGHT(numero_asiento, 9),
              '[^0-9]',
              ''
          ) AS UNSIGNED
      ) AS SEC_ASI,
      cod_origen,
      NULL AS FK_MOV
      FROM
          contabilidad_asientos
      INNER JOIN transacciones ON transacciones.COD_TRAC = contabilidad_asientos.FK_CODTRAC
      WHERE
          TIP_TRAC IN(
              'nota-compra',
              'nota-compra-devuelto'
          );` );

    if (rows.length === 0) {
      return { mapEntryAccount };
    }

    const BATCH_SIZE = 1000;
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);

      const insertValues: any[] = batch.map(o => {
        const periodoId = mapPeriodo[o.FK_PERIODO];
        const idAuditTr = mapAuditCreditNote[o.COD_TRAC];
        const idMovimiento = mapNoteMovementsFull[o.COD_TRAC];
        return [
          o.FECHA_ASI,
          o.DESCRIP_ASI,
          o.NUM_ASI,
          o.ORG_ASI,
          o.TDEBE_ASI,
          o.THABER_ASI,
          o.TIP_ASI,
          periodoId,
          o.FECHA_REG,
          o.FECHA_ACT,
          o.JSON_ASI,
          o.RES_ASI,
          o.BEN_ASI,
          idAuditTr,
          newCompanyId,
          o.SEC_ASI,
          null,
          idMovimiento
        ];
      });

      const [res]: any = await conn.query(`
        INSERT INTO accounting_movements(
            FECHA_ASI,
            DESCRIP_ASI,
            NUM_ASI,
            ORG_ASI,
            TDEBE_ASI,
            THABER_ASI,
            TIP_ASI,
            FK_PERIODO,
            FECHA_REG,
            FECHA_ACT,
            JSON_ASI,
            RES_ASI,
            BEN_ASI,
            FK_AUDIT,
            FK_COD_EMP,
            SEC_ASI,
            FK_MOVTRAC,
            FK_MOV
        )
        VALUES ?`, [insertValues]);

      let newId = res.insertId;
      for (const o of batch) {
        mapEntryAccount[o.cod_asiento] = newId++;
      }
    }
    console.log("✅ Migración asiento contable completada correctamente");
    return { mapEntryAccount };
  } catch (err) {
    console.error("❌ Error en migración de asiento contable:", err);
    throw err;
  }
}

export async function migrateDetailedAccountingEntriesCustomerObligations(
  legacyConn: any,
  conn: any,
  newCompanyId: number,
  mapProject: Record<number, number | null>,
  mapCenterCost: Record<number, number | null>,
  mapAccounts: Record<number, number | null>,
  mapEntryAccount: Record<number, number | null>
): Promise<{ mapAccountDetail: Record<number, number> }> {
  console.log("🚀 Cuentas contables");
  console.log("🚀 Iniciando migración de detalles de asientos contables compra..........");

  const mapAccountDetail: Record<number, number> = {};

  const [rows]: any[] = await legacyConn.query(`
    SELECT
        d.cod_detalle_asiento,
        contabilidad_asientos.fecha_asiento,
        contabilidad_asientos.cod_asiento AS FK_COD_ASIENTO,
        d.debe_detalle_asiento AS DEBE_DET,
        d.haber_detalle_asiento AS HABER_DET,
        d.fk_cod_plan AS FK_CTAC_PLAN,
        d.fkProyectoCosto AS FK_COD_PROJECT,
        d.fkCentroCosto AS FK_COD_COST
    FROM
        contabilidad_asientos
    INNER JOIN transacciones ON transacciones.COD_TRAC = contabilidad_asientos.FK_CODTRAC
    INNER JOIN contabilidad_detalle_asiento d ON
        d.fk_cod_asiento = contabilidad_asientos.cod_asiento
    WHERE
        TIP_TRAC IN(
            'nota-compra',
            'nota-compra-devuelto'
        );
  `);

  if (rows.length === 0) {
    console.log("⚠️ No hay registros para migrar");
    return { mapAccountDetail };
  }

  const BATCH_SIZE = 1000;
  console.log(`📦 Total registros a migrar: ${rows.length}`);
  let totalDebe = 0;
  let totalHaber = 0;

  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    console.log(`➡️ Procesando batch ${i / BATCH_SIZE + 1}`);

    try {
      const insertValues: any[] = [];
      const totalsMap = new Map<string, any>();

      for (const o of batch) {
        const idPlan = mapAccounts[o.FK_CTAC_PLAN];
        const idProyecto = mapProject[o.FK_COD_PROJECT] ?? null;
        const idCentroCosto = mapCenterCost[o.FK_COD_COST] ?? null;
        const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO] ?? null;

        if (!idPlan || !idCodAsiento) continue;

        const debe = Number(o.DEBE_DET) || 0;
        const haber = Number(o.HABER_DET) || 0;

        insertValues.push([
          idCodAsiento,
          debe,
          haber,
          idPlan,
          idProyecto,
          idCentroCosto
        ]);

        const key = `${newCompanyId}-${idPlan}-${o.fecha_asiento}`;
        if (!totalsMap.has(key)) {
          totalsMap.set(key, {
            id_plan: idPlan,
            fecha: o.fecha_asiento,
            debe: 0,
            haber: 0,
            total: 0,
            operacion: "suma"
          });
        }

        const acc = totalsMap.get(key);
        acc.debe += debe;
        acc.haber += haber;
        acc.total++;

        totalHaber += haber;
        totalDebe += debe;
      }

      if (!insertValues.length) {
        console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
        continue;
      }

      const [res]: any = await conn.query(`
              INSERT INTO accounting_movements_det (
                  FK_COD_ASIENTO,
                  DEBE_DET,
                  HABER_DET,
                  FK_CTAC_PLAN,
                  FK_COD_PROJECT,
                  FK_COD_COST
              ) VALUES ?
          `, [insertValues]);

      let newId = res.insertId;

      for (const o of batch) {
        const idPlan = mapAccounts[o.FK_CTAC_PLAN];
        console.log(`➡️ Procesando detalle de asiento  ${idPlan}`);
        const idCodAsiento = mapEntryAccount[o.FK_COD_ASIENTO];

        if (!idPlan || !idCodAsiento) continue;

        mapAccountDetail[o.cod_detalle_asiento] = newId++;
      }

      for (const t of totalsMap.values()) {
        await upsertTotaledEntry(conn, t, newCompanyId);
      }

      console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado`);

    } catch (err) {
      console.error("❌ Error en batch:", err);
      throw err;
    }
  }

  console.log("🎉 Migración de detalles de asiento contables completada");
  return { mapAccountDetail };
}
