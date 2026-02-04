import { Connection, FieldPacket, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { findNextAuditCode, insertAudit, restructureProductDetails, restructureRetentionDetail, RetentionCodeValue, toJSONArray, toNumber } from "./purchaseHelpers";
import { fetchAccountingPeriod, upsertTotaledEntry } from "./migrationTools";

type MigratePurchasesParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	branchMap: Record<number, number>;
	userMap: Record<number, number | null>;
	mapSuppliers: Record<number, number>;
	mapProducts: Record<number, number>;
	oldRetentionCodeMap: Map<string, RetentionCodeValue>;
	newRetentionIdMap: Record<number, number>;
	mapCostExpenses: Record<number, number | null>;
	storeMap: Record<number, number>;
	idFirstBranch: number
};

type ResultSet = [RowDataPacket[] | RowDataPacket[][] | ResultSetHeader, FieldPacket[]];

interface BranchSequentialData {
	COD_SURC: number;
	ELECTRONICA: string;
	FISICA: string;
	COMPINGRESO: string;
}

type MigrateMovementsParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	purchaseLiquidationAuditIdMap: Record<number, number>;
	purchaseLiquidationIdMap: Record<number, number>;
	mapSuppliers: Record<number, number>;
	mapPeriodo: Record<number, number>;
	mapProject: Record<number, number>;
	mapCenterCost: Record<number, number>;
	userMap: Record<number, number>;
	mapAccounts: Record<number, number>;
	bankMap: Record<number, number>;
	boxMap: Record<number, number>;
	mapConciliation: Record<number, number>;
	mapCloseCash: Record<number, number | null>,
};

type MigrateObligationsParams = Omit<
	MigrateMovementsParams,
	'mapPeriodo' |
	'mapProject' |
	'mapCenterCost' |
	'bankMap' |
	'boxMap' |
	'mapConciliation' |
	'mapCloseCash'
>;

type MigrateDataMovementsParams = Omit<MigrateMovementsParams, 'mapCenterCost' | 'mapProject' | 'mapPeriodo' | 'mapAccounts' | 'mapSuppliers' | 'mapCloseCash'>

type MigrateAccountingEntriesParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	movementIdMap: Record<number, number>,
	purchaseLiquidationAuditIdMap: Record<number, number>;
	purchaseLiquidationIdMap: Record<number, number>;
	mapPeriodo: Record<number, number>;
}

type MigrateAccountingEntriesDetailParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	mapProject: Record<number, number>;
	mapCenterCost: Record<number, number>;
	mapAccounts: Record<number, number>;
	accountingEntryIdMap: Record<number, number>;
}

type MigrateImportedObligationsParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	oblAsiToAuditIdUser: Record<number, number>;
	mapAccounts: Record<number, number>;
}

type MigratePurchaseObligationDetailParams = Omit<MigrateMovementsParams, 'purchaseLiquidationAuditIdMap' | 'mapSuppliers'> & {
	purchaseLiquidationObligationIdMap: Record<number, number>;
}

type MigrateDetailsOfObligationsParams = {
	legacyConn: Connection;
	conn: Connection;
	purchaseLiquidationObligationIdMap: Record<number, number>;
	movementIdMap: Record<number, number>;
}

type MigrateAccountingEntryObligationsParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	movementIdMap: Record<number, number>;
	mapPeriodo: Record<number, number>;
	movementAuditIdMap: Record<number, number>;
}

type MigrateObligationJournalEntryDetailsParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	mapProject: Record<number, number>;
	mapCenterCost: Record<number, number>;
	mapAccounts: Record<number, number>;
	accountingEntriesIdMap: Record<number, number>;
}
export async function migratePurchasesAndLiquidations({
	legacyConn,
	conn,
	newCompanyId,
	branchMap,
	userMap,
	mapSuppliers,
	mapProducts,
	oldRetentionCodeMap,
	newRetentionIdMap,
	mapCostExpenses,
	storeMap,
	idFirstBranch
}: MigratePurchasesParams): Promise<{ purchaseLiquidationIdMap: Record<number, number>; purchaseLiquidationAuditIdMap: Record<number, number> }> {

	const purchaseLiquidationIdMap: Record<number, number> = {};
	const purchaseLiquidationAuditIdMap: Record<number, number> = {};

	const resultPurchasesQuery: ResultSet = await legacyConn.query(`
		SELECT
				COD_TRAC AS COD_TRANS,
				CONCAT(
						SUBSTRING(secuencialFacturaRet, 1, 3),
						'-',
						SUBSTRING(secuencialFacturaRet, 4, 3)
				) AS PUNTO_EMISION_DOC,
				SUBSTR(secuencialFacturaRet, 7) AS SECUENCIA_DOC,
				CASE WHEN secuencialCompraFisica <> '--' THEN SUBSTRING_INDEX(secuencialCompraFisica, '-', -1) WHEN RET_AUTCOMP IS NOT NULL THEN SUBSTRING_INDEX(RET_AUTCOMP, '-', -1)
		END AS SECUENCIA_REL_DOC,
		claveFacturaRet AS CLAVE_TRANS,
		CASE WHEN secuencialCompraFisica <> '--' THEN claveCompraFisica WHEN RET_AUTCOMP IS NOT NULL THEN claveFactura
		END AS CLAVE_REL_TRANS,
		CASE tipo_compra WHEN 'Mercaderia' THEN 'mercaderia' WHEN 'Gastos y Servicios' THEN 'gastos_servicios'
		END AS TIP_DET_DOC,
		YEAR(FEC_TRAC) AS FEC_PERIODO_TRAC,
		MONTH(FEC_TRAC) AS FEC_MES_TRAC,
		fechaFacturaRet AS FEC_TRAC,
		FEC_TRAC AS FEC_REL_TRAC,
		FEC_MERC_TRAC,
		METPAG_TRAC AS MET_PAG_TRAC,
		OBS_TRAC,
		FK_COD_USU AS FK_USER,
		FK_COD_PROVE AS FK_USER_VEND,
		FK_COD_PROVE AS FK_PERSON,
		CASE WHEN TIP_TRAC = 'Compra' THEN estado_compra WHEN TIP_TRAC = 'Liquidacion' THEN estado
		END AS ESTADO,
		estado AS ESTADO_REL,
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
		TOTRET_TRAC AS TOT_RET_TRAC,
		TOTPAG_TRAC AS TOT_PAG_TRAC,
		COALESCE(PROPINA_TRAC, 0) AS PROPINA_TRAC,
		OTRA_PER,
		CASE WHEN TIP_TRAC = 'Compra' THEN '01' WHEN TIP_TRAC = 'Liquidacion' THEN '03'
		END AS COD_COMPROBANTE,
		CASE WHEN secuencialCompraFisica <> '--' THEN '07' WHEN RET_AUTCOMP IS NOT NULL THEN '07'
		END AS COD_COMPROBANTE_REL,
		DOCSUCCOM_TRAC AS COD_DOCSUS_TRIB,
		NULL AS FK_COD_EMP,
		NULL AS COD_ASIENTO,
		firmado AS FRIMADO,
		enviado AS ENVIADO,
		autorizado AS AUTORIZADO,
		enviadoCliente AS ENVIADO_CLIEMAIL,
		NULL AS FECHA_AUTORIZACION,
		fechaAutorizado AS FECHA_AUTORIZACION_REL,
		CASE WHEN secuencialCompraFisica <> '--' THEN 'fisica' WHEN RET_AUTCOMP IS NOT NULL THEN 'electronica'
		END AS TIP_DOC_REL,
		DES_TRAC AS DSTO_TRAC,
		NULL AS FK_CODSUCURSAL,
		NULL AS FK_AUDITTR,
		CASE WHEN TIP_TRAC = 'Compra' THEN 'compra' WHEN TIP_TRAC = 'Liquidacion' THEN 'liquidacion'
		END AS TIP_TRAC,
		fecha AS FECHA_REG,
		DET_TRACCOM AS DOCUMENT_DETAIL,
		CASE WHEN secuencialCompraFisica <> '--' THEN SUBSTRING_INDEX(secuencialCompraFisica, '-', 2) WHEN RET_AUTCOMP IS NOT NULL THEN SUBSTRING_INDEX(RET_AUTCOMP, '-', 2)
		END AS PUNTO_EMISION_REC,
		NULL AS FK_DOC_REL,
		fechaAnulacion AS FECHA_ANULACION,
		CASE WHEN TIP_TRAC = 'Compra' THEN 'electronica' WHEN TIP_TRAC = 'Liquidacion' AND SUBTIPO_TRAC = 'Reembolso' THEN 'reembolso' WHEN TIP_TRAC = 'Liquidacion' AND NUMRETCOM_TRAC IS NULL AND RET_AUTCOMP IS NULL THEN 'fisica' WHEN TIP_TRAC = 'Liquidacion' AND NUMRETCOM_TRAC IS NOT NULL AND RET_AUTCOMP IS NOT NULL THEN 'electronica'
		END AS TIP_DOC,
		NULL AS SRI_PAY_CODE,
		IP_CLIENTE AS CLIENT_IP,
		NULL AS FK_AUDIT_REL,
		CONCAT(
				SUBSTRING(secuencialFacturaRet, 1, 3),
				'-',
				SUBSTRING(secuencialFacturaRet, 4, 3),
				'-',
				SUBSTRING(secuencialFacturaRet, 7)
		) AS NUM_TRANS,
		CASE WHEN secuencialCompraFisica <> '--' THEN secuencialCompraFisica WHEN RET_AUTCOMP IS NOT NULL THEN RET_AUTCOMP
		END AS NUM_REL_DOC,
		serial_producto AS DIV_PAY_YEAR,
		DET_TRAC AS DOCUMENT_REL_DETAIL,
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
				transacciones.TIP_TRAC IN('Compra', 'liquidacion')
		ORDER BY
				COD_TRAC
		DESC
				;
	`);
	const [purchases]: any[] = resultPurchasesQuery as Array<any>;
	if (purchases.length === 0) {
		return { purchaseLiquidationIdMap, purchaseLiquidationAuditIdMap };
	}

	const branchSequenseQuery: string = `
	SELECT
			COD_SURC,
			SUBSTRING(secuencial, 1, 7) AS ELECTRONICA,
			SUBSTRING(secuencialFisica, 1, 7) AS FISICA,
			SUBSTRING(SURC_SEC_COMPINGR, 1, 7) AS COMPINGRESO
	FROM
			sucursales;`;

	const resultSequentialQuery: ResultSet = await legacyConn.query(branchSequenseQuery, [newCompanyId]);
	const [sequentialBranches]: any[] = resultSequentialQuery as Array<any>;

	const electronicSequences = new Map<string, number>();
	sequentialBranches.forEach((branch: BranchSequentialData, index: number) => {
		electronicSequences.set(branch.ELECTRONICA, branch.COD_SURC);
	});

	const auditId = await findNextAuditCode({ conn, companyId: newCompanyId });

	const BATCH_SIZE: number = 1000;

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

	for (let i = 0; i < purchases.length; i += BATCH_SIZE) {

		const batchPurchase = purchases.slice(i, i + BATCH_SIZE);
		const auditValues = batchPurchase.map((purchase: any, index: number) => {
			const module = String(purchase.TIP_TRAC).toUpperCase();
			const auditIdIsert = auditId + index;
			return [
				auditIdIsert,
				module,
				newCompanyId
			]
		});

		const resultCreateAudit: ResultSet = await conn.query(
			`INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
			[auditValues]
		);

		const firstAuditId = (resultCreateAudit[0] as ResultSetHeader).insertId;
		const purchaseValues = batchPurchase.map((p: any, index: number) => {
			console.log(`transformando y normalizando secuencial: ${p.NUM_TRANS}`);

			const codTrans = p.COD_TRANS;
			const purchaseType = p.TIP_DET_DOC;
			const userId = userMap[p.FK_USER];// id usuario
			const sellerId = userMap[p.FK_USER];// id vendedor
			const supplierId = mapSuppliers[p.FK_PERSON];// id proveedor
			const auditId = firstAuditId + index;
			const sriPayCode = p.MET_PAG_TRAC === 'EFECTIVO' ? '01' : '20';
			// Mapeo de auditoria por transaccion
			purchaseLiquidationAuditIdMap[codTrans] = auditId;

			const previusDetailProd = toJSONArray(p.DOCUMENT_DETAIL);
			const detailProducts = restructureProductDetails({
				idFirstBranch,
				branchMap,
				purchaseType,
				inputDetail: previusDetailProd,
				mapCostExpenses,
				mapProducts,
				storeMap
			});

			const previusDetailRet = toJSONArray(p.DOCUMENT_REL_DETAIL);
			const structureRetention = restructureRetentionDetail({
				inputDetail: previusDetailRet,
				oldRetentionCodeMap,
				newRetentionIdMap,
			});

			let branchId: number = idFirstBranch;
			if (electronicSequences.has(p.PUNTO_EMISION_REC)) {
				const oldBranchId = electronicSequences.get(p.PUNTO_EMISION_REC);
				branchId = branchMap[oldBranchId] || idFirstBranch;
			}
			const paymentMethod = toJSONArray(p.JSON_METODO);
			return [
				p.PUNTO_EMISION_DOC,
				p.SECUENCIA_DOC,
				p.SECUENCIA_REL_DOC,
				p.CLAVE_TRANS,
				p.CLAVE_REL_TRANS,
				p.TIP_DET_DOC,
				p.FEC_PERIODO_TRAC,
				p.FEC_MES_TRAC,
				p.FEC_TRAC,
				p.FEC_REL_TRAC,
				p.FEC_MERC_TRAC,
				safeJson(p.MET_PAG_TRAC),
				p.OBS_TRAC,
				userId,
				sellerId,
				supplierId,
				p.ESTADO,
				p.ESTADO_REL,
				p.RUTA_DOC_ANULADO,
				p.SUB_BASE_5,
				p.SUB_BASE_8,
				p.SUB_BASE_12,
				p.SUB_BASE_13,
				p.SUB_BASE_14,
				p.SUB_BASE_15,
				p.SUB_12_TRAC,
				p.SUB_0_TRAC,
				p.SUB_N_OBJETO_TRAC,
				p.SUB_EXENTO_TRAC,
				p.SUB_TRAC,
				p.IVA_TRAC,
				p.TOT_RET_TRAC,
				p.TOT_PAG_TRAC,
				p.PROPINA_TRAC ?? 0,
				p.OTRA_PER,
				p.COD_COMPROBANTE,
				p.COD_COMPROBANTE_REL,
				p.COD_DOCSUS_TRIB,
				newCompanyId,
				p.FRIMADO,
				p.ENVIADO,
				p.AUTORIZADO,
				p.ENVIADO_CLIEMAIL,
				p.FECHA_AUTORIZACION,
				p.FECHA_AUTORIZACION_REL,
				p.TIP_DOC_REL,
				p.DSTO_TRAC,
				branchId,
				auditId,
				p.TIP_TRAC,
				p.FECHA_REG,
				JSON.stringify(detailProducts),
				p.PUNTO_EMISION_REC,
				p.FECHA_ANULACION,
				p.TIP_DOC,
				sriPayCode,
				p.CLIENT_IP ?? '0.0.0.0',
				p.FK_AUDIT_REL,
				p.NUM_TRANS,
				p.NUM_REL_DOC,
				p.DIV_PAY_YEAR,
				JSON.stringify(structureRetention),
				safeJson(p.RESP_SRI),
				p.INFO_ADIC,
				p.DET_EXP_REEMBOLSO,
				JSON.stringify(paymentMethod),
				p.ITEMS_PROF,
				p.OBS_AUXILIAR,
				p.OBS_ORDEN
			];
		});

		const resultCreatePurchase: ResultSet = await conn.query(`
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
		`, [purchaseValues]);

		let nextPurchaseId = (resultCreatePurchase[0] as ResultSetHeader).insertId;
		batchPurchase.forEach(({ COD_TRANS }) => {
			purchaseLiquidationIdMap[COD_TRANS] = nextPurchaseId++;
		});
	}

	return { purchaseLiquidationIdMap, purchaseLiquidationAuditIdMap };
}

export async function migratePurchaseAndLiquidationsMovements({
	legacyConn,
	conn,
	newCompanyId,
	mapPeriodo,
	mapProject,
	mapCenterCost,
	userMap,
	mapSuppliers,
	purchaseLiquidationAuditIdMap,
	purchaseLiquidationIdMap,
	mapAccounts,
	mapConciliation,
	bankMap,
	boxMap,
	mapCloseCash
}) {

	console.log("🚀 Iniciando migración obligaciones compra y liquidacion");
	const { purchaseLiquidationObligationIdMap, purchaseLiquidationObligationAuditIdMap, oblAsiToAuditId } = await migrateObligationsTransactions({
		legacyConn,
		conn,
		newCompanyId,
		purchaseLiquidationIdMap,
		userMap,
		mapSuppliers,
		purchaseLiquidationAuditIdMap,
		mapAccounts,
		mapCloseCash
	})
	console.log(`✅ Obligaciones migradas: ${Object.keys(purchaseLiquidationObligationIdMap).length}`);

	console.log("🚀 Migrando movimientos compra y liquidacion");
	const { movementIdMap } = await migrateDataMovements({
		legacyConn,
		conn,
		newCompanyId,
		purchaseLiquidationIdMap,
		purchaseLiquidationAuditIdMap,
		mapConciliation,
		userMap,
		bankMap,
		boxMap,
		mapCloseCash
	});
	console.log("✅ Migración de movimientos compra y liquidacion completada correctamente");

	console.log("🚀 Migrando asientos contable compra y liquidacion...");
	const { accountingEntryIdMap } = await migrateAccountingEntries({
		legacyConn,
		conn,
		newCompanyId,
		movementIdMap,
		purchaseLiquidationIdMap,
		purchaseLiquidationAuditIdMap,
		mapPeriodo
	});
	console.log("✅ Migrando detalle asientos contable compra y liquidacion completada correctamente");

	const { accountingEntryDetailIdMap } = await migrateAccountingEntriesDetail({
		legacyConn,
		conn,
		newCompanyId,
		mapProject,
		mapCenterCost,
		mapAccounts,
		accountingEntryIdMap
	})

	console.log("🎉 Migración de detalles completada");
	return { accountingEntryIdMap, accountingEntryDetailIdMap, purchaseLiquidationObligationIdMap }
}

async function migrateDataMovements({
	legacyConn,
	conn,
	newCompanyId,
	purchaseLiquidationIdMap,
	purchaseLiquidationAuditIdMap,
	mapConciliation,
	userMap,
	bankMap,
	boxMap,
	mapCloseCash
}): Promise<{
	movementIdMap: Record<number, number>
}> {
	try {
		const movementIdMap: Record<number, number> = {};

		const movementsQuery: ResultSet = await legacyConn.query(`
			SELECT
					m.ID_MOVI,
					t.COD_TRAC AS COD_TRANS,
					m.FK_COD_CAJAS_MOVI,
					m.FK_COD_BANCO_MOVI,
					IFNULL(m.TIP_MOVI, t.METPAG_TRAC) AS TIP_MOVI,
					m.periodo_caja,
					IFNULL(m.FECHA_MOVI, t.fecha) AS FECHA_MOVI,
					CASE WHEN t.TIP_TRAC = 'Compra' THEN 'COMPRA' WHEN t.TIP_TRAC = 'liquidacion' THEN 'LIQUIDACION'
			END AS ORIGEN_MOVI,
			IFNULL(m.IMPOR_MOVI, t.TOTPAG_TRAC) AS IMPOR_MOVI,
			IFNULL(m.TIPO_MOVI, t.METPAG_TRAC) AS TIPO_MOVI,
			m.REF_MOVI,
			t.NUM_TRAC AS CONCEP_MOVIMIG,
			CASE WHEN m.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0
			END AS ESTADO_MOVI,
			IFNULL(
					m.PER_BENE_MOVI,
					IFNULL(mt.PER_BENE_MOVI, 'MIG')
			) AS PER_BENE_MOVI,
			'EGRESO' AS CAUSA_MOVI,
			CASE WHEN t.TIP_TRAC = 'Compra' THEN 'COMPRA' WHEN t.TIP_TRAC = 'liquidacion' THEN 'LIQUIDACION'
			END AS MODULO,
			IFNULL(m.FECHA_MANUAL, t.FEC_TRAC) AS FECHA_MANUAL,
			m.CONCILIADO,
			m.FK_COD_CX,
			m.SECU_MOVI,
			m.FK_CONCILIADO,
			m.FK_ANT_MOVI,
			IFNULL(
					m.FK_USER_EMP_MOVI,
					t.FK_COD_USU
			) AS FK_USER_EMP_MOVI,
			IFNULL(m.FK_TRAC_MOVI, t.COD_TRAC) AS FK_TRAC_MOVI,
			mt.NUM_VOUCHER AS NUM_VOUCHER,
			mt.NUM_LOTE AS NUM_LOTE,
			m.CONCEP_MOVI AS OBS_MOVI,
			t.TOTPAG_TRAC,
			NULL AS FK_ASIENTO,
			NULL AS FK_ARQUEO,
			m.RECIBO_CAJA,
			m.NUM_UNIDAD
			FROM
					transacciones t
			LEFT JOIN movimientos m ON
					m.FK_TRAC_MOVI = t.COD_TRAC
			LEFT JOIN movimientos_tarjeta mt ON
					mt.FK_TRAC_MOVI = t.COD_TRAC
			WHERE
					t.TIP_TRAC IN('Compra', 'liquidacion')
			ORDER BY
					ID_MOVI ASC;
		`);
		const [movements]: any[] = movementsQuery as Array<any>;
		if (movements.length === 0) {
			return { movementIdMap };
		}

		const cardQuery: ResultSet = await conn.query(`SELECT ID_TARJETA FROM cards WHERE FK_COD_EMP = ?`, [newCompanyId]);
		const [cardData]: any[] = cardQuery as Array<any>;
		const cardId = cardData[0].ID_TARJETA ?? null;

		const movementSequenceQuery = await conn.query(`SELECT MAX(SECU_MOVI)+1 AS SECU_MOVI FROM movements WHERE MODULO = 'COMPRAS' AND  FK_COD_EMP = ?`,
			[newCompanyId]
		);
		const [movementData] = movementSequenceQuery as Array<any>;
		let movementSequence = movementData[0]?.SECU_MOVI ?? 1;

		const BATCH_SIZE = 1500;

		for (let i = 0; i < movements.length; i += BATCH_SIZE) {
			const batchMovements = movements.slice(i, i + BATCH_SIZE);
			const movementValues: any[] = [];

			for (const m of batchMovements) {
				const bankId = bankMap[m.FK_COD_BANCO_MOVI];
				const idBoxDetail = boxMap[m.FK_COD_CAJAS_MOVI];
				const transactionId = purchaseLiquidationIdMap[m.FK_TRAC_MOVI];
				const userId = userMap[m.FK_USER_EMP_MOVI];
				const transAuditId = purchaseLiquidationAuditIdMap[m.COD_TRANS];
				const idFkConciliation = mapConciliation[m.FK_CONCILIADO] ?? null;
				const idPlanAccount = null;
				m.FK_ARQUEO = mapCloseCash[m.FK_ARQUEO] ?? null;
				movementValues.push([
					bankId,
					transactionId,
					idFkConciliation,
					userId,
					m.FECHA_MOVI,
					m.FECHA_MANUAL,
					m.TIP_MOVI,
					m.ORIGEN_MOVI,
					m.TIPO_MOVI,
					m.REF_MOVI,
					m.CONCEP_MOVIMIG,
					m.NUM_VOUCHER,
					m.NUM_LOTE,
					m.CAUSA_MOVI,
					m.MODULO,
					movementSequence,
					m.IMPOR_MOVI,
					m.ESTADO_MOVI,
					m.PER_BENE_MOVI,
					m.CONCILIADO,
					newCompanyId,
					idBoxDetail,
					m.OBS_MOVI,
					m.TOTPAG_TRAC,
					m.FK_ASIENTO,
					transAuditId,
					m.FK_ARQUEO,
					m.TIPO_MOVI === 'TARJETA' ? cardId : null,
					m.RECIBO_CAJA,
					idPlanAccount,
					m.NUM_UNIDAD,
					m.JSON_PAGOS
				]);
				movementSequence++;
			}

			const resultCreateMovement = await conn.query(`
				INSERT INTO movements(
						FKBANCO,
						FK_COD_TRAN,
						FK_CONCILIADO,
						FK_USER,
						FECHA_MOVI,
						FECHA_MANUAL,
						TIP_MOVI,
						ORIGEN_MOVI,
						TIPO_MOVI,
						REF_MOVI,
						CONCEP_MOVI,
						NUM_VOUCHER,
						NUM_LOTE,
						CAUSA_MOVI,
						MODULO,
						SECU_MOVI,
						IMPOR_MOVI,
						ESTADO_MOVI,
						PER_BENE_MOVI,
						CONCILIADO,
						FK_COD_EMP,
						IDDET_BOX,
						OBS_MOVI,
						IMPOR_MOVITOTAL,
						FK_ASIENTO,
						FK_AUDITMV,
						FK_ARQUEO,
						ID_TARJETA,
						RECIBO_CAJA,
						FK_CTAM_PLAN,
						NUMERO_UNIDAD,
						JSON_PAGOS
				)
				VALUES ?
			`, [movementValues]);
			let nextMovementId = (resultCreateMovement[0] as ResultSetHeader).insertId;
			batchMovements.forEach(({ COD_TRANS }) => {
				movementIdMap[COD_TRANS] = nextMovementId++;
			});
		}
		return { movementIdMap };
	} catch (error) {
		throw error;
	}
}

async function migrateObligationsTransactions({
	legacyConn,
	conn,
	newCompanyId,
	userMap,
	mapSuppliers,
	purchaseLiquidationIdMap,
	purchaseLiquidationAuditIdMap,
	mapAccounts,
	mapCloseCash
}) {

	try {
		console.log("🚀 Migrando obligaciones compra");
		const purchaseLiquidationObligationIdMap: Record<number, number> = {};
		const purchaseLiquidationObligationAuditIdMap: Record<number, number> = {};
		const oblAsiToAuditId: Record<number, number> = {};
		const oblAsiToAuditIdUser: Record<number, number | null> = {};
		const oblAsientoToAuditId: Record<number, number> = {};

		const auditId = await findNextAuditCode({ conn, companyId: newCompanyId });

		const resultObligationsQuery: ResultSet = await legacyConn.query(`
		SELECT
				cp.cod_cp AS old_id,
				cp.fk_cod_prov_cp AS fk_persona_old,
				cp.FK_TRAC_CUENTA AS fk_cod_trans_old,
				cp.OBL_ASI AS obl_asi_group,
				cp.Tipo_cxp AS tipo_obl,
				CASE
	        	WHEN 
				 cp.fecha_emision_cxp < '2015-01-01'
	        	THEN cp.fecha_vence_cxp
	        	ELSE cp.fecha_emision_cxp
    			END AS fech_emision,
				cp.fecha_vence_cxp AS fech_vencimiento,
				cp.tipo_documento AS tip_doc,
				cp.estado_cxp AS estado,
				cp.saldo_cxp AS saldo,
				cp.valor_cxp AS total,
				cp.referencia_cxp AS ref_secuencia,
				cp.TIPO_CUENTA AS tipo_cuenta,
				cp.FK_SERVICIO AS fk_id_infcon,
				cp.TIPO_ESTADO_CUENTA AS tipo_estado_cuenta,
				t.TIP_TRAC AS tipoTransaccion,
				fk_cod_usu_cp
		FROM
				cuentascp cp
		LEFT JOIN transacciones t ON
				t.COD_TRAC = cp.FK_TRAC_CUENTA
		WHERE
				cp.Tipo_cxp = 'CXP' AND(
						t.TIP_TRAC = 'Compra' OR t.TIP_TRAC = 'liquidacion' OR cp.tipo_cuenta = 'Importado'
				)
		ORDER BY
				cp.cod_cp;
	`);
		const [obligations]: any[] = resultObligationsQuery as Array<any>;
		if (obligations.length === 0) {
			return { purchaseLiquidationObligationIdMap, purchaseLiquidationObligationAuditIdMap, oblAsiToAuditId };
		}

		const BATCH_SIZE: number = 1500;
		let nexAuditId = auditId;
		let importedAuditId: number | null = null;

		for (let i = 0; i < obligations.length; i += BATCH_SIZE) {
			const batchObligation = obligations.slice(i, i + BATCH_SIZE);
			const valuesInsertObligations: any[] = [];
			for (const o of batchObligation) {
				let auditIdInsert: number | null = null;
				if (o.fk_cod_trans_old && purchaseLiquidationAuditIdMap[o.fk_cod_trans_old]) {
					auditIdInsert = purchaseLiquidationAuditIdMap[o.fk_cod_trans_old];
				} else if (o.obl_asi_group !== null) {
					if (!oblAsiToAuditId[o.obl_asi_group]) {
						const codigoAut = nexAuditId++;
						const auditIdInserted = await insertAudit({
							conn,
							codigoAudit: codigoAut,
							module: 'COMPRAS',
							companyId: newCompanyId
						});
						oblAsiToAuditId[o.obl_asi_group] = auditIdInserted;
						oblAsiToAuditIdUser[auditIdInserted] = userMap[o.fk_cod_usu_cp];
						oblAsientoToAuditId[auditIdInserted] = o.obl_asi_group;
					}
					auditIdInsert = oblAsiToAuditId[o.obl_asi_group];
				} else if (o.tipo_cuenta === 'Importado') {
					if (!importedAuditId) {
						const codigoAut = nexAuditId++;
						importedAuditId = await insertAudit({
							conn,
							codigoAudit: codigoAut,
							module: 'COMPRAS',
							companyId: newCompanyId
						});
						oblAsiToAuditIdUser[importedAuditId] = userMap[o.fk_cod_usu_cp];
						oblAsientoToAuditId[importedAuditId] = o.obl_asi_group;
					}
					auditIdInsert = importedAuditId;
				}
				if (!auditIdInsert) {
					throw new Error(`No se pudo resolver AUDIT para obligación ${o.old_id}`);
				}
				purchaseLiquidationObligationAuditIdMap[o.old_id] = auditIdInsert;

				const supplierId = mapSuppliers[o.fk_persona_old];
				if (!supplierId) {
					throw new Error(`Cliente no mapeado: ${o.fk_persona_old}`);
				}

				valuesInsertObligations.push([
					supplierId,
					o.tipo_obl,
					o.fech_emision,
					o.fech_vencimiento,
					o.tip_doc,
					o.estado,
					o.saldo,
					o.total,
					o.ref_secuencia,
					purchaseLiquidationIdMap[o.fk_cod_trans_old] ?? null,
					o.tipo_cuenta,
					o.fk_id_infcon,
					o.tipo_estado_cuenta,
					auditIdInsert,
					new Date(),
					newCompanyId
				]);
			};

			const resultCreateObligations: ResultSet = await conn.query(`
			INSERT INTO cuentas_obl(
					FK_PERSONA,
					TIPO_OBL,
					FECH_EMISION,
					FECH_VENCIMIENTO,
					TIP_DOC,
					ESTADO,
					SALDO,
					TOTAL,
					REF_SECUENCIA,
					FK_COD_TRANS,
					TIPO_CUENTA,
					FK_ID_INFCON,
					TIPO_ESTADO_CUENTA,
					FK_AUDITOB,
					OBLG_FEC_REG,
					FK_COD_EMP
			)
			VALUES ?
		`, [valuesInsertObligations]);
			let nextPurchaseId = (resultCreateObligations[0] as ResultSetHeader).insertId;
			batchObligation.forEach((o: any) => {
				purchaseLiquidationObligationIdMap[o.old_id] = nextPurchaseId++;
			});
		}

		const { migratedMovementsIdMap } = await migrateImportedObligations({
			legacyConn,
			conn,
			newCompanyId,
			oblAsiToAuditIdUser,
			mapAccounts
		})

		return { purchaseLiquidationObligationIdMap, purchaseLiquidationObligationAuditIdMap, oblAsiToAuditId };
	} catch (error) {
		throw error;
	}
}

async function migrateImportedObligations({
	legacyConn,
	conn,
	newCompanyId,
	oblAsiToAuditIdUser,
	mapAccounts
}: MigrateImportedObligationsParams): Promise<{
	migratedMovementsIdMap: Record<number, number>
}> {
	try {
		console.log("🚀 Migrando movimientos importados");
		const migratedMovementsIdMap: Record<number, number> = {};
		const seatIdMap: Record<number, number> = {};
		const movements: any[] = [];
		const BATCH_SIZE = 1500;
		type AggRow = {
			IMPORTE: number;
			COD_CUENTA: number | null;
			FK_PERSONA: number | null;
			TIPO_OBL: string | null;
			FECH_EMISION: any;
			FECH_VENCIMIENTO: any;
			TIP_DOC: string | null;
			ESTADO: string | null;
			SALDO: number;
			FK_COD_TRANS: number | null;
			FK_PAYROLL: number | null;
			TIPO_CUENTA: string | null;
			TIPO_ESTADO_CUENTA: string | null;
			FK_COD_EMP: number | null;
			FK_AUDITOB: number;
			REF_SECUENCIA: string | null;
			FK_ID_INFCON: number | null;
			OBLG_FEC_REG: any;
		};
		// consultar movimientos importados (migrados) agrupados
		const resultObligationsQuery: ResultSet = await conn.query(`
			SELECT
					SUM(TOTAL) AS IMPORTE,
					MAX(COD_CUENTA) AS COD_CUENTA,
					MAX(FK_PERSONA) AS FK_PERSONA,
					MAX(TIPO_OBL) AS TIPO_OBL,
					MAX(FECH_EMISION) AS FECH_EMISION,
					MAX(FECH_VENCIMIENTO) AS FECH_VENCIMIENTO,
					MAX(TIP_DOC) AS TIP_DOC,
					MAX(ESTADO) AS ESTADO,
					SUM(SALDO) AS SALDO,
					MAX(FK_COD_TRANS) AS FK_COD_TRANS,
					MAX(FK_PAYROLL) AS FK_PAYROLL,
					MAX(TIPO_CUENTA) AS TIPO_CUENTA,
					MAX(TIPO_ESTADO_CUENTA) AS TIPO_ESTADO_CUENTA,
					MAX(FK_COD_EMP) AS FK_COD_EMP,
					FK_AUDITOB,
					MAX(REF_SECUENCIA) AS REF_SECUENCIA,
					MAX(FK_ID_INFCON) AS FK_ID_INFCON,
					MAX(OBLG_FEC_REG) AS OBLG_FEC_REG
			FROM
					cuentas_obl
			WHERE
					TIPO_OBL = 'CXP' AND TIPO_CUENTA = 'Importado' AND FK_COD_EMP = ?
			GROUP BY
					FK_AUDITOB;
		`, [newCompanyId]);

		const [importedObligations]: any[] = resultObligationsQuery as Array<any>;
		if (importedObligations.length === 0) {
			return { migratedMovementsIdMap };
		}

		const resultSequenceMovement: ResultSet = await conn.query(`
			SELECT
					MAX(SECU_MOVI) +1 AS SECU_MOVI
			FROM
					movements
			WHERE
					MODULO = 'IMP-CXP' AND FK_COD_EMP = ?;`,
			[newCompanyId]
		);

		const [movement]: any[] = resultSequenceMovement as Array<any>;
		let movementSequence: number = movement[0]?.SECU_MOVI || 1;
		for (let i = 0; i < importedObligations.length; i += BATCH_SIZE) {
			const batchObligation: AggRow[] = importedObligations.slice(i, i + BATCH_SIZE);
			const valuesInsertObligations: any[] = [];
			for (const o of batchObligation) {
				const userId = oblAsiToAuditIdUser[o.FK_AUDITOB];
				const audtiObligationId = o.FK_AUDITOB;
				movements.push(
					{
						secuencia: movementSequence,
						importe: o.IMPORTE,
						auditoria: o.FK_AUDITOB,
						emision: o.FECH_EMISION,
						registro: o.OBLG_FEC_REG,
					}
				);
				valuesInsertObligations.push([
					null,
					null,
					null,
					userId,
					o.FECH_EMISION,
					o.OBLG_FEC_REG,
					'IMP-CXP',
					'IMP-CXP',
					'IMPORTACION',
					'IMPORTACION DE OBLIGACIONES',
					'Importación',
					null,
					null,
					'EGRESO',
					'IMP-CXP',
					movementSequence,
					o.IMPORTE,
					1,
					'IMPORTACION A SISTEMA',
					null,
					newCompanyId,
					null,
					'Importación',
					o.IMPORTE,
					null,
					audtiObligationId,
					null,
					null,
					null,
					null,
					null,
					'[]'
				]);
				movementSequence++;
			}

			const resultCreateMovement: ResultSet = await conn.query(`
				INSERT INTO movements(
						FKBANCO,
						FK_COD_TRAN,
						FK_CONCILIADO,
						FK_USER,
						FECHA_MOVI,
						FECHA_MANUAL,
						TIP_MOVI,
						ORIGEN_MOVI,
						TIPO_MOVI,
						REF_MOVI,
						CONCEP_MOVI,
						NUM_VOUCHER,
						NUM_LOTE,
						CAUSA_MOVI,
						MODULO,
						SECU_MOVI,
						IMPOR_MOVI,
						ESTADO_MOVI,
						PER_BENE_MOVI,
						CONCILIADO,
						FK_COD_EMP,
						IDDET_BOX,
						OBS_MOVI,
						IMPOR_MOVITOTAL,
						FK_ASIENTO,
						FK_AUDITMV,
						FK_ARQUEO,
						ID_TARJETA,
						RECIBO_CAJA,
						FK_CTAM_PLAN,
						NUMERO_UNIDAD,
						JSON_PAGOS
				)
				VALUES ?;`,
				[valuesInsertObligations]
			);

			let nextMoveId = (resultCreateMovement[0] as ResultSetHeader).insertId;
			for (const o of batchObligation) {
				migratedMovementsIdMap[o.FK_AUDITOB] = nextMoveId++;
			}
		}



		for (let i = 0; i < movements.length; i += BATCH_SIZE) {
			const batchMovements = movements.slice(i, i + BATCH_SIZE);
			const accountingValues: any[] = [];
			for (const o of batchMovements) {



				const idMovement = migratedMovementsIdMap[o.auditoria];
				const periodData = await fetchAccountingPeriod(conn,
					{
						fechaManual: o.emision,
						companyId: newCompanyId
					});
				const idPeriod = periodData[0].COD_PERIODO;
				const movementSequence = String(o.secuencia).padStart(9, '0');
				accountingValues.push([
					o.emision,
					'N' + movementSequence + '-Importación',
					'ASICXP-' + movementSequence,
					'CXP',
					o.importe,
					o.importe,
					'IMP-CXP',
					idPeriod,
					o.registro,
					o.registro,
					'[]',
					'US-MIG',
					'IMPORTACION A SISTEMA',
					o.auditoria,
					newCompanyId,
					o.secuencia,
					null,
					idMovement
				]);
			}
			const createAccountingEntries = await conn.query(`
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
				VALUES ?`,
				[accountingValues]
			);
			let nextSeatId = (createAccountingEntries[0] as ResultSetHeader).insertId;
			for (const o of batchMovements) {
				seatIdMap[o.auditoria] = nextSeatId++;
			}
		}

		const seatDetailResult: ResultSet = await legacyConn.query(`
			SELECT
					cda.fk_cod_asiento,
					cda.fk_cod_plan,
					cda.debe_detalle_asiento,
					cda.haber_detalle_asiento
			FROM
					contabilidad_asientos ca
			INNER JOIN contabilidad_detalle_asiento cda ON
					ca.cod_asiento = cda.fk_cod_asiento
			WHERE
					tipo_asiento = 'OBLIGACIONES'
			LIMIT 2;
		`);
		const [rowsSeatDetail]: any[] = seatDetailResult as Array<any>;

		if (rowsSeatDetail.length !== 2) {
			throw new Error("Asiento invalido: debe tener 2 detalles");
		}

		const rowDebe = rowsSeatDetail.find((r: any) => r.debe_detalle_asiento > 0);
		const rowHaber = rowsSeatDetail.find((r: any) => r.haber_detalle_asiento > 0);
		if (!rowDebe || !rowHaber) {
			throw new Error("Asiento invalido: debe tener detalle de debe y haber");
		}

		const accountDebePlan = mapAccounts[rowDebe.fk_cod_plan];
		const accountHaberPlan = mapAccounts[rowHaber.fk_cod_plan];

		let totalDebe = 0;
		let totalHaber = 0;

		for (let i = 0; i < movements.length; i += BATCH_SIZE) {
			const batchMovements = movements.slice(i, i + BATCH_SIZE);
			const setDetailValues: any[] = [];
			const totalsMap = new Map<string, any>();
			for (const o of batchMovements) {
				const idSeat = seatIdMap[o.auditoria];
				if (!idSeat) continue;
				const debe = toNumber(o.importe);
				const haber = toNumber(o.importe);
				setDetailValues.push([
					idSeat,
					0,
					haber,
					accountDebePlan,
					null,
					null
				]);
				setDetailValues.push([
					idSeat,
					debe,
					0,
					accountHaberPlan,
					null,
					null
				]);
				const keyDebe = `${newCompanyId}-${accountDebePlan}-${o.emision}`;
				if (!totalsMap.has(keyDebe)) {
					totalsMap.set(keyDebe, {
						id_plan: accountDebePlan,
						fecha: o.emision,
						debe: 0,
						haber: 0,
						total: 0,
						operacion: "suma"
					});
				}

				const accDebe = totalsMap.get(keyDebe);
				accDebe.debe += debe;
				accDebe.total++;

				const keyHaber = `${newCompanyId}-${accountHaberPlan}-${o.emision}`;
				if (!totalsMap.has(keyHaber)) {
					totalsMap.set(keyHaber, {
						id_plan: accountHaberPlan,
						fecha: o.emision,
						debe: 0,
						haber: 0,
						total: 0,
						operacion: "suma"
					});
				}

				const accHaber = totalsMap.get(keyHaber);
				accHaber.haber += haber;
				accHaber.total++;

				totalDebe += debe;
				totalHaber += haber;
			}
			if (setDetailValues.length === 0) {
				console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
				continue;
			}
			const resultCreateSeatDetail = await conn.query(`
				INSERT INTO accounting_movements_det(
						FK_COD_ASIENTO,
						DEBE_DET,
						HABER_DET,
						FK_CTAC_PLAN,
						FK_COD_PROJECT,
						FK_COD_COST
				)
				VALUES ?`,
				[setDetailValues]
			);
			for (const objTotal of totalsMap.values()) {
				await upsertTotaledEntry(conn, objTotal, newCompanyId);
			}
			console.log(`✅ Batch obligaciones importadas ${i / BATCH_SIZE + 1} procesado`);
		}
		return { migratedMovementsIdMap };
	} catch (error) {
		throw error;
	}
}

async function migrateAccountingEntries({
	legacyConn,
	conn,
	newCompanyId,
	movementIdMap,
	purchaseLiquidationIdMap,
	purchaseLiquidationAuditIdMap,
	mapPeriodo
}: MigrateAccountingEntriesParams): Promise<{
	accountingEntryIdMap: Record<number, number>
}> {
	try {
		const accountingEntryIdMap: Record<number, number> = {};
		const resultAccountingEntries: ResultSet = await legacyConn.query(`
			SELECT
					cod_asiento,
					fecha_asiento AS FECHA_ASI,
					descripcion_asiento AS DESCRIP_ASI,
					numero_asiento AS NUM_ASI,
					origen_asiento AS ORG_ASI,
					debe_asiento AS TDEBE_ASI,
					haber_asiento AS THABER_ASI,
					origen_asiento AS TIP_ASI,
					fk_cod_periodo AS FK_PERIODO,
					fecha_registro_asiento AS FECHA_REG,
					fecha_update_asiento AS FECHA_ACT,
					json_asi AS JSON_ASI,
					res_asiento AS RES_ASI,
					ben_asiento AS BEN_ASI,
					NULL AS FK_AUDIT,
					NULL AS FK_COD_EMP,
					CAST(
							REGEXP_REPLACE(
									RIGHT(numero_asiento, 9),
									'[^0-9]',
									''
							) AS UNSIGNED
					) AS SEC_ASI,
					transacciones.COD_TRAC AS FK_MOVTRAC,
					NULL AS FK_MOV
			FROM
					transacciones
			LEFT JOIN contabilidad_asientos ON contabilidad_asientos.FK_CODTRAC = transacciones.COD_TRAC
			WHERE
					TIP_TRAC IN('Compra', 'liquidacion') AND contabilidad_asientos.descripcion_asiento NOT LIKE '%(RETENCION%'
			ORDER BY
					transacciones.COD_TRAC;
		`);

		const [accountingEntries]: any[] = resultAccountingEntries as Array<any>;
		if (accountingEntries.length === 0) {
			return { accountingEntryIdMap };
		}

		const BATCH_SIZE = 1000;
		for (let i = 0; i < accountingEntries.length; i += BATCH_SIZE) {
			const batchAccountingEntries = accountingEntries.slice(i, i + BATCH_SIZE);
			const accountingEntryValues: any[] = [];
			for (const acc of batchAccountingEntries) {
				const transaccionId = purchaseLiquidationIdMap[acc.FK_MOVTRAC];
				const periodId = mapPeriodo[acc.FK_PERIODO];
				const transAuditId = purchaseLiquidationAuditIdMap[acc.FK_MOVTRAC];
				const movementId = movementIdMap[acc.FK_MOVTRAC];
				accountingEntryValues.push([
					acc.FECHA_ASI,
					acc.DESCRIP_ASI,
					acc.NUM_ASI,
					acc.ORG_ASI,
					acc.TDEBE_ASI,
					acc.THABER_ASI,
					acc.TIP_ASI,
					periodId,
					acc.FECHA_REG,
					acc.FECHA_ACT,
					acc.JSON_ASI,
					acc.RES_ASI,
					acc.BEN_ASI,
					transAuditId,
					newCompanyId,
					acc.SEC_ASI,
					transaccionId,
					movementId
				]);
			}

			const createAccountingEntries: ResultSet = await conn.query(`
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
				VALUES ?
			`, [accountingEntryValues]);

			let nextAccountingEntryId = (createAccountingEntries[0] as ResultSetHeader).insertId;
			batchAccountingEntries.forEach(({ cod_asiento }) => {
				accountingEntryIdMap[cod_asiento] = nextAccountingEntryId++;
			});
		}
		return { accountingEntryIdMap };
	} catch (error) {
		throw error;
	}
}

async function migrateAccountingEntriesDetail({
	legacyConn,
	conn,
	newCompanyId,
	mapProject,
	mapCenterCost,
	mapAccounts,
	accountingEntryIdMap
}: MigrateAccountingEntriesDetailParams): Promise<{
	accountingEntryDetailIdMap: Record<number, number>
}> {
	try {
		const accountingEntryDetailIdMap: Record<number, number> = {};

		const resultAccountingEntriesDetail: ResultSet = await legacyConn.query(`
			SELECT
					d.cod_detalle_asiento,
					a.fecha_asiento,
					a.cod_asiento AS FK_COD_ASIENTO,
					d.debe_detalle_asiento AS DEBE_DET,
					d.haber_detalle_asiento AS HABER_DET,
					d.fk_cod_plan AS FK_CTAC_PLAN,
					d.fkProyectoCosto AS FK_COD_PROJECT,
					d.fkCentroCosto AS FK_COD_COST
			FROM
					transacciones t
			INNER JOIN contabilidad_asientos a ON
					a.FK_CODTRAC = t.COD_TRAC
			INNER JOIN contabilidad_detalle_asiento d ON
					d.fk_cod_asiento = a.cod_asiento
			WHERE
					t.TIP_TRAC IN('Compra', 'liquidacion') AND a.descripcion_asiento NOT LIKE '%(RETENCION%'
			ORDER BY
					t.COD_TRAC;
		`);

		const [accountingEntriesDetails]: any[] = resultAccountingEntriesDetail as Array<any>;
		if (accountingEntriesDetails.length === 0) {
			return { accountingEntryDetailIdMap };
		}
		console.log(`📦 Total detalle asientos a migrar: ${accountingEntriesDetails.length}`);

		const BATCH_SIZE = 1000;
		let totalDebe: number = 0;
		let totalHaber: number = 0;

		for (let i = 0; i < accountingEntriesDetails.length; i += BATCH_SIZE) {
			const batchAccountingEntries = accountingEntriesDetails.slice(i, i + BATCH_SIZE);
			const accountingEntryValues: any[] = [];
			const totalsMap = new Map<string, any>();

			for (const a of batchAccountingEntries) {
				const planId = mapAccounts[a.FK_CTAC_PLAN];
				const projectId = mapProject[a.FK_COD_PROJECT] ?? null;
				const costCenterId = mapCenterCost[a.FK_COD_COST] ?? null;
				const seatCodeId = accountingEntryIdMap[a.FK_COD_ASIENTO] ?? null;

				if (!planId || !seatCodeId) continue;

				const debe = toNumber(a.DEBE_DET);
				const haber = toNumber(a.HABER_DET);

				accountingEntryValues.push([
					seatCodeId,
					debe,
					haber,
					planId,
					projectId,
					costCenterId
				]);

				const key = `${newCompanyId}-${planId}-${a.fecha_asiento}`;
				if (!totalsMap.has(key)) {
					totalsMap.set(key, {
						id_plan: planId,
						fecha: a.fecha_asiento,
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
			if (accountingEntryValues.length === 0) {
				console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
				continue;
			}

			const createAccountingEntryDetails: ResultSet = await conn.query(`
				INSERT INTO accounting_movements_det(
						FK_COD_ASIENTO,
						DEBE_DET,
						HABER_DET,
						FK_CTAC_PLAN,
						FK_COD_PROJECT,
						FK_COD_COST
				)
				VALUES ?
			`, [accountingEntryValues]);
			let nextId = (createAccountingEntryDetails[0] as ResultSetHeader).insertId;
			for (const o of batchAccountingEntries) {
				const planId = mapAccounts[o.FK_CTAC_PLAN];
				const seatCodeId = accountingEntryDetailIdMap[o.FK_COD_ASIENTO];
				if (!planId || !seatCodeId) continue;
				accountingEntryDetailIdMap[o.cod_detalle_asiento] = nextId++;
			};
			for (const t of totalsMap.values()) {
				await upsertTotaledEntry(conn, t, newCompanyId);
			}

			console.log(`✅ Batch detalle contable asiento ${i / BATCH_SIZE + 1} procesado`)
		}
		return { accountingEntryDetailIdMap };
	} catch (error) {
		throw error;
	}
}

// Migrar cobros obligaciones CXP
export async function migratePurchaseObligationDetail({
	legacyConn,
	conn,
	newCompanyId,
	purchaseLiquidationIdMap,
	purchaseLiquidationObligationIdMap,
	bankMap,
	boxMap,
	userMap,
	mapPeriodo,
	mapProject,
	mapCenterCost,
	mapAccounts,
	mapConciliation,
}: MigratePurchaseObligationDetailParams) {
	try {

		console.log("🚀 Migrando movimientos de obligaciones de compra");
		const movementIdMap: Record<number, number> = {};
		const movementAuditIdMap: Record<number, number> = {};

		let nexAuditId = await findNextAuditCode({ conn, companyId: newCompanyId });

		const movementSequenceQuery: ResultSet = await conn.query(`
			SELECT
					IFNULL(MAX(SECU_MOVI) + 1,
					1) AS nextSeq
			FROM
					movements
			WHERE
					MODULO = 'CXP' AND FK_COD_EMP = ?
		`,
			[newCompanyId]
		);
		const [movementData] = movementSequenceQuery as Array<any>;
		let movementSeq = movementData[0]?.nextSeq ?? 1;

		const cardQuery: ResultSet = await conn.query(
			`SELECT ID_TARJETA AS idCard FROM cards WHERE FK_COD_EMP = ? LIMIT 1`,
			[newCompanyId]
		);
		const [cardData]: any[] = cardQuery as Array<any>;
		const cardId = cardData[0].idCard ?? null;

		const accountPlanQuery: ResultSet = await conn.query(
			`SELECT ID_PLAN, CODIGO_PLAN FROM account_plan WHERE FK_COD_EMP = ?`,
			[newCompanyId]
		);
		const [chartOfAccounts]: any[] = accountPlanQuery as Array<any>;

		const accountMap = new Map(
			chartOfAccounts.map((a: any) => [a.CODIGO_PLAN, a.ID_PLAN])
		);

		const [obligationDetails]: any[] = await legacyConn.query(`
			SELECT
					detalles_cuentas.fk_cod_cuenta,
					detalles_cuentas.FK_COD_GD,
					detalles_cuentas.fk_cod_cli_c,
					detalles_cuentas.Tipo_cp,
					detalles_cuentas.fecha,
					detalles_cuentas.importe,
					detalles_cuentas.saldo,
					IFNULL(
							movimientos.IMPOR_MOVI,
							detalles_cuentas.importe
					) AS IMPOR_MOVI,
					grupo_detalles_t.IMPORTE_GD AS IMPOR_MOVITOTAL,
					detalles_cuentas.saldo,
					CASE detalles_cuentas.forma_pago_cp WHEN 1 THEN 'EFECTIVO' WHEN 2 THEN 'CHEQUE' WHEN 3 THEN 'TRANSFERENCIA' WHEN 5 THEN 'TARJETA' WHEN 7 THEN 'ANTICIPO' WHEN 8 THEN 'CCTACONT' WHEN 16 THEN 'RET-VENTA' WHEN 17 THEN 'NOTA DE CREDITO' ELSE CAST(
							detalles_cuentas.forma_pago_cp AS CHAR
					)
			END AS forma,
			IFNULL(movimientos.PER_BENE_MOVI, 'MIG') AS PER_BENE_MOVI,
			detalles_cuentas.fk_cod_cajas,
			detalles_cuentas.fk_cod_banco,
			detalles_cuentas.fk_cod_Vemp,
			detalles_cuentas.NUM_VOUCHER,
			detalles_cuentas.NUM_LOTE,
			movimientos.ORIGEN_MOVI,
			IFNULL(
					movimientos.FECHA_MOVI,
					detalles_cuentas.FECH_REG
			) AS FECHA_MOVI,
			IFNULL(
					movimientos.REF_MOVI,
					detalles_cuentas.documento_cp
			) AS REF_MOVI,
			IFNULL(
					movimientos.FECHA_MANUAL,
					detalles_cuentas.fecha
			) AS FECHA_MANUAL,
			movimientos.FK_CONCILIADO,
			movimientos.CONCILIADO,
			movimientos.ID_MOVI,
			CASE WHEN movimientos.ESTADO_MOVI = 'ACTIVO' THEN 1 ELSE 0
			END AS ESTADO_MOVI,
			IFNULL(movimientos.CAUSA_MOVI, 'EGRESO') AS CAUSA_MOVI,
			movimientos.TIP_MOVI,
			movimientos.TIPO_MOVI,
			NULL AS FK_ASIENTO,
			NULL AS FK_ARQUEO,
			NULL AS RECIBO_CAJA,
			NULL AS NUM_UNIDAD,
			NULL AS JSON_PAGOS,
			'CXP' AS MODULO,
			grupo_detalles_t.FECH_REG,
			IFNULL(
					movimientos.CONCEP_MOVI,
					detalles_cuentas.observacion_cp
			) AS OBS_MOVI,
			IFNULL(
					movimientos.CONCEP_MOVI,
					detalles_cuentas.observacion_cp
			) AS CONCEP_MOVI,
			grupo_detalles_t.SECU_CXC AS SECU_MOVI,
			movimientos.FK_COD_CAJAS_MOVI,
			movimientos.FK_COD_BANCO_MOVI,
			movimientos.FK_TRAC_MOVI
			FROM
					cuentascp
			INNER JOIN detalles_cuentas ON cuentascp.cod_cp = detalles_cuentas.fk_cod_cuenta
			INNER JOIN grupo_detalles_t ON detalles_cuentas.FK_COD_GD = grupo_detalles_t.ID_GD
			LEFT JOIN movimientos ON movimientos.FK_COD_CX = grupo_detalles_t.ID_GD
			WHERE
					cuentascp.Tipo_cxp = 'CXP' AND forma_pago_cp NOT IN('17', '16')
			GROUP BY
					detalles_cuentas.FK_COD_GD
			ORDER BY
					cod_detalle
			DESC;`);

		if (obligationDetails.length === 0) {
			return { movementIdMap, movementAuditIdMap };
		}
		const BATCH_SIZE = 1500;

		for (let i = 0; i < obligationDetails.length; i += BATCH_SIZE) {
			const obligationBatch = obligationDetails.slice(i, i + BATCH_SIZE);

			const auditValues = obligationBatch.map((o) => [
				nexAuditId++,
				o.forma,
				newCompanyId,
			]);

			const resultCreateAudit: ResultSet = await conn.query(
				`INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
				[auditValues]
			);
			const firstAuditId = (resultCreateAudit[0] as ResultSetHeader).insertId;

			const movementValues = obligationBatch.map((obl: any, index: number) => {
				const auditId = firstAuditId + index;
				movementAuditIdMap[obl.FK_COD_GD] = auditId;

				let modulo = "CXP";
				let origen = "CXP";
				let causa = "EGRESO";
				let tipMovi = obl.TIP_MOVI;
				let tipoMovi = obl.TIPO_MOVI;
				let idPlanCuenta = null;

				if (obl.forma === "CCTACONT") {
					const codigoPlan = obl.REF_MOVI?.split("--")[0].trim();
					idPlanCuenta = accountMap.get(codigoPlan) || null;
					tipMovi = tipoMovi = "CCTACONT";
				} else if (obl.forma === "RET-VENTA") {
					tipMovi = tipoMovi = origen = "RET-VENTA";
					modulo = "RETENCION-VENTA";
				} else if (obl.forma === "NOTA DE CREDITO") {
					tipMovi = tipoMovi = "CREDITO";
					modulo = "NCCOMPRA";
					origen = "NOTA CREDITO COMPRA";
				}

				const importMovi = Math.abs(obl.IMPOR_MOVI);
				const importMoviTotal = Math.abs(obl.IMPOR_MOVITOTAL);
				return [
					bankMap[obl.FK_COD_BANCO_MOVI] ?? null,
					purchaseLiquidationIdMap[obl.FK_TRAC_MOVI] ?? null,
					mapConciliation[obl.FK_CONCILIADO] ?? null,
					userMap[obl.fk_cod_Vemp] ?? null,
					obl.FECHA_MOVI,
					obl.FECHA_MANUAL,
					tipMovi,
					origen,
					tipoMovi,
					obl.REF_MOVI,
					obl.CONCEP_MOVI,
					obl.NUM_VOUCHER,
					obl.NUM_LOTE,
					causa,
					modulo,
					movementSeq++,
					importMovi,
					obl.ESTADO_MOVI,
					obl.PER_BENE_MOVI,
					obl.CONCILIADO ?? 0,
					newCompanyId,
					boxMap[obl.FK_COD_CAJAS_MOVI] ?? null,
					obl.OBS_MOVI,
					importMoviTotal,
					null, // FK_ASIENTO
					auditId,
					null, // FK_ARQUEO
					obl.forma === "TARJETA" ? cardId : null,
					null, // RECIBO_CAJA
					idPlanCuenta,
					null, // NUM_UNIDAD
					"[]", // JSON_PAGOS
				];
			});
			const resultCreateMovement = await conn.query(`
				INSERT INTO movements(
						FKBANCO,
						FK_COD_TRAN,
						FK_CONCILIADO,
						FK_USER,
						FECHA_MOVI,
						FECHA_MANUAL,
						TIP_MOVI,
						ORIGEN_MOVI,
						TIPO_MOVI,
						REF_MOVI,
						CONCEP_MOVI,
						NUM_VOUCHER,
						NUM_LOTE,
						CAUSA_MOVI,
						MODULO,
						SECU_MOVI,
						IMPOR_MOVI,
						ESTADO_MOVI,
						PER_BENE_MOVI,
						CONCILIADO,
						FK_COD_EMP,
						IDDET_BOX,
						OBS_MOVI,
						IMPOR_MOVITOTAL,
						FK_ASIENTO,
						FK_AUDITMV,
						FK_ARQUEO,
						ID_TARJETA,
						RECIBO_CAJA,
						FK_CTAM_PLAN,
						NUMERO_UNIDAD,
						JSON_PAGOS
				)
				VALUES ?
			`,
				[movementValues]
			);
			let nextMovementId = (resultCreateMovement[0] as ResultSetHeader).insertId;
			obligationBatch.forEach(({ FK_COD_GD }) => {
				movementIdMap[FK_COD_GD] = nextMovementId++;
			});
			console.log(` -> Batch migrado: ${obligationBatch.length} movimiento de compras y liquidaciones`);
		}

		const { detailObligationIdMap } = await migrateDetailsOfObligations({
			legacyConn,
			conn,
			purchaseLiquidationObligationIdMap,
			movementIdMap,
		});
		console.log(
			"Movimientos de cobros realizados:",
			Object.keys(movementIdMap).length
		);
		console.log("Detalle migrado:", Object.keys(detailObligationIdMap).length);
		console.log("✅ Migración de cobros completadas");

		const { accountingEntriesIdMap } = await migrateObligationJournalEntries({
			legacyConn,
			conn,
			newCompanyId,
			movementIdMap,
			mapPeriodo,
			movementAuditIdMap,
		});

		console.log(
			"Encabezado de asiento contable migrados CXP:",
			Object.keys(accountingEntriesIdMap).length
		);

		const { accountingEntryDetailsIdMap } = await migrateObligationJournalEntryDetails({
			legacyConn,
			conn,
			newCompanyId,
			mapProject,
			mapCenterCost,
			mapAccounts,
			accountingEntriesIdMap,
		});
		console.log("Detalle de asientos contables migrados CXP:", Object.keys(accountingEntryDetailsIdMap).length);
		return { movementIdMap, movementAuditIdMap };
	} catch (error) {
		throw error;
	}
}

// Migrar detalles de cobros obligaciones CXP
async function migrateDetailsOfObligations({
	legacyConn,
	conn,
	purchaseLiquidationObligationIdMap,
	movementIdMap,
}: MigrateDetailsOfObligationsParams): Promise<{
	detailObligationIdMap: Record<number, number>;
}> {
	try {
		const detailObligationIdMap: Record<number, number> = {};

		const [detailsObligations]: any[] = await legacyConn.query(`
			SELECT
					detalles_cuentas.fk_cod_cuenta,
					detalles_cuentas.FK_COD_GD,
					detalles_cuentas.fecha,
					detalles_cuentas.importe,
					detalles_cuentas.saldo,
					detalles_cuentas.nuevo_saldo
			FROM
					cuentascp
			INNER JOIN detalles_cuentas ON cuentascp.cod_cp = detalles_cuentas.fk_cod_cuenta
			INNER JOIN grupo_detalles_t ON detalles_cuentas.FK_COD_GD = grupo_detalles_t.ID_GD
			LEFT JOIN movimientos ON movimientos.FK_COD_CX = grupo_detalles_t.ID_GD
			WHERE
					cuentascp.Tipo_cxp = 'CXP' AND detalles_cuentas.forma_pago_cp NOT IN('17', '16')
			ORDER BY
					cod_detalle
			DESC;
		`);
		if (detailsObligations.length === 0) {
			return { detailObligationIdMap };
		}
		const BATCH_SIZE = 1500;

		for (let i = 0; i < detailsObligations.length; i += BATCH_SIZE) {
			const batchObligations = detailsObligations.slice(i, i + BATCH_SIZE);
			const movementValues = batchObligations.map((o: any, index: number) => {
				const idCuenta = purchaseLiquidationObligationIdMap[o.fk_cod_cuenta];
				const idMovimiento = movementIdMap[o.FK_COD_GD];
				return [
					idCuenta,
					idMovimiento,
					o.fecha,
					o.importe,
					o.saldo,
					o.nuevo_saldo,
				];
			});
			const resultCreateDetail: ResultSet = await conn.query(
				`INSERT INTO account_detail (
						FK_COD_CUENTA, FK_ID_MOVI, FECHA_REG, IMPORTE, SALDO, NEW_SALDO
				) VALUES ?`,
				[movementValues]
			);
			let nextDetailId = (resultCreateDetail[0] as ResultSetHeader).insertId;
			batchObligations.forEach(({ FK_COD_GD }) => {
				detailObligationIdMap[FK_COD_GD] = nextDetailId++;
			});
		}
		console.log("✅ Migración completada");
		return { detailObligationIdMap };
	} catch (error) {
		throw error;
	}
}

// Migrar asientos contables de cobros obligaciones CXP
async function migrateObligationJournalEntries({
	legacyConn,
	conn,
	newCompanyId,
	movementIdMap,
	mapPeriodo,
	movementAuditIdMap,
}: MigrateAccountingEntryObligationsParams): Promise<{
	accountingEntriesIdMap: Record<number, number>;
}> {
	try {
		console.log(
			"🚀 Migrando encabezado de asiento contables cobros obligaciones CXP"
		);
		const accountingEntriesIdMap: Record<number, number> = {};

		const [accountingEntries]: any[] = await legacyConn.query(`
			SELECT
					cod_asiento,
					fecha_asiento AS FECHA_ASI,
					descripcion_asiento AS DESCRIP_ASI,
					numero_asiento AS NUM_ASI,
					origen_asiento AS ORG_ASI,
					debe_asiento AS TDEBE_ASI,
					haber_asiento AS THABER_ASI,
					origen_asiento AS TIP_ASI,
					fk_cod_periodo AS FK_PERIODO,
					fecha_registro_asiento AS FECHA_REG,
					fecha_update_asiento AS FECHA_ACT,
					json_asi AS JSON_ASI,
					res_asiento AS RES_ASI,
					ben_asiento AS BEN_ASI,
					NULL AS FK_AUDIT,
					NULL AS FK_COD_EMP,
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
			WHERE
					origen_asiento = 'CXP' AND tipo_asiento <> 'OBLIGACIONES';
		`);
		if (accountingEntries.length === 0) {
			return { accountingEntriesIdMap };
		}
		const BATCH_SIZE = 1000;
		for (let i = 0; i < accountingEntries.length; i += BATCH_SIZE) {
			const batchAccountingEntries = accountingEntries.slice(i, i + BATCH_SIZE);
			const accountingEntriesValues = batchAccountingEntries.map(
				(o: any, index: number) => {
					const periodId = mapPeriodo[o.FK_PERIODO];
					const transAuditId = movementAuditIdMap[o.cod_origen];
					const movementId = movementIdMap[o.cod_origen];
					return [
						o.FECHA_ASI,
						o.DESCRIP_ASI,
						o.NUM_ASI,
						o.ORG_ASI,
						o.TDEBE_ASI,
						o.THABER_ASI,
						o.TIP_ASI,
						periodId,
						o.FECHA_REG,
						o.FECHA_ACT,
						o.JSON_ASI,
						o.RES_ASI,
						o.BEN_ASI,
						transAuditId,
						newCompanyId,
						o.SEC_ASI,
						null,
						movementId,
					];
				}
			);
			const resultCreateAccountingEntries: ResultSet = await conn.query(`
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
				VALUES ?
				`,
				[accountingEntriesValues]
			);
			let nextId = (resultCreateAccountingEntries[0] as ResultSetHeader).insertId;
			batchAccountingEntries.forEach(({ cod_asiento }) => {
				accountingEntriesIdMap[cod_asiento] = nextId++;
			});
		}
		console.log("✅ Migración de asiento contable completada CXP");
		return { accountingEntriesIdMap };
	} catch (error) {
		throw error;
	}
}

// Migrar detalles de asientos contables de cobros obligaciones CXP
async function migrateObligationJournalEntryDetails({
	legacyConn,
	conn,
	newCompanyId,
	mapProject,
	mapCenterCost,
	mapAccounts,
	accountingEntriesIdMap,
}: MigrateObligationJournalEntryDetailsParams): Promise<{
	accountingEntryDetailsIdMap: Record<number, number>;
}> {
	try {
		console.log(
			"🚀 Migrando detalle de asiento contable cobros obligaciones CXP"
		);
		const accountingEntryDetailsIdMap: Record<number, number> = {};

		const [accountingEntryDetails]: any[] = await legacyConn.query(`
			SELECT
					d.cod_detalle_asiento,
					a.fecha_asiento,
					a.cod_asiento AS FK_COD_ASIENTO,
					d.debe_detalle_asiento AS DEBE_DET,
					d.haber_detalle_asiento AS HABER_DET,
					d.fk_cod_plan AS FK_CTAC_PLAN,
					d.fkProyectoCosto AS FK_COD_PROJECT,
					d.fkCentroCosto AS FK_COD_COST
			FROM
					contabilidad_asientos a
			INNER JOIN contabilidad_detalle_asiento d ON
					d.fk_cod_asiento = a.cod_asiento
			WHERE
					origen_asiento = 'CXP' AND tipo_asiento <> 'OBLIGACIONES';
		`);

		if (accountingEntryDetails.length === 0) {
			return { accountingEntryDetailsIdMap };
		}
		const BATCH_SIZE = 1000;
		let totalDebe = 0;
		let totalHaber = 0;
		for (let i = 0; i < accountingEntryDetails.length; i += BATCH_SIZE) {
			const batchAccountingEntryDetails = accountingEntryDetails.slice(i, i + BATCH_SIZE);
			const insertValues = [];
			const totalsMap = new Map<string, any>();

			for (const o of batchAccountingEntryDetails) {
				const planId = mapAccounts[o.FK_CTAC_PLAN];
				const projectId = mapProject[o.FK_COD_PROJECT] || null;
				const costId = mapCenterCost[o.FK_COD_COST] || null;
				const entryId = accountingEntriesIdMap[o.FK_COD_ASIENTO] || null;

				if (!planId || !entryId) continue;
				const debe = Number(o.DEBE_DET) || 0;
				const haber = Number(o.HABER_DET) || 0;

				insertValues.push([entryId, debe, haber, planId, projectId, costId]);

				const key = `${newCompanyId}-${planId}-${o.fecha_asiento}`;

				if (!totalsMap.has(key)) {
					totalsMap.set(key, {
						id_plan: planId,
						fecha: o.fecha_asiento,
						debe: 0,
						haber: 0,
						total: 0,
						operacion: "suma",
					});
				}

				const acc = totalsMap.get(key);
				acc.debe += debe;
				acc.haber += haber;
				acc.total++;

				totalHaber += haber;
				totalDebe += debe;
			}
			if (insertValues.length === 0) {
				console.warn(`⚠️ Batch ${i / BATCH_SIZE + 1} sin registros válidos`);
				continue;
			}
			const resultCreateDetail: ResultSet = await conn.query(`
				INSERT INTO accounting_movements_det(
						FK_COD_ASIENTO,
						DEBE_DET,
						HABER_DET,
						FK_CTAC_PLAN,
						FK_COD_PROJECT,
						FK_COD_COST
				)
				VALUES ?
			`,
				[insertValues]
			);
			let nextId = (resultCreateDetail[0] as ResultSetHeader).insertId;
			for (const o of batchAccountingEntryDetails) {
				const idPlan = mapAccounts[o.FK_CTAC_PLAN];
				console.log(`➡️ Procesando detalle de asiento CXP  ${idPlan}`);
				const idCodAsiento = accountingEntriesIdMap[o.FK_COD_ASIENTO];
				if (!idPlan || !idCodAsiento) continue;
				accountingEntryDetailsIdMap[o.cod_detalle_asiento] = nextId++;
			}
			for (const t of totalsMap.values()) {
				await upsertTotaledEntry(conn, t, newCompanyId);
			}
			console.log(`✅ Batch ${i / BATCH_SIZE + 1} procesado`);
		}
		console.log("🎉 Migración de detalles de asiento contable cobros obligaciones CXP completada");
		return { accountingEntryDetailsIdMap };
	} catch (error) {
		throw error;
	}
}