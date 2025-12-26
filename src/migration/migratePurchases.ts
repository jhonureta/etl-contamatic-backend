import { Connection, FieldPacket, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { restructureProductDetails, restructureRetentionDetail, RetentionCodeValue, toJSONArray } from "./purchaseHelpers";

type MigratePurchasesParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	branchMap: Record<number, number>;
	userMap: Record<number, number | null>;
	mapSuppliers: Record<number, number>;
	mapProducts: Record<number, number>;
	mapRetentions: Record<number, number>;
	retentionsByCode: Map<string, RetentionCodeValue>;
	mapCostExpenses: Record<number, number | null>;
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
	purchaseAuditIdMap: Record<number, number>;
	purchasesIdMap: Record<number, number>;
	mapSuppliers: Record<number, number>;
	branchMap: Record<number, number>;
	userMap: Record<number, number>;
};

type MigrateObligationsParams = Omit<
	MigrateMovementsParams,
	'branchMap' | 'userMap'
>;

export async function migratePurchases({
	legacyConn,
	conn,
	newCompanyId,
	branchMap,
	userMap,
	mapSuppliers,
	mapProducts,
	mapRetentions,
	retentionsByCode,
	mapCostExpenses
}: MigratePurchasesParams): Promise<{ purchasesIdMap: Record<number, number>; purchaseAuditIdMap: Record<number, number> }> {

	const resultPurchasesQuery: ResultSet = await legacyConn.query(`
		SELECT COD_TRAC AS COD_TRANS,
			CONCAT(SUBSTRING(secuencialFacturaRet, 1, 3), '-', SUBSTRING(secuencialFacturaRet, 4, 3)) AS PUNTO_EMISION_DOC,
			SUBSTR(secuencialFacturaRet, 7) AS SECUENCIA_DOC,
			CASE
					WHEN secuencialCompraFisica <> '--' THEN SUBSTRING_INDEX(secuencialCompraFisica, '-', -1)
					WHEN RET_AUTCOMP IS NOT NULL THEN SUBSTRING_INDEX(RET_AUTCOMP, '-', -1)
			END AS SECUENCIA_REL_DOC,
			claveFacturaRet AS CLAVE_TRANS,
			CASE
					WHEN secuencialCompraFisica <> '--' THEN claveCompraFisica
					WHEN RET_AUTCOMP IS NOT NULL THEN claveFactura
			END AS CLAVE_REL_TRANS,
			CASE tipo_compra
					WHEN 'Mercaderia' THEN 'mercaderia'
					WHEN 'Gastos y Servicios' THEN 'gastos_servicios'
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
			estado_compra AS ESTADO,
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
			'01' AS COD_COMPROBANTE,
			CASE
					WHEN secuencialCompraFisica <> '--' THEN '07'
					WHEN RET_AUTCOMP IS NOT NULL THEN '07'
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
			CASE
					WHEN secuencialCompraFisica <> '--' THEN 'fisica'
					WHEN RET_AUTCOMP IS NOT NULL THEN 'electronica'
			END AS TIP_DOC_REL,
			DES_TRAC AS DSTO_TRAC,
			NULL AS FK_CODSUCURSAL,
			NULL AS FK_AUDITTR,
			'compra' AS TIP_TRAC,
			fecha AS FECHA_REG,
			DET_TRACCOM AS DOCUMENT_DETAIL,
			CASE
					WHEN secuencialCompraFisica <> '--' THEN SUBSTRING_INDEX(secuencialCompraFisica, '-', 2)
					WHEN RET_AUTCOMP IS NOT NULL THEN SUBSTRING_INDEX(RET_AUTCOMP, '-', 2)
			END AS PUNTO_EMISION_REC,
			NULL AS FK_DOC_REL,
			fechaAnulacion AS FECHA_ANULACION,
			'electronica' AS TIP_DOC,
			NULL AS SRI_PAY_CODE,
			IP_CLIENTE AS CLIENT_IP,
			NULL AS FK_AUDIT_REL,
			CONCAT(SUBSTRING(secuencialFacturaRet, 1, 3), '-', SUBSTRING(secuencialFacturaRet, 4, 3), '-', SUBSTRING(secuencialFacturaRet, 7)) AS NUM_TRANS,
			CASE
					WHEN secuencialCompraFisica <> '--' THEN secuencialCompraFisica
					WHEN RET_AUTCOMP IS NOT NULL THEN RET_AUTCOMP
			END AS SECUENCIA_REL_DOC,
			serial_producto AS DIV_PAY_YEAR,
			DET_TRAC AS DOCUMENT_REL_DETAIL,
			RESPU_SRI AS RESP_SRI,
			INFO_ADIC AS INFO_ADIC,
			DET_REMBOLSO AS DET_EXP_REEMBOLSO,
			METPAG_JSON_TRAC AS JSON_METODO,
			INFO_ADIC AS ITEMS_PROF,
			OBS_AUXILIAR AS OBS_AUXILIAR,
			OBS_ORD AS OBS_ORDEN
		FROM transacciones
		WHERE transacciones.TIP_TRAC = 'Compra'
		ORDER BY COD_TRAC DESC;
	`);
	const [purchases]: any[] = resultPurchasesQuery as Array<any>;
	if (purchases.length === 0) {
		throw new Error(" -> No existen registros de compras para migrar");
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

	let idFirstBranch: number | null = null;
	if (sequentialBranches.length > 0) {
		idFirstBranch = sequentialBranches[0].COD_SURC;
	}

	const electronicSequences = new Map<string, number>();
	sequentialBranches.forEach((branch: BranchSequentialData, index: number) => {
		electronicSequences.set(branch.ELECTRONICA, branch.COD_SURC);
	});

	const auditQuery = `SELECT IFNULL(MAX(CODIGO_AUT +0) +1, 1) AS auditId FROM audit WHERE FK_COD_EMP = ?;`;
	const auditQueryResult: ResultSet = await conn.query(auditQuery, [newCompanyId]);
	const [auditData]: any[] = auditQueryResult as Array<any>;
	const auditId = auditData[0].auditId;

	const BATCH_SIZE: number = 1000;
	const purchasesIdMap: Record<number, number> = {};
	const purchaseAuditIdMap: Record<number, number> = {};

	for (let i = 0; i < purchases.length; i += BATCH_SIZE) {

		const batchPurchase = purchases.slice(i, i + BATCH_SIZE);
		const auditValues = batchPurchase.map((purchase: any, index: number) => {
			const auditIdIsert = auditId + index;
			return [
				auditIdIsert,
				'COMPRAS',
				newCompanyId
			]
		});

		const resultCreateAudit: ResultSet = await conn.query(
			`INSERT INTO audit (CODIGO_AUT, MOD_AUDIT, FK_COD_EMP) VALUES ?`,
			[auditValues]
		);

		const firstAuditId = (resultCreateAudit[0] as ResultSetHeader).insertId;
		const purchaseValues = batchPurchase.map((p: any, index: number) => {
			console.log(`transformando y normalizando ${p.NUM_TRANS}`);

			const codTrans = p.COD_TRANS;
			const purchaseType = p.TIP_DET_DOC;
			const userId = userMap[p.FK_USER];// id usuario
			const sellerId = userMap[p.FK_USER];// id vendedor
			const supplierId = mapSuppliers[p.FK_PERSON];// id proveedor
			const auditId = firstAuditId + index;
			// Mapeo de auditoria por transaccion
			purchaseAuditIdMap[codTrans] = auditId;

			const previusDetailProd = toJSONArray(p.DOCUMENT_DETAIL);
			const detailProducts = restructureProductDetails({
				idFirstBranch,
				branchMap,
				purchaseType,
				inputDetail: previusDetailProd,
				mapCostExpenses,
				mapProducts
			});

			const previusDetailRet = toJSONArray(p.DOCUMENT_REL_DETAIL);
			const structureRetention = restructureRetentionDetail({
				inputDetail: previusDetailRet,
				mapRetentions,
				retentionsByCode
			});

			let branchId: number = idFirstBranch;
			if (electronicSequences.has(p.PUNTO_EMISION_REC)) {
				branchId = electronicSequences.get(p.PUNTO_EMISION_REC);
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
				p.MET_PAG_TRAC,
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
				p.PROPINA_TRAC,
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
				p.SRI_PAY_CODE,
				p.CLIENT_IP,
				p.FK_AUDIT_REL,
				p.NUM_TRANS,
				p.NUM_REL_DOC,
				p.DIV_PAY_YEAR,
				JSON.stringify(structureRetention),
				p.RESP_SRI,
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
			purchasesIdMap[COD_TRANS] = nextPurchaseId++;
		});
	}

	return { purchasesIdMap, purchaseAuditIdMap };
}

export async function migratePurchaseMovements({
	legacyConn,
	conn,
	newCompanyId,
	branchMap,
	userMap,
	mapSuppliers,
	purchaseAuditIdMap,
	purchasesIdMap
}: MigrateMovementsParams) {
	console.log("🧩 Iniciando migración movimientos compra");

	await migratePurchaseObligations({
		legacyConn,
		conn,
		newCompanyId,
		purchasesIdMap,
		mapSuppliers,
		purchaseAuditIdMap
	})

}

async function migratePurchaseObligations({
	legacyConn,
	conn,
	newCompanyId,
	mapSuppliers,
	purchasesIdMap,
	purchaseAuditIdMap
}: MigrateObligationsParams) {

	console.log("🚀 Migrando obligaciones compra");
	const purchaseObligationIdMap: Record<number, number> = {};
	const purchaseObligationAuditIdMap: Record<number, number> = {};

	const auditQuery = `SELECT IFNULL(MAX(CODIGO_AUT +0) +1, 1) AS auditId FROM audit WHERE FK_COD_EMP = ?;`;
	const auditQueryResult: ResultSet = await conn.query(auditQuery, [newCompanyId]);
	const [auditData]: any[] = auditQueryResult as Array<any>;
	const auditId = auditData[0].auditId;

	const resultObligationsQuery: ResultSet = await legacyConn.query(`
		SELECT
				cp.cod_cp AS old_id,
				cp.fk_cod_prov_cp AS fk_persona_old,
				cp.FK_TRAC_CUENTA AS fk_cod_trans_old,
				cp.OBL_ASI AS obl_asi_group,
				cp.Tipo_cxp AS tipo_obl,
				cp.fecha_emision_cxp AS fech_emision,
				cp.fecha_vence_cxp AS fech_vencimiento,
				cp.tipo_documento AS tip_doc,
				cp.estado_cxp AS estado,
				cp.saldo_cxp AS saldo,
				cp.valor_cxp AS total,
				cp.referencia_cxp AS ref_secuencia,
				cp.TIPO_CUENTA AS tipo_cuenta,
				cp.FK_SERVICIO AS fk_id_infcon,
				cp.TIPO_ESTADO_CUENTA AS tipo_estado_cuenta,
				t.TIP_TRAC AS tipoTransaccion
		FROM
				cuentascp cp
		LEFT JOIN transacciones t ON
				t.COD_TRAC = cp.FK_TRAC_CUENTA
		WHERE
				cp.Tipo_cxp = 'CXP' AND(
						t.TIP_TRAC = 'Compra' OR cp.tipo_cuenta = 'Importado'
				)
		ORDER BY
				cp.cod_cp;
	`);
	const [obligations]: any[] = resultObligationsQuery as Array<any>;
	if (obligations.length === 0) {
		return { purchaseObligationIdMap, purchaseObligationAuditIdMap };
	}

	const oblAsiToAuditId: Record<number, number> = {};
	const BATCH_SIZE: number = 500;
	let nexAuditId = auditId;
	let importedAuditId: number | null = null;

	for (let i = 0; i < obligations.length; i += BATCH_SIZE) {
		const batchObligation = obligations.slice(i, i + BATCH_SIZE);
		const valuesInsertObligations: any[] = [];
		for (const o of batchObligation) {
			let auditIdInsert: number | null = null;
			if (o.fk_cod_trans_old && purchaseAuditIdMap[o.fk_cod_trans_old]) {
				auditIdInsert = purchaseAuditIdMap[o.fk_cod_trans_old];
			} else if (o.obl_asi_group !== null) {
				if (!oblAsiToAuditId[o.obl_asi_group]) {
					const codigoAut = nexAuditId++;
					const resultCreateAudit: ResultSet = await conn.query(`
						INSERT INTO audit(
							CODIGO_AUT,
							MOD_AUDIT,
							FK_COD_EMP
						)
						VALUES(?, 'VENTAS', ?)`,
						[codigoAut, newCompanyId]
					);
					const auditIdInserted = (resultCreateAudit[0] as ResultSetHeader).insertId;
					oblAsiToAuditId[o.obl_asi_group] = auditIdInserted;
				}
				auditIdInsert = oblAsiToAuditId[o.obl_asi_group];
			} else if (o.tipo_cuenta === 'Importado') {
				if (!importedAuditId) {
					const codigoAut = nexAuditId++;
					const resultCreateAudit: ResultSet = await conn.query(`
						INSERT INTO audit(
								CODIGO_AUT,
								MOD_AUDIT,
								FK_COD_EMP
						)
						VALUES(?, 'VENTAS', ?)`,
						[codigoAut, newCompanyId]
					);
					const auditIdInserted = (resultCreateAudit[0] as ResultSetHeader).insertId;
					importedAuditId = auditIdInserted;
				}
				auditIdInsert = importedAuditId;
			}
			if (!auditIdInsert) {
				throw new Error(`No se pudo resolver AUDIT para obligación ${o.old_id}`);
			}
			purchaseObligationAuditIdMap[o.old_id] = auditId;
			
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
				purchasesIdMap[o.fk_cod_trans_old] ?? null,
				o.tipo_cuenta,
				o.fk_id_infcon,
				o.tipo_estado_cuenta,
				auditId,
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
		batchObligation.forEach((o) => {
			purchaseObligationIdMap[o.old_id] = nextPurchaseId++;
		});
	}
	return { purchaseObligationIdMap, purchaseObligationAuditIdMap };
}