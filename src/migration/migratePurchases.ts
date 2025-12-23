import { Connection, FieldPacket, ResultSetHeader, RowDataPacket } from "mysql2/promise";
import { restructureProductDetails, restructureRetentionDetail, RetentionCodeValue, toJSONArray } from "./purchaseHelpers";

type MigratePurchasesParams = {
	legacyConn: Connection;
	conn: Connection;
	newCompanyId: number;
	branchMap: Record<number, number>;
	userMap: any;
	mapClients: any;
	mapProducts: Record<number, number>;
	mapRetentions: any;
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

export async function migratePurchases({
	legacyConn,
	conn,
	newCompanyId,
	branchMap,
	userMap,
	mapClients,
	mapProducts,
	mapRetentions,
	retentionsByCode,
	mapCostExpenses
}: MigratePurchasesParams): Promise<void> {

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

	// Asignacion de id primera sucursal (default)
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
	const purchasesMap: Record<number, number> = {};
	const auditCodesMap: Record<number, number> = {};

	for (let i = 0; i < purchases.length; i += BATCH_SIZE) {

		const batch = purchases.slice(i, i + BATCH_SIZE);

		const auditValues = batch.map((purchase: any, index: number) => {
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

		const purchaseValues = batch.map((p: any, index: number) => {
			console.log(`transformando y normalizando ${p.NUM_TRANS}`);
			const codTrans = p.COD_TRANS;
			const purchaseType = p.TIP_DET_DOC;
			//== Mapeo de Auditoria con transaccion ==/
			const auditId = firstAuditId + index;
			auditCodesMap[codTrans] = auditId;

			const previusDetailProd = toJSONArray(p.DOCUMENT_DETAIL);
			const detailProducts = restructureProductDetails({
				idFirstBranch,
				branchMap,
				purchaseType,
				inputDetail: previusDetailProd,
				mapCostExpenses,
				mapProducts
			})

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
			console.log(JSON.stringify(structureRetention));
			console.log('*********** DETALLE DE PRODUCTO ***********');
			console.log(JSON.stringify(detailProducts));

		});

	}

}