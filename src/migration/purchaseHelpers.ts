import { Connection, FieldPacket, ResultSetHeader, RowDataPacket } from "mysql2/promise";

type ResultSet = [RowDataPacket[] | RowDataPacket[][] | ResultSetHeader, FieldPacket[]];

export interface RetentionCodeValue {
  id: number;
  name: string;
}

type RestructureRetentionParams = {
  inputDetail: any[];
  oldRetentionCodeMap: Map<string, RetentionCodeValue>;
  newRetentionIdMap: Record<number, number>;
};

type RestructureProductParams = {
  idFirstBranch: number;
  purchaseType: string;
  inputDetail: any[];
  branchMap: Record<number, number>;
  mapProducts: Record<number, number>;
  mapCostExpenses: Record<number, number | null>;
};

type InsertAuditParams = {
  conn: Connection;
  codigoAudit: number;
  module: string;
  companyId: number
}
type FindNextAdutiCodeParams = {
  conn: Connection;
  companyId: number
}

type FindFirstDefaultUserParams = {
  conn: Connection;
  companyId: number
}
type FindFirstDefaultCustomerParams = {
  conn: Connection;
  companyId: number
}

export const codigoIvaPorcentaje: Record<string, number> = {
  '12': 2,
  '0': 0,
  '14': 3,
  '15': 4,
  '5': 5,
  '13': 10
}

export const toNumber = (v: unknown, fallback = 0): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
};

export const toInteger = (v: unknown, fallback = 0): number => {
  const n = Number(v);
  return Number.isInteger(n) ? n : fallback;
};

export function toJSONArray(value: any): any[] {
  if (!value) return [];

  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value.trim());
      return Array.isArray(parsed) ? parsed : [];
    } catch (_e) {
      return [];
    }
  }
  if (Array.isArray(value)) {
    return value;
  }
  return [];
}

export function restructureProductDetails({
  idFirstBranch,
  branchMap,
  purchaseType,
  inputDetail,
  mapCostExpenses,
  mapProducts
}: RestructureProductParams): any[] {
  if (purchaseType === 'mercaderia') {
    return transformProductsMerchandise(
      idFirstBranch,
      branchMap,
      inputDetail,
      mapProducts
    );
  }
  return transformProductsExpensesServices(
    inputDetail,
    mapCostExpenses
  );
}

function transformProductsMerchandise(
  idFirstBranch: number,
  branchMap: Record<number, number>,
  inputDetail: any[],
  mapProducts: Record<number, number>
) {
  const isOld = isDetailOldMerchandise(inputDetail);
  return inputDetail.map(product => {
    const idBodega = isOld ? idFirstBranch : branchMap[product.idBodega];
    const oldCode = product.codigoXml ? product.codigoXml : product.codigoArticulo;
    const codigo = isOld ? oldCode : product.codigo;
    const productName = isOld ? product.descripcion : product.nombre;
    const stock = isOld ? 0 : toNumber(product.stock);
    const impuestoCode = isOld ?
      (codigoIvaPorcentaje?.[product.impuesto] ?? "2") :
      toInteger(product.codigoimpuesto, 0);
    const impuestoName = isOld ? toNumber(product.impuesto) : product.nombreImpuesto;
    const cost = isOld ? toNumber(product.precioProducto) : toNumber(product.costoProducto);
    const manualPrice = isOld ? 1 : toInteger(mapProducts[product.preciomanual]);
    return {
      idProducto: mapProducts[product.idProducto] || '',
      idBodega,
      bodega: String(product.bodega ?? ''),
      codigo,
      nombre: String(productName ?? ''),
      stock,
      observacion: String(product.descripcion ?? ""),
      cantidad: toNumber(product.cantidad),
      impuesto: toNumber(product.impuesto),
      codigoImpuesto: impuestoCode,
      nombreImpuesto: impuestoName,
      precioProducto: toNumber(product.precioProducto),
      costoProducto: cost,
      porcentajeDescuento: toNumber(product.porcentajeDescuento),
      valorDescuento: toNumber(product.valorDescuento),
      total: toNumber(product.total),
      preciomanual: manualPrice,
      codigoAuxiliar: '',
      tipo: ''
    }
  })
}

function transformProductsExpensesServices(
  inputDetail: any[],
  mapCostExpenses: Record<number, number | null>
): any[] {

  const isOld = isDetailOldCostExpenses(inputDetail);
  return inputDetail.map((product) => {
    const type = String(product.tipo ?? "").toLowerCase() === "costo" ? "costo" : "gasto"
    const codigo = mapCostExpenses[product.codigoArticulo] || '';
    const impuestoCode = isOld
      ? (codigoIvaPorcentaje?.[product.impuesto] ?? "2")
      : toInteger(product.codigoimpuesto, 0);
    const impuestoName = isOld ? toNumber(product.impuesto) : toNumber(product.nombreImpuesto);
    return {
      idProducto: "",
      idBodega: "",
      bodega: "",
      codigo: codigo,
      nombre: product.opcionSelect || "",
      stock: 0,
      observacion: product.descripcion || "",
      cantidad: toNumber(product.cantidad),
      impuesto: toNumber(product.impuesto),
      codigoImpuesto: impuestoCode,
      nombreImpuesto: impuestoName,
      precioProducto: toNumber(product.precioProducto),
      costoProducto: toNumber(product.precioProducto),
      porcentajeDescuento: toNumber(product.porcentajeDescuento),
      valorDescuento: toNumber(product.valorDescuento),
      total: toNumber(product.total),
      preciomanual: 1,
      codigoAuxiliar: "",
      tipo: type,
      projectId: "",
      costCenterId: "",
    };
  });
}

function isDetailOldMerchandise(data: unknown[]): boolean {
  return Array.isArray(data) && data.length > 0 && Object.keys(data[0] as object).length === 14;
}

function isDetailOldCostExpenses(data: unknown[]): boolean {
  return Array.isArray(data) && data.length > 0 && Object.keys(data[0] as object).length === 11;
}


function isDetailOldRetention(data: any): boolean {
  return data && data.length > 0 && data[0].hasOwnProperty('subcero') && data[0].hasOwnProperty('sub12');
}

function transformRetentionOldToNewVersion(inputDetail: any[], oldRetentionCodeMap: Map<string, RetentionCodeValue>): any[] {

  const listadoRetenciones = inputDetail.filter(
    ({
      renta
    }) => Number(renta) > 0
  ).map(item => {
    return {
      codigoRenta: item.renta || '',
      idRetRenta: oldRetentionCodeMap.get(`${item.renta}:RENTA`)?.id || '',
      nombreRenta: oldRetentionCodeMap.get(`${item.renta}:RENTA`)?.name || '',
      porcentajeRenta: item.porcentaje || '',
      subtotalBase0: item.subcero || '0.00',
      subtotalDiferente: item.sub12 || '0.00',
      valorRetenido: item.valorRetenido || '0.00'
    }
  });

  const listadoRetencionesIva = inputDetail.filter(
    ({
      rentaIva
    }) => Number(rentaIva) > 0
  ).map(item => {
    return {
      codigoIva: item.rentaIva || '',
      idRetIva: oldRetentionCodeMap.get(`${item.rentaIva}:IVA`)?.id || '',
      nombreIva: oldRetentionCodeMap.get(`${item.rentaIva}:IVA`)?.name || '',
      porcentajeIva: item.porcentajeIva || '',
      subtotalDiferenteIva: item.noGrabaIva || '0.00',
      valorRetenido: item.valorRetenidoIva || '0.00',
      impuestos: [{
        codigo: 2,
        tarifa: 12,
        total: item.sub12Iva || '0.00'
      }]
    }
  });
  return [{
    listadoRetenciones,
    listadoRetencionesIva
  }]
}

function transformRetentionNewVersion(inputDetail: any[], newRetentionIdMap: Record<number, number>): any[] {
  const {
    listadoRetenciones = [], listadoRetencionesIva = []
  } = inputDetail[0] || {};

  const listRetentionRent = listadoRetenciones
    .filter(({
      renta
    }) => renta)
    .map((retention: any) =>
    ({
      codigoRenta: retention.renta,
      idRetRenta: newRetentionIdMap[retention.idRetencionRenta] ?? '',
      nombreRenta: retention.nombreRetencionFuente || '',
      porcentajeRenta: retention.porcentaje || '',
      subtotalBase0: retention.subtotalBase0 || '0.00',
      subtotalDiferente: retention.subtotalDiferente || '0.00',
      valorRetenido: retention.valorRetenido || '0.00'
    })
    );

  const listRententionVat = listadoRetencionesIva
    .filter(({
      rentaIva
    }) => rentaIva)
    .map((retention: any) =>
    ({
      codigoIva: retention.rentaIva,
      idRetIva: newRetentionIdMap[retention.idRetencionIva] || '',
      nombreIva: retention.nombreRetencionIva || '',
      porcentajeIva: retention.porcentajeIva || '',
      subtotalDiferenteIva: retention.subtotalDiferenteIva || '0.00',
      valorRetenido: retention.valorRetenidoIva || '0.00',
      impuestos: retention.arraryImpuestos ?
        retention.arraryImpuestos.filter((impuesto: any) => impuesto.impuestoActivoIva === 1).map(impuesto => ({
          codigo: toNumber(impuesto.codigo),
          tarifa: toNumber(impuesto.tarifa),
          total: impuesto.totalImpuestoIva || '0.00'
        })) : []
    })
    );

  return [{
    listadoRetenciones: listRetentionRent,
    listadoRetencionesIva: listRententionVat
  }];
}

export function restructureRetentionDetail({
  inputDetail,
  oldRetentionCodeMap,
  newRetentionIdMap,
}: RestructureRetentionParams): any[] {
  if (isDetailOldRetention(inputDetail)) {
    return transformRetentionOldToNewVersion(
      inputDetail,
      oldRetentionCodeMap
    );
  }
  return transformRetentionNewVersion(
    inputDetail,
    newRetentionIdMap
  );
}

export async function insertAudit({
  conn,
  codigoAudit,
  module,
  companyId
}: InsertAuditParams) {
  try {
    const resultCreateAudit: ResultSet = await conn.query(`
    INSERT INTO audit(
      CODIGO_AUT,
      MOD_AUDIT,
      FK_COD_EMP
    )
    VALUES(?, ?, ?)`,
      [codigoAudit, module, companyId]
    );
    return (resultCreateAudit[0] as ResultSetHeader).insertId;
  } catch (err) {
    throw err;
  }
}

export async function findNextAuditCode({
  conn,
  companyId
}: FindNextAdutiCodeParams) {
  try {
    const auditQuery = `
    SELECT
        IFNULL(
            MAX(CAST(CODIGO_AUT AS UNSIGNED)) + 1,
            1
        ) AS auditId
    FROM
        audit
    WHERE
        FK_COD_EMP = ?;`;
    const auditQueryResult: ResultSet = await conn.query(auditQuery, [companyId]);
    const [auditData]: any[] = auditQueryResult as Array<any>;
    return auditData[0].auditId;
  } catch (err) {
    throw err;
  }
}

export async function findFirstDefaultUser({
  conn,
  companyId
}: FindFirstDefaultUserParams){
  try {
    const userQuery = `
      SELECT
          COD_USUEMP,
          IDE_USUEMP,
          NOM_USUEMP,
          EMA_USUEMP,
          ROL_USUEMP,
          EST_USUEMP
      FROM
          users
      WHERE
          FK_COD_EMP = ? AND ROL_USUEMP <> 'superadmin'
      ORDER BY
          COD_USUEMP ASC
      LIMIT 1;
    `;
    const userQueryResult: ResultSet = await conn.query(userQuery, [companyId]);
    const [user]: any[] = userQueryResult as Array<any>;
    return user;
  } catch (err) {
    throw err;
  }
}

export async function findFirstDefaultCustomer({
  conn,
  companyId
}: FindFirstDefaultCustomerParams){
  try {
    const customerQuery = `
      SELECT
          CUST_ID,
          CUST_CI,
          CUST_NOM,
          CUST_NOMCOM,
          CUST_DIR,
          CUST_TELF
      FROM
          customers
      WHERE
          CUST_TYPE = 'CLIENTE' AND FK_COD_EMP = ?
      ORDER BY
          CUST_ID ASC
      LIMIT 1;
    `;
    const customerQueryResult: ResultSet = await conn.query(customerQuery, [companyId]);
    const [customer]: any[] = customerQueryResult as Array<any>;
    return customer;
  } catch (err) {
    throw err;
  }
} 