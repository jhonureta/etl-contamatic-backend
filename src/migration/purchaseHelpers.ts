export interface RetentionCodeValue {
  id: number;
  name: string;
}

type RestructureRetentionParams = {
  inputDetail: any[];
  mapRetentions: any;
  retentionsByCode: Map<string, RetentionCodeValue>;
};

type RestructureProductParams = {
  idFirstBranch: number;
  purchaseType: string;
  inputDetail: any[];
  branchMap: Record<number, number>;
  mapProducts: Record<number, number>;
  mapCostExpenses: Record<number, number | null>;
};

const codigoIvaPorcentaje: Record<string, number> = {
  '12': 2,
  '0': 0,
  '14': 3,
  '15': 4,
  '5': 5,
  '13': 10
}

const toNumber = (v: unknown, fallback = 0): number => {
  const n = Number.parseFloat(String(v));
  return Number.isFinite(n) ? n : fallback;
};

const toInteger = (v: unknown, fallback = 0): number => {
  const n = Number.parseInt(String(v), 10);
  return Number.isFinite(n) ? n : fallback;
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
    const idBodega = isOld ? idFirstBranch : toInteger(branchMap[product.idBodega]);
    const oldCode = product.codigoXml ? product.codigoXml : product.codigoArticulo;
    const codigo = isOld ? oldCode : product.codigo;
    const productName = isOld ? product.descripcion : product.nombre;
    const stock = isOld ? 0 : toNumber(product.stock, 0);
    const impuestoCode = isOld ?
      (codigoIvaPorcentaje?.[product.impuesto] ?? "2") :
      toInteger(product.codigoimpuesto, 0);
    const impuestoName = isOld ? toNumber(product.impuesto, 0) : toNumber(product.nombreImpuesto, 0);
    const cost = isOld ? toNumber(product.precioProducto, 0) : toNumber(product.costoProducto, 0);
    const manualPrice = isOld ? 1 : toInteger(mapProducts[product.preciomanual]);
    return {
      idProducto: toInteger(mapProducts[product.idProducto]),
      idBodega,
      bodega: String(product.bodega ?? ''),
      codigo,
      nombre: String(productName ?? ''),
      stock,
      observacion: String(product.descripcion ?? ""),
      cantidad: toNumber(product.cantidad, 0),
      impuesto: toNumber(product.impuesto, 0),
      codigoImpuesto: impuestoCode,
      nombreImpuesto: impuestoName,
      precioProducto: toNumber(product.precioProducto, 0),
      costoProducto: cost,
      porcentajeDescuento: toNumber(product.porcentajeDescuento, 0),
      valorDescuento: toNumber(product.valorDescuento, 0),
      total: toNumber(product.total, 0),
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
    const impuestoName = isOld ? toNumber(product.impuesto, 0) : toNumber(product.nombreImpuesto, 0);
    return {
      idProducto: "",
      idBodega: "",
      bodega: "",
      codigo: codigo,
      nombre: product.opcionSelect || "",
      stock: 0,
      observacion: product.descripcion || "",
      cantidad: toNumber(product.cantidad, 0),
      impuesto: toNumber(product.impuesto, 0),
      codigoImpuesto: impuestoCode,
      nombreImpuesto: impuestoName,
      precioProducto: toNumber(product.precioProducto, 0),
      costoProducto: toNumber(product.precioProducto, 0),
      porcentajeDescuento: toNumber(product.porcentajeDescuento, 0),
      valorDescuento: toNumber(product.valorDescuento, 0),
      total: toNumber(product.total, 0),
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

function transformRetentionOldToNewVersion(inputDetail: any[], retentionsByCode: Map<string, RetentionCodeValue>): any[] {

  const listadoRetenciones = inputDetail.filter(
    ({
      renta
    }) => Number(renta) > 0
  ).map(item => {
    return {
      codigoRenta: item.renta || '',
      idRetRenta: retentionsByCode.get(`${item.renta}:RENTA`).id || '',
      nombreRenta: retentionsByCode.get(`${item.renta}:RENTA`).name || '',
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
      idRetIva: retentionsByCode.get(`${item.rentaIva}:IVA`)?.id || '',
      nombreIva: retentionsByCode.get(`${item.rentaIva}:IVA`)?.name || '',
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

function transformRetentionNewVersion(inputDetail: any[], mapRetentions: any): any[] {
  const {
    listadoRetenciones = [], listadoRetencionesIva = []
  } = inputDetail[0] || {};

  const listRetentionRent = listadoRetenciones
    .filter(({
      renta
    }) => renta)
    .map(retention =>
    ({
      codigoRenta: retention.renta,
      idRetRenta: mapRetentions[retention.idRetencionRenta],
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
    .map(retention =>
    ({
      codigoIva: retention.rentaIva,
      idRetIva: mapRetentions[retention.idRetencionIva],
      nombreIva: retention.nombreRetencionIva || '',
      porcentajeIva: retention.porcentajeIva || '',
      subtotalDiferenteIva: retention.subtotalDiferenteIva || '0.00',
      valorRetenido: retention.valorRetenidoIva || '0.00',
      impuestos: retention.arraryImpuestos ?
        retention.arraryImpuestos.filter(impuesto => impuesto.impuestoActivoIva === 1).map(impuesto => ({
          codigo: parseInt(impuesto.codigo, 10),
          tarifa: parseInt(impuesto.tarifa, 10),
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
  mapRetentions,
  retentionsByCode,
}: RestructureRetentionParams): any[] {
  if (isDetailOldRetention(inputDetail)) {
    return transformRetentionOldToNewVersion(
      inputDetail,
      retentionsByCode
    );
  }
  return transformRetentionNewVersion(
    inputDetail,
    mapRetentions
  );
}


