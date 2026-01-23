import {  codigoIvaPorcentaje, toInteger, toNumber } from "./purchaseHelpers";

export function restructureCreditNoteDetail({
  inputDetail,
  mapProducts,
  branchMap,
  idFirstBranch,
  creditNoteType,
  oldProductCodeMap
}) {
  if (creditNoteType === 'descuento') {
    return transformDiscountCreditNoteDetail(inputDetail, oldProductCodeMap, mapProducts, branchMap, idFirstBranch);
  }

  return transformReturnedCreditNoteDetail(inputDetail, mapProducts, branchMap, idFirstBranch);

}

function transformDiscountCreditNoteDetail(inputDetail, oldProductCodeMap, mapProducts, branchMap, idFirstBranch) {
  const isOldDiscount = idOldDiscountDetail(inputDetail);
  let branchId = idFirstBranch;
  const detailTransformed = inputDetail.map((product: any, index: number) => {
    let idProducto = "";
    let idBodega = "";
    let cantidad = toNumber(product?.cantidad ?? 0);
    let impuesto = toNumber(product?.impuesto ?? 0);
    let codigoImpuesto = 0;
    let nombreImpuesto = "";
    let total = toNumber(product?.total ?? 0);
    
    if (isOldDiscount) {
      const codigo = product?.codigo;
      const isCodigoNoNumerico = Number.isNaN(Number(codigo));
    
      idProducto = isCodigoNoNumerico
        ? (oldProductCodeMap?.get(codigo)?.id ?? "")
        : (mapProducts?.[codigo] ?? "");
    
      idBodega = idFirstBranch;
      codigoImpuesto = codigoIvaPorcentaje?.[String(impuesto)] ?? 0;
      nombreImpuesto = String(impuesto);
    } else {
      idProducto = mapProducts?.[product?.idProducto] ?? "";
      idBodega = branchMap?.[product?.idBodega] ?? "";
      codigoImpuesto = toInteger(product?.codigoimpuesto ?? 0, 0);
      nombreImpuesto = String(product?.nombreImpuesto ?? "");
    }
    
    // branchId solo se setea si viene mapeada (
    if (index === 0 && idBodega) branchId = idBodega;
    
    // fallback final para el detalle
    idBodega = idBodega || idFirstBranch;

    return {
      idProducto,
      idBodega,
      bodega: "",
      codigo: String(product?.codigo ?? ""),
      nombre: String(product?.nombre ?? ""),
      stock: 0,
      observacion: String(product?.observacion ?? ""),
      cantidad,
      impuesto,
      codigoImpuesto,
      nombreImpuesto,
      precioProducto: 0,
      costoProducto: 0,
      porcentajeDescuento: toNumber(product?.porcentajeDescuento ?? 0),
      valorDescuento: toNumber(product?.valorDescuento ?? 0),
      total,
      preciomanual: 1,
      codigoAuxiliar: String(product?.codigo ?? ""),
      tipo: "",
      cantidadAnterior: cantidad,
      valorImpuesto: toNumber(product?.valorImpuesto ?? 0),
    };
  });
  return { detailTransformed, branchId };
}

function transformReturnedCreditNoteDetail(inputDetail, mapProducts, branchMap, idFirstBranch) {
  let branchId = idFirstBranch;
  const detailTransformed = inputDetail.map((product: any, index: number) => {
    let idProducto = mapProducts[product.idProducto] ?? "";

    const mappedBodega = branchMap?.[product?.idBodega];
    if (index === 0 && mappedBodega) branchId = mappedBodega;
    const idBodega = mappedBodega || idFirstBranch;
    
    let cantidad = toNumber(product?.cantidad ?? 0);
    let impuesto = toNumber(product?.impuesto ?? 0);
    let codigoImpuesto = toInteger(product?.codigoimpuesto ?? 0);
    let nombreImpuesto = String(product.nombreImpuesto ?? "");
    let total = toNumber(product?.total ?? 0);
    let precioProducto = toNumber(product?.precioProducto ?? 0);
    let costoProducto = product?.costoProducto ?  toNumber(product?.costoProducto ?? 0) : precioProducto;
    let codigo = product?.codigo ? String(product?.codigo ?? "") : (product?.codigoArticulo ?? "");
    let observacion = product?.descripcion ? String(product?.descripcion ?? "") : "";
    return {
      idProducto,
      idBodega,
      bodega: "",
      codigo,
      nombre: String(product?.nombre ?? ""),
      stock: 0,
      observacion,
      cantidad,
      impuesto,
      codigoImpuesto,
      nombreImpuesto,
      precioProducto,
      costoProducto,
      porcentajeDescuento: toNumber(product?.porcentajeDescuento ?? 0),
      valorDescuento: toNumber(product?.valorDescuento ?? 0),
      total,
      preciomanual: 1,
      codigoAuxiliar: codigo,
      tipo: "",
      cantidadAnterior: cantidad,
      valorImpuesto: 0,
    };
  });
  return { detailTransformed, branchId };
}

function idOldDiscountDetail(data): boolean {
  return Array.isArray(data) && data.length > 0 && Object.keys(data[0] as object).length === 8;
}