// normalizador.ts

export type ProductoNuevo = {
    idProducto: number | null;
    idBodega: number | null;
    codigo: string | null;
    nombre: string | null;
    observacion: string | null;
    cantidad: string | number | null;
    total: number | null;
    impuesto: number | null;
    codigoImpuesto: number | null;
    nombreImpuesto: string | null;
    cantidadAnterior: string | number | null;
    precioProducto: number | null;
    porcentajeDescuento: string | number | null;
    valorDescuento: string | number | null;
    precios: {
        name: string;
        price: number;
        discount: number;
        select: boolean;
    }[] | null;
    tota: number | null;
    tarifaice: string | number | null;
};

/* ================= DETECTOR ================= */

const detectarTipoProducto = (p: any): 'xml' | 'manual' | 'nuevo' => {
    if (p?.valores) return 'xml';
    if (p?.subtotal !== undefined) return 'manual';
    if (p?.precios) return 'nuevo';
    return 'manual';
};

/* ================= NORMALIZADORES ================= */

const normalizarDesdeXml = (p: any, mapProducts: any, branchMap: any, oldProductCodeMap: any, storeMap: Record<number, number>): ProductoNuevo => ({
    idProducto: p.idProducto ? Number(mapProducts[p.idProducto]) : null,
    idBodega: p.idBodega ? Number(storeMap[p.idBodega]) : null,
    codigo: p.codigo ?? null,
    nombre: p.nombre ?? null,
    observacion: p.observacion ?? null,
    cantidad: p.cantidad ?? null,
    total: p.tota ? Number(p.tota) : null,
    impuesto: p.impuesto ?? null,
    codigoImpuesto: p.codigoimpuesto ?? null,
    nombreImpuesto: p.nombreImpuesto ?? null,
    cantidadAnterior: p.cantidadfinal ?? p.cantidad ?? null,
    precioProducto: p.precioProducto ?? null,
    porcentajeDescuento: p.porcentajeDescuento ?? null,
    valorDescuento: p.valorDescuento ?? null,
    precios: Array.isArray(p.valores)
        ? p.valores.map((v: any) => ({
            name: v.nombre,
            price: v.valor,
            discount: Number(v.descuento) || 0,
            select: false
        }))
        : null,
    tota: p.tota ? Number(p.tota) : null,
    tarifaice: p.tarifaice ?? null
});

const normalizarDesdeManual = (p: any, mapProducts: any, branchMap: any, oldProductCodeMap: any, idSucursal: number, storeMap: Record<number, number>): ProductoNuevo => ({
    idProducto: oldProductCodeMap.get(`${p.codigo}`)?.id || '',
    idBodega: idSucursal,
    codigo: p.codigo ?? null,
    nombre: p.nombre ?? null,
    observacion: p.observacion ?? null,
    cantidad: p.cantidad ?? null,
    total: p.total ? Number(p.total) : null,
    impuesto: p.impuesto ? Number(p.impuesto) : null,
    codigoImpuesto: p.codigoimpuesto ? Number(p.codigoimpuesto) : null,
    nombreImpuesto: p.nombreImpuesto ?? null,
    cantidadAnterior: p.cantidadfinal ?? p.cantidad ?? null,
    precioProducto: p.total ? Number(p.total) : null,
    porcentajeDescuento: p.porcentajeDescuento ?? null,
    valorDescuento: p.valorDescuento ?? null,
    precios: [{ name: "Publico", price: p.total ? Number(p.total) : null, discount: 0, "select": true }],
    tota: p.tota ? Number(p.tota) : null,
    tarifaice: 0.0
});

const normalizarDesdeNuevo = (p: any, mapProducts: any, branchMap: any, oldProductCodeMap: any, storeMap: Record<number, number>): ProductoNuevo => ({
    ...p
});

/* ================= NORMALIZADOR AUTOMÁTICO ================= */

export const normalizarProducto = (p: any, mapProducts: any, branchMap: any, oldProductCodeMap: any, idSucursal: number, storeMap: Record<number, number>): ProductoNuevo => {
    const tipo = detectarTipoProducto(p);
    switch (tipo) {
        case 'xml':
            return normalizarDesdeXml(p, mapProducts, branchMap, oldProductCodeMap, storeMap);
        case 'manual':
            return normalizarDesdeManual(p, mapProducts, branchMap, oldProductCodeMap, idSucursal, storeMap);
        case 'nuevo':
            return normalizarDesdeNuevo(p, mapProducts, branchMap, oldProductCodeMap, storeMap);
        default:
            return normalizarDesdeManual(p, mapProducts, branchMap, oldProductCodeMap, idSucursal, storeMap);
    }
};
