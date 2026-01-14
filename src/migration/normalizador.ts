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
    precioProducto: string | null;
    porcentajeDescuento: string | number | null;
    valorDescuento: string | number | null;
    precios: {
        name: string;
        price: string;
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

const normalizarDesdeXml = (p: any): ProductoNuevo => ({
    idProducto: p.idProducto ? Number(p.idProducto) : null,
    idBodega: p.idBodega ? Number(p.idBodega) : null,
    codigo: p.codigo ?? null,
    nombre: p.nombre ?? null,
    observacion: p.observacion ?? null,
    cantidad: p.cantidad ?? null,
    total: p.tota ? Number(p.tota) : null,
    impuesto: Number(p.impuestoEmpresa ?? p.impuesto) || null,
    codigoImpuesto: Number(p.codigoImpuestoEmpresa ?? p.codigoimpuesto) || null,
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

const normalizarDesdeManual = (p: any): ProductoNuevo => ({
    idProducto: null,
    idBodega: null,
    codigo: p.codigo ?? null,
    nombre: p.nombre ?? null,
    observacion: p.observacion ?? null,
    cantidad: p.cantidad ?? null,
    total: p.total ? Number(p.total) : null,
    impuesto: p.impuesto ? Number(p.impuesto) : null,
    codigoImpuesto: p.codigoimpuesto ? Number(p.codigoimpuesto) : null,
    nombreImpuesto: p.nombreImpuesto ?? null,
    cantidadAnterior: p.cantidadfinal ?? p.cantidad ?? null,
    precioProducto: null,
    porcentajeDescuento: p.porcentajeDescuento ?? null,
    valorDescuento: p.valorDescuento ?? null,
    precios: null,
    tota: p.tota ? Number(p.tota) : null,
    tarifaice: null
});

const normalizarDesdeNuevo = (p: any): ProductoNuevo => ({
    ...p
});

/* ================= NORMALIZADOR AUTOMÁTICO ================= */

export const normalizarProducto = (p: any): ProductoNuevo => {
    const tipo = detectarTipoProducto(p);

    switch (tipo) {
        case 'xml':
            return normalizarDesdeXml(p);
        case 'manual':
            return normalizarDesdeManual(p);
        case 'nuevo':
            return normalizarDesdeNuevo(p);
        default:
            return normalizarDesdeManual(p);
    }
};
