export type TipoObligacion = 'CXC' | 'CXP';
export type Orientacion = 'debe' | 'haber';

export interface AccountingParameterInput {
    companyId: number;
    cuenta: string;
    valor: number;
    orientacion: Orientacion;
}

export interface AccountingEntry {
    nombre: string;
    codigoplan: string;
    debePlan: number;
    haber: number;
    id_plan: number;
}

export interface ClientIdentity {
    id: number;
    ci: string;
}

export interface UserIdentity  {
    id: number;
    ci: string;
}

export class AccountingService {
    async checkAccountingParameters(
        data: AccountingParameterInput,
        conn: any
    ): Promise<AccountingEntry> {

        try {
            const query = `
                SELECT 
                    tbPlan.ID_PLAN,
                    tbPlan.CODIGO_PLAN,
                    tbPlan.NOM_PLAN
                FROM accounting_parameters ap
                LEFT JOIN company_acct_params cap 
                    ON cap.COD_PARAMETROID = ap.COD_PARAMETRO
                    AND cap.FK_COD_EMP = ?
                LEFT JOIN account_plan tbPlan
                    ON cap.FK_CODIGOPLAN = tbPlan.ID_PLAN
                    AND tbPlan.FK_COD_EMP = ?
                WHERE ap.CODIGO_REFERENCIA = ?;
            `;

            const [rows]: any[] = await conn.query(query, [
                data.companyId,
                data.companyId,
                data.cuenta,
            ]);

            if (!rows.length) {
                throw new Error(
                    'Cuenta no encontrada'
                );
            }

            if (!rows[0].ID_PLAN) {
                throw new Error(
                    'Cuenta no encontrada'
                );
            }

            return {
                nombre: rows[0].NOM_PLAN,
                codigoplan: rows[0].CODIGO_PLAN,
                debePlan: data.orientacion === 'debe' ? data.valor : 0,
                haber: data.orientacion === 'haber' ? data.valor : 0,
                id_plan: rows[0].ID_PLAN,
            };

        } catch (error: any) {
            throw new Error(
                error.message
            );
        }
    }
}

export async function upsertTotaledEntry(
    conn: any,
    data: {
        id_plan: number;
        fecha: string;
        debe: number;
        haber: number;
        total: number;
        operacion:string;
    },
    companyId: number
) {
    const asientoQuery = `
                        SELECT TOTAL_IDENTRY, TOTAL_DEBE, TOTAL_HABER, SALDO_CONTABLE 
                        FROM totaledentries 
                        WHERE ID_FKPLAN = ? AND FECHA_ENTRY = ? AND FK_COD_EMP = ?;
                    `;
    const [existingEntry] = await conn.query(asientoQuery, [
        data.id_plan,
        data.fecha,
        companyId
    ]);

    if (existingEntry.length > 0) {
        // Determinar las operaciones según el tipo de movimiento
        const isSum = data.operacion === "suma";
        const totalDebe = isSum ? "COALESCE(TOTAL_DEBE, 0) + ?" : "COALESCE(TOTAL_DEBE, 0) - ?";
        const totalHaber = isSum ? "COALESCE(TOTAL_HABER, 0)  + ?" : "COALESCE(TOTAL_HABER, 0) - ?";
        const totalOperacion = isSum ? "COALESCE(TOTAL_NUMASI, 0)  + 1" : "COALESCE(TOTAL_NUMASI, 0) - 1";
        const updateQuery = `
                            UPDATE totaledentries 
                            SET 
                                TOTAL_DEBE = ${totalDebe}, 
                                TOTAL_HABER = ${totalHaber},
                                TOTAL_NUMASI = ${totalOperacion} 
                            WHERE 
                                TOTAL_IDENTRY = ? 
                                AND ID_FKPLAN = ?;
                        `;
        const [updateResult] = await conn.query(updateQuery, [
            data.debe,
            data.haber,
            existingEntry[0].TOTAL_IDENTRY,
            data.id_plan
        ]);
        return updateResult;
    }

    // Si no existe el registro, insertar uno nuevo
    const totalSaldo = data.debe || data.haber;
    const totalNum = 1;
    const insertQuery = `
                        INSERT INTO totaledentries (
                            ID_FKPLAN, FECHA_ENTRY, TOTAL_DEBE, TOTAL_HABER, SALDO_CONTABLE, TOTAL_NUMASI, FK_COD_EMP
                        ) VALUES (?, ?, ?, ?, ?, ?, ?);
                    `;
    const [insertResult] = await conn.query(insertQuery, [
        data.id_plan,
        data.fecha,
        data.debe,
        data.haber,
        totalSaldo,
        totalNum,
        companyId
    ]);
    return insertResult;
}


export async function fetchAccountingPeriod(conn, dataMov) {
    try {
        if (!dataMov || dataMov.fechaManual == null) {
            throw new Error('fechaManual is required');
        }

        const fm = dataMov.fechaManual;
        let anioPeriodo: string;
        let numeroMes: string;

        if (typeof fm === 'string') {
            const datePart = fm.includes('T') ? fm.split('T')[0] : fm.split(' ')[0];
            const parts = datePart.split('-');
            if (parts.length >= 2 && parts[0].length === 4) {
                anioPeriodo = parts[0];
                numeroMes = parts[1].padStart(2, '0');
            } else {
                const d = new Date(fm);
                if (isNaN(d.getTime())) throw new Error('Invalid fechaManual value');
                anioPeriodo = String(d.getFullYear());
                numeroMes = String(d.getMonth() + 1).padStart(2, '0');
            }
        } else {
            const d = fm instanceof Date ? fm : new Date(fm);
            if (isNaN(d.getTime())) throw new Error('Invalid fechaManual value');
            anioPeriodo = String(d.getFullYear());
            numeroMes = String(d.getMonth() + 1).padStart(2, '0');
        }
        const query = `SELECT COD_PERIODO,ESTADO_CIERRE,FECHA_INICIO,FECHA_CIERRE FROM accounting_period WHERE ANIO_PERIODO=? AND NUMERO_MES =? AND FK_COD_EMP=?;`;
        const [dataSecuencia] = await conn.query(query, [anioPeriodo, numeroMes, dataMov.companyId]);

        if (dataSecuencia.length == 0) {
            throw new Error('No existe periodo contable en esta fecha')
        }

        return dataSecuencia;
    } catch (error) {
        throw new Error(error);
    }
}




export function chunk<T>(array: T[], size: number): T[][] {
    const result: T[][] = [];
    for (let i = 0; i < array.length; i += size) {
        result.push(array.slice(i, i + size));
    }
    return result;
}