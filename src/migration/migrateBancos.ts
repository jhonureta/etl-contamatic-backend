export async function migrateBancos(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapAccounts: Record<number, number | null>
): Promise<Record<number, number>> {

    const bankMap: Record<number, number> = {};
    const bankNationalMap: Record<number, number> = {};

    try {
        await cardGeneric(conn, newCompanyId);
        // 1️⃣ Traer todos los bancos migrados
        const [bancosMigrados] = await legacyConn.query(`
            SELECT ID_BANCO as BANCKID_MIG, FECREG_BANCO, FK_ENTIDAD_BANCARIA, NUMCUEN_BANC, DIR_BANC, TEL_BANC, CONTAC_BANC, AGEN_BANC, SALDO_BANCO, EST_BANCO, FK_CTAB_PLAN, SECU_CHEQ, CODN_BAN, NOM_BAN, EST_BAN
            FROM bancos inner join bancos_nacionales on bancos.FK_ENTIDAD_BANCARIA = bancos_nacionales.COD_BAN
        `);
        console.log("Migrando bancos...");

        if (!bancosMigrados.length) return bankMap;

        // 2️⃣ Traer todos los bancos nacionales existentes
        const [nationalBanks] = await conn.query(`SELECT COD_BAN, NOM_BAN, EST_BAN FROM national_banks;`);
        const existingNationalMap: Record<number, number> = {};
        const existingNationalNameMap: Record<string, number> = {};
        for (const nb of nationalBanks as any[]) {
            existingNationalMap[nb.COD_BAN] = nb.COD_BAN;
            existingNationalNameMap[normalizeBankName(nb.NOM_BAN)] = nb.COD_BAN;
        }
        console.log(` -> Batch migrado: ${bancosMigrados.length} bancos`);
        // 3️⃣ Iterar bancos migrados
        for (const banco of bancosMigrados as any[]) {
            const cuentaContableId = mapAccounts[banco.FK_CTAB_PLAN] ?? null;
            const nationalBankId = await findOrCreateNationalBank(conn, banco, {
                bankNationalMap,
                existingNationalMap,
                existingNationalNameMap
            });

            // Crear registro de banco
            const { insertId } = await createBankReg(conn, {
                bankIdReferencia: nationalBankId,
                numCuentaBank: banco.NUMCUEN_BANC,
                dirBank: banco.DIR_BANC,
                telBank: banco.TEL_BANC,
                agentContactBank: banco.CONTAC_BANC,
                agenciaBank: banco.AGEN_BANC,
                bankSal: banco.SALDO_BANCO,
                statusBank: banco.EST_BANCO,
                planCountId: cuentaContableId,
                secCheqBank: banco.SECU_CHEQ,
                companyId: newCompanyId,
                bancoIdMig: banco.BANCKID_MIG
            });

            bankMap[banco.BANCKID_MIG] = insertId;
            console.log(`-> Batch migrado: ${[banco].length} bancos`);
        }

    } catch (error) {
        throw error;
    }

    return bankMap;
}

async function createBankReg(conn: any, bankdata: any) {
    try {
        const [user] = await conn.query(
            `INSERT INTO banks(
                FK_ENTIDAD_BANCARIA, NUMCUEN_BANC, DIR_BANC, TEL_BANC, CONTAC_BANC, AGEN_BANC, SALDO_BANCO, EST_BANCO, FK_CTAB_PLAN, SECU_CHEQ, FK_COD_EMP
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?);`,
            [
                bankdata.bankIdReferencia,
                bankdata.numCuentaBank,
                bankdata.dirBank,
                bankdata.telBank,
                bankdata.agentContactBank,
                bankdata.agenciaBank,
                bankdata.bankSal,
                bankdata.statusBank,
                bankdata.planCountId ?? null,
                bankdata.secCheqBank,
                bankdata.companyId
            ]
        );
        return user;
    } catch (error) {
        throw error;
    }
}

function normalizeBankName(name: any): string {
    return String(name ?? '').trim().toUpperCase();
}

async function findOrCreateNationalBank(
    conn: any,
    banco: any,
    maps: {
        bankNationalMap: Record<number, number>;
        existingNationalMap: Record<number, number>;
        existingNationalNameMap: Record<string, number>;
    }
): Promise<number> {
    const oldNationalBankId = banco.FK_ENTIDAD_BANCARIA;
    const normalizedName = normalizeBankName(banco.NOM_BAN);

    if (maps.bankNationalMap[oldNationalBankId]) {
        return maps.bankNationalMap[oldNationalBankId];
    }

    const existingByCode = maps.existingNationalMap[oldNationalBankId];
    if (existingByCode) {
        maps.bankNationalMap[oldNationalBankId] = existingByCode;
        return existingByCode;
    }

    const existingByName = maps.existingNationalNameMap[normalizedName];
    if (existingByName) {
        maps.bankNationalMap[oldNationalBankId] = existingByName;
        maps.existingNationalMap[oldNationalBankId] = existingByName;
        return existingByName;
    }

    const [insertNational]: any = await conn.query(
        `INSERT INTO national_banks (NOM_BAN,EST_BAN) VALUES (?,?)`,
        [
            banco.NOM_BAN,
            banco.EST_BAN
        ]
    );
    const nationalBankId = insertNational.insertId;
    maps.bankNationalMap[oldNationalBankId] = nationalBankId;
    maps.existingNationalMap[oldNationalBankId] = nationalBankId;
    maps.existingNationalNameMap[normalizedName] = nationalBankId;
    return nationalBankId;
}

async function cardGeneric(conn: any, newCompanyId: number) {
    try {

        // 🔒 VALIDACIÓN FK
        if (
            newCompanyId === null ||
            newCompanyId === undefined ||
            !Number.isInteger(Number(newCompanyId)) ||
            Number(newCompanyId) <= 0
        ) {
            throw new Error('FK_COD_EMP inválido');
        }

        console.log("Migrando bancos...");

        const [bancosMigrados]: any = await conn.query(
            `SELECT COUNT(*) AS TOTALCARD FROM cards WHERE FK_COD_EMP = ?`,
            [newCompanyId]
        );

        const total = bancosMigrados[0]?.TOTALCARD ?? 0;

        // 🛑 YA EXISTE
        if (total > 0) {
            console.log('Tarjeta genérica ya creada, se omite');
            return;
        }

        const fecha = new Date();

        // ✅ INSERT
        const [result] = await conn.query(
            `INSERT INTO cards (
                FECREG_TARJETA,
                NOMBRE_TARJETA,
                NUMCUEN_TARJETA,
                SALDO_TARJETA,
                EST_TARJETA,
                TIPO_TARJETA,
                FK_COD_EMP,
                FK_CTAT_PLAN
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [
                fecha,
                'TARJETA GENERICA',
                '9999999999',
                0.00,
                'ACTIVO',
                'CREDITO',
                newCompanyId,
                null
            ]
        );

        return result;

    } catch (error: any) {
        console.error('Error cardGeneric:', error.message);
        throw error;
    }
}






