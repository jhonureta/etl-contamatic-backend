export async function migrateBancos(
    legacyConn: any,
    conn: any,
    newCompanyId: number,
    mapAccounts: Record<number, number | null>
): Promise<Record<number, number>> {

    const bankMap: Record<number, number> = {};
    const bankNationalMap: Record<number, number> = {};

    try {
        await cardGeneric(conn,newCompanyId);
        // 1️⃣ Traer todos los bancos migrados
        const [bancosMigrados] = await legacyConn.query(`
            SELECT ID_BANCO as BANCKID_MIG, FECREG_BANCO, FK_ENTIDAD_BANCARIA, NUMCUEN_BANC, DIR_BANC, TEL_BANC, CONTAC_BANC, AGEN_BANC, SALDO_BANCO, EST_BANCO, FK_CTAB_PLAN, SECU_CHEQ
            FROM bancos
        `);
        console.log("Migrando bancos...");

        if (!bancosMigrados.length) return bankMap;

        // 2️⃣ Traer todos los bancos nacionales existentes
        const [nationalBanks] = await conn.query(`SELECT * FROM national_banks;`);
        const existingNationalMap: Record<number, any> = {};
        for (const nb of nationalBanks as any[]) {
            existingNationalMap[nb.COD_BAN] = nb;
        }
        console.log(` -> Batch migrado: ${bancosMigrados.length} bancos`);
        // 3️⃣ Iterar bancos migrados
        for (const banco of bancosMigrados as any[]) {
            const cuentaContableId = mapAccounts[banco.FK_CTAB_PLAN] ?? null;
            let nationalBankId: number;

            if (!existingNationalMap[banco.FK_ENTIDAD_BANCARIA]) {
                // Insertar banco nacional si no existe
                const [insertNational] = await conn.query(
                    `INSERT INTO national_banks (CODN_BAN,NOM_BAN,EST_BAN) VALUES (?,?,?)`,
                    [
                        banco.CODN_BAN,
                        banco.NOM_BAN,
                        banco.EST_BAN
                    ]
                );
                nationalBankId = insertNational.insertId;
                existingNationalMap[banco.FK_ENTIDAD_BANCARIA] = { ID: nationalBankId };
                bankNationalMap[banco.BANCKID_MIG] = nationalBankId;
            } else {
                nationalBankId = existingNationalMap[banco.FK_ENTIDAD_BANCARIA].ID || banco.FK_ENTIDAD_BANCARIA;
            }

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
             console.log(` -> Batch migrado: ${banco.length} bancos`);
        }

    } catch (error) {
        throw new Error(error);
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
        throw new Error(error);
    }
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






