export async function migrateWithholdingBanks(
    legacyConn: any,
    conn: any
): Promise<{ mapWithholdingBanks: Record<string, number> }> {//Promise<{ mapWithholdingBanks: Record<string, number> }> {
    try {
        console.log("Migrando Bancos retenedores...");

        const mapWithholdingBanks: Record<string, number> = {};
        /* 1️⃣ Bancos origen */
        const [sourceBanks]: any[] = await legacyConn.query(`
            SELECT COD_BANK, RUC, NOMBRE, FECREG_BANKS
            FROM bancos_emisores_retencion
        `);

        if (sourceBanks.length === 0) {
            return { mapWithholdingBanks };
        }

        /* 2️⃣ Bancos ya existentes */
        const [existingBanks]: any[] = await conn.query(`
            SELECT ID_BANK, RUC
            FROM withholding_banks
        `);

        /* 3️⃣ Mapa por RUC */
        const bankByRuc = new Map<string, number>();
        existingBanks.forEach((b: any) => {
            bankByRuc.set(b.RUC, b.ID_BANK);
        });

        /* 4️⃣ Procesar uno por uno */
        for (const bank of sourceBanks) {

            // 🟢 Ya existe
            if (bankByRuc.has(bank.RUC)) {
                mapWithholdingBanks[bank.RUC] = bankByRuc.get(bank.RUC)!;
                continue;
            }

            // 🔵 No existe → crear
            const [insertResult]: any = await conn.query(`
                INSERT INTO withholding_banks (RUC, NOMBRE, FECREG_BANKS)
                VALUES (?, ?, ?)
            `, [
                bank.RUC,
                bank.NOMBRE,
                bank.FECREG_BANKS ?? new Date()
            ]);

            const newBankId = insertResult.insertId;

            // actualizar mapas
            bankByRuc.set(bank.RUC, newBankId);
            mapWithholdingBanks[bank.RUC] = newBankId;
        }

        console.log(` -> Bancos retenedores procesados: ${Object.keys(mapWithholdingBanks).length}`);

        return { mapWithholdingBanks };

    } catch (error) {
        console.error("Error migrando bancos retenedores", error);
        throw error;
    }
}
