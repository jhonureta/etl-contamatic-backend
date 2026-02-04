import fs from 'fs';
import path from 'path';

interface RetentionSri {
    COD_RET: number;
    ACCESS_KEY_RET: string;
}
export async function migrateRetentionFiles(conn): Promise<{ processed: number }> {
    try {
        console.log("Validando archivos XML de retenciones...");

        const [retentions]: any[] = await conn.query(`
            SELECT 
                COD_RET,
                ACCESS_KEY_RET
            FROM retention_sri
            WHERE STATE_RET = 'NO_REGISTRADO'
        `);

        if (retentions.length === 0) {
            console.log(" -> No existen retenciones pendientes.");
            return { processed: 0 };
        }

        console.log(` -> Retenciones a procesar: ${retentions.length}.`);

        let processed = 0;

        for (const ret of retentions) {
            // Eliminamos la "/" inicial para evitar rutas mal formadas
            const xmlPath = path.join(process.env.LEGACY_SYSTEM_PATH || '', `${ret.ACCESS_KEY_RET}.xml`);
            
            // Lógica corregida: Entra si el archivo EXISTE
            if (fs.existsSync(xmlPath)) {
                console.log(`Archivo encontrado: ${ret.ACCESS_KEY_RET}.xml - Actualizando estado...`);

                const [update]: any[] = await conn.query(
                    `UPDATE retention_sri SET STATE_FILE_RET = 1 WHERE COD_RET = ?`,
                    [ret.COD_RET]
                );

                if (update.affectedRows === 0) {
                    throw new Error(`Error al actualizar la retención ${ret.COD_RET}`);
                }

                processed++;
            } else {
                // Opcional: Log para saber cuáles faltan
                console.warn(`Archivo NO encontrado en ruta: ${ret.ACCESS_KEY_RET}.xml`);
            }
        }

        console.log(` -> Total retenciones actualizadas: ${processed}.`);
        return { processed };

    } catch (error) {
        console.error("Error al validar retenciones:", error);
        throw error;
    }
}
