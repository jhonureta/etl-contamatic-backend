

export async function migrateVehicles({
  legacyConn,
  conn,
  newCompanyId,
}): Promise<{ vehicleIdMap: Record<number, number> }> {
  try {
    console.log("Migrando vehiculos...");
    const vehicleIdMap: Record<number, number> = {};

    const [vehicles]: any[] = await legacyConn.query(`
      SELECT
          COD_VEH,
          PLACA AS PLACA_VEH,
          TIPO AS TIPO_VEH,
          NULL AS DESC_VEH,
          ESTADO AS ESTADO_VEH,
          NULL AS FK_COD_EMPRESA,
          NOW() AS FEC_REG, NOW() AS FEC_MOD
      FROM
          vehiculo;
    `);
    if (vehicles.length === 0) {
      throw new Error(" -> No hay vehiculos para migrar.");
    }
    console.log(` -> Vehiculos totales a procesar: ${vehicles.length}.`);
    const vehicleValues = vehicles.map((v) => [
      v.PLACA,
      v.TIPO_VEH,
      v.DESC_VEH,
      v.ESTADO_VEH,
      newCompanyId,
      v.FEC_REG,
      v.FEC_MOD
    ]);

    const [createVehicle]: any[] = await conn.query(`
      INSERT INTO vehicles (PLACA_VEH, TIPO_VEH, DESC_VEH, ESTADO_VEH, FK_COD_EMPRESA, FEC_REG, FEC_MOD) VALUES ?`,
      [vehicleValues]
    );

    let nextVehicleId = createVehicle.insertId;
    vehicles.forEach((v) => {
      vehicleIdMap[v.COD_VEH] = nextVehicleId++;
    });
    console.log(` -> Vehiculos migrados: ${Object.keys(vehicleIdMap).length}.`);
    return { vehicleIdMap };
  } catch (error) {
    throw error;
  }
}