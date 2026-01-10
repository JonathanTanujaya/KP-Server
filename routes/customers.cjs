function registerCustomerRoutes(fastify, { db }) {
  fastify.get("/api/customers", async () => {
    return db.all(
      `SELECT id,
              kode AS kode_customer,
              nama AS nama_customer,
              area_kode AS kode_area,
              telepon,
              COALESCE(NULLIF(TRIM(kontak_person), ''), nama) AS kontak_person,
              alamat,
              created_at,
              updated_at
       FROM m_customer
       ORDER BY nama ASC`
    );
  });

  fastify.put("/api/customers/:kode", async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? "").trim();
    const body = request.body || {};
    const kodeBody = body.kode_customer ?? body.kode;
    const nama = body.nama_customer ?? body.nama;
    const kode_area = body.kode_area ?? null;
    const telepon = body.telepon ?? null;
    const kontak_person = body.kontak_person ?? null;
    const alamat = body.alamat ?? null;

    if (!kodeParam)
      return reply.code(400).send({ error: "kode_customer is required" });
    if (
      kodeBody != null &&
      String(kodeBody).trim() &&
      String(kodeBody).trim() !== kodeParam
    ) {
      return reply.code(400).send({ error: "kode_customer cannot be changed" });
    }
    if (!nama || typeof nama !== "string" || !nama.trim()) {
      return reply.code(400).send({ error: "nama_customer is required" });
    }

    const exists = db.get("SELECT id FROM m_customer WHERE kode = ?", [
      kodeParam,
    ]);
    if (!exists) return reply.code(404).send({ error: "not found" });

    if (kode_area) {
      const areaExists = db.get("SELECT id FROM m_area WHERE kode = ?", [
        String(kode_area).trim(),
      ]);
      if (!areaExists)
        return reply.code(400).send({ error: "kode_area not found" });
    }

    try {
      db.run(
        "UPDATE m_customer SET nama = ?, area_kode = ?, telepon = ?, kontak_person = ?, alamat = ? WHERE kode = ?",
        [
          nama.trim(),
          kode_area ? String(kode_area).trim() : null,
          telepon,
          kontak_person,
          alamat,
          kodeParam,
        ]
      );

      const updated = db.get(
        `SELECT id,
                kode AS kode_customer,
                nama AS nama_customer,
                area_kode AS kode_area,
                telepon,
                COALESCE(NULLIF(TRIM(kontak_person), ''), nama) AS kontak_person,
                alamat,
                created_at,
                updated_at
         FROM m_customer
         WHERE kode = ?`,
        [kodeParam]
      );
      return reply.send(updated);
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });

  fastify.delete("/api/customers/:kode", async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? "").trim();
    if (!kodeParam)
      return reply.code(400).send({ error: "kode_customer is required" });

    const exists = db.get("SELECT id FROM m_customer WHERE kode = ?", [
      kodeParam,
    ]);
    if (!exists) return reply.code(404).send({ error: "not found" });

    try {
      db.run("DELETE FROM m_customer WHERE kode = ?", [kodeParam]);
      return reply.code(204).send();
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });

  fastify.post("/api/customers", async (request, reply) => {
    const body = request.body || {};
    const kode = body.kode_customer ?? body.kode;
    const nama = body.nama_customer ?? body.nama;
    const kode_area = body.kode_area ?? null;
    const telepon = body.telepon ?? null;
    const kontak_person = body.kontak_person ?? null;
    const alamat = body.alamat ?? null;

    if (!kode || typeof kode !== "string" || !kode.trim()) {
      return reply.code(400).send({ error: "kode_customer is required" });
    }

    if (!nama || typeof nama !== "string" || !nama.trim()) {
      return reply.code(400).send({ error: "nama_customer is required" });
    }

    try {
      const result = db.run(
        "INSERT INTO m_customer (kode, nama, area_kode, telepon, kontak_person, alamat) VALUES (?, ?, ?, ?, ?, ?)",
        [kode.trim(), nama.trim(), kode_area, telepon, kontak_person, alamat]
      );

      const created = db.get(
        `SELECT id,
                kode AS kode_customer,
                nama AS nama_customer,
                area_kode AS kode_area,
                telepon,
                COALESCE(NULLIF(TRIM(kontak_person), ''), nama) AS kontak_person,
                alamat,
                created_at,
                updated_at
         FROM m_customer
         WHERE id = ?`,
        [result.lastInsertRowid]
      );

      return reply.code(201).send(created);
    } catch (err) {
      if (err && String(err.message || "").includes("UNIQUE")) {
        return reply.code(409).send({ error: "kode_customer already exists" });
      }
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });
}

module.exports = { registerCustomerRoutes };
