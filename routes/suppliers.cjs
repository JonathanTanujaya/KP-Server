function registerSupplierRoutes(fastify, { db }) {
  fastify.get("/api/suppliers", async () => {
    return await db.all(
      "SELECT id, kode AS kode_supplier, nama AS nama_supplier, telepon, email, alamat, created_at, updated_at FROM m_supplier ORDER BY nama ASC"
    );
  });

  fastify.put("/api/suppliers/:kode", async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? "").trim();
    const body = request.body || {};
    const kodeBody = body.kode_supplier ?? body.kode;
    const nama = body.nama_supplier ?? body.nama;
    const telepon = body.telepon ?? null;
    const email = body.email ?? null;
    const alamat = body.alamat ?? null;

    if (!kodeParam)
      return reply.code(400).send({ error: "kode_supplier is required" });
    if (
      kodeBody != null &&
      String(kodeBody).trim() &&
      String(kodeBody).trim() !== kodeParam
    ) {
      return reply.code(400).send({ error: "kode_supplier cannot be changed" });
    }
    if (!nama || typeof nama !== "string" || !nama.trim()) {
      return reply.code(400).send({ error: "nama_supplier is required" });
    }

    const exists = await db.get("SELECT id FROM m_supplier WHERE kode = ?", [
      kodeParam,
    ]);
    if (!exists) return reply.code(404).send({ error: "not found" });

    try {
      await db.run(
        "UPDATE m_supplier SET nama = ?, telepon = ?, email = ?, alamat = ? WHERE kode = ?",
        [nama.trim(), telepon, email, alamat, kodeParam]
      );
      const updated = await db.get(
        "SELECT id, kode AS kode_supplier, nama AS nama_supplier, telepon, email, alamat, created_at, updated_at FROM m_supplier WHERE kode = ?",
        [kodeParam]
      );
      return reply.send(updated);
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });

  fastify.delete("/api/suppliers/:kode", async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? "").trim();
    if (!kodeParam)
      return reply.code(400).send({ error: "kode_supplier is required" });

    const exists = await db.get("SELECT id FROM m_supplier WHERE kode = ?", [
      kodeParam,
    ]);
    if (!exists) return reply.code(404).send({ error: "not found" });

    try {
      await db.run("DELETE FROM m_supplier WHERE kode = ?", [kodeParam]);
      return reply.code(204).send();
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });

  fastify.post("/api/suppliers", async (request, reply) => {
    const body = request.body || {};
    const kode = body.kode_supplier ?? body.kode;
    const nama = body.nama_supplier ?? body.nama;
    const telepon = body.telepon ?? null;
    const email = body.email ?? null;
    const alamat = body.alamat ?? null;

    if (!kode || typeof kode !== "string" || !kode.trim()) {
      return reply.code(400).send({ error: "kode_supplier is required" });
    }

    if (!nama || typeof nama !== "string" || !nama.trim()) {
      return reply.code(400).send({ error: "nama_supplier is required" });
    }

    try {
      const result = await db.run(
        "INSERT INTO m_supplier (kode, nama, telepon, email, alamat) VALUES (?, ?, ?, ?, ?)",
        [kode.trim(), nama.trim(), telepon, email, alamat]
      );
      const created = await db.get(
        "SELECT id, kode AS kode_supplier, nama AS nama_supplier, telepon, email, alamat, created_at, updated_at FROM m_supplier WHERE id = ?",
        [result.lastInsertRowid]
      );
      return reply.code(201).send(created);
    } catch (err) {
      if (err && String(err.message || "").includes("UNIQUE")) {
        return reply.code(409).send({ error: "kode_supplier already exists" });
      }
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });
}

module.exports = { registerSupplierRoutes };
