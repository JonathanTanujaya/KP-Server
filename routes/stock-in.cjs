function parsePositiveInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const intVal = Math.trunc(n);
  if (intVal <= 0) return null;
  return intVal;
}

const MAX_QTY_PER_ITEM = 200;

function registerStockInRoutes(fastify, { db }) {
  fastify.get("/api/stock-in", async (request) => {
    const limit = Math.min(
      500,
      Math.max(1, Number(request.query?.limit ?? 100))
    );
    return await db.all(
      `SELECT id,
              no_faktur,
              tanggal,
              supplier_kode,
              catatan,
              created_at,
              updated_at
       FROM t_stok_masuk
       ORDER BY tanggal DESC, id DESC
       LIMIT ?`,
      [limit]
    );
  });

  fastify.get("/api/stock-in/:id", async (request, reply) => {
    const id = Number(request.params?.id);
    if (!Number.isFinite(id))
      return reply.code(400).send({ error: "id is required" });

    const header = await db.get(
      `SELECT id,
              no_faktur,
              tanggal,
              supplier_kode,
              catatan,
              created_at,
              updated_at
       FROM t_stok_masuk
       WHERE id = ?`,
      [id]
    );
    if (!header) return reply.code(404).send({ error: "not found" });

    const items = await db.all(
      `SELECT d.id,
              d.barang_kode AS kode_barang,
              b.nama_barang,
              b.satuan,
              d.qty AS jumlah,
              d.harga_beli
       FROM t_stok_masuk_detail d
       JOIN m_barang b ON b.kode_barang = d.barang_kode
       WHERE d.stok_masuk_id = ?
       ORDER BY d.id ASC`,
      [id]
    );

    return reply.send({ ...header, items });
  });

  fastify.post("/api/stock-in", async (request, reply) => {
    const body = request.body || {};
    const no_faktur = String(body.no_faktur ?? "").trim();
    const tanggal = String(body.tanggal ?? "").trim();
    const supplier_kode =
      body.kode_supplier != null
        ? String(body.kode_supplier).trim()
        : body.supplier_kode != null
        ? String(body.supplier_kode).trim()
        : null;
    const catatan = body.catatan != null ? String(body.catatan) : null;
    const items = Array.isArray(body.items) ? body.items : [];

    if (!no_faktur)
      return reply.code(400).send({ error: "no_faktur is required" });
    if (!tanggal) return reply.code(400).send({ error: "tanggal is required" });
    if (items.length === 0)
      return reply.code(400).send({ error: "items is required" });

    if (supplier_kode) {
      const sup = await db.get("SELECT id FROM m_supplier WHERE kode = ?", [
        supplier_kode,
      ]);
      if (!sup)
        return reply.code(400).send({ error: "kode_supplier not found" });
    }

    try {
      const result = await db.transaction(async (tx) => {
        const headerRes = await tx.run(
          `INSERT INTO t_stok_masuk (no_faktur, tanggal, supplier_kode, catatan)
           VALUES (?, ?, ?, ?)`,
          [no_faktur, tanggal, supplier_kode || null, catatan]
        );

        const headerId = headerRes.lastInsertRowid;

        for (const it of items) {
          const kode_barang = String(it.kode_barang ?? "").trim();
          const jumlah = parsePositiveInt(it.jumlah ?? it.qty);
          const harga_beli = it.harga_beli ?? null;

          if (!kode_barang) throw new Error("kode_barang is required");
          if (!jumlah) throw new Error("jumlah must be > 0");
          if (jumlah > MAX_QTY_PER_ITEM) {
            const e = new Error(
              `jumlah maksimal ${MAX_QTY_PER_ITEM} per item per transaksi`
            );
            e.statusCode = 400;
            e.code = "QTY_LIMIT";
            e.meta = { kode_barang, jumlah, max: MAX_QTY_PER_ITEM };
            throw e;
          }

          const barang = await tx.get(
            "SELECT stok FROM m_barang WHERE kode_barang = ?",
            [kode_barang]
          );
          if (!barang) {
            const e = new Error("kode_barang not found");
            e.statusCode = 400;
            e.code = "ITEM_NOT_FOUND";
            e.meta = { kode_barang };
            throw e;
          }

          await tx.run(
            `INSERT INTO t_stok_masuk_detail (stok_masuk_id, barang_kode, qty, harga_beli)
             VALUES (?, ?, ?, ?)`,
            [headerId, kode_barang, jumlah, harga_beli]
          );

          await tx.run("UPDATE m_barang SET stok = stok + ? WHERE kode_barang = ?", [
            jumlah,
            kode_barang,
          ]);

          const after = await tx.get(
            "SELECT stok FROM m_barang WHERE kode_barang = ?",
            [kode_barang]
          );
          await tx.run(
            `INSERT INTO t_kartu_stok (waktu, ref_type, ref_no, barang_kode, qty_in, qty_out, stok_after, keterangan)
             VALUES (?, 'IN', ?, ?, ?, 0, ?, ?)`,
            [
              tanggal,
              no_faktur,
              kode_barang,
              jumlah,
              after?.stok ?? null,
              catatan,
            ]
          );
        }

        return { headerId };
      });

      const created = await db.get(
        `SELECT id,
                no_faktur,
                tanggal,
                supplier_kode,
                catatan,
                created_at,
                updated_at
         FROM t_stok_masuk
         WHERE id = ?`,
        [result.headerId]
      );

      return reply.code(201).send(created);
    } catch (err) {
      const msg = String(err?.message || "internal error");
      if (msg.includes("UNIQUE"))
        return reply.code(409).send({ error: "no_faktur already exists" });
      if (err?.statusCode)
        return reply
          .code(err.statusCode)
          .send({ error: msg, ...(err.meta ? { meta: err.meta } : {}) });
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });
}

module.exports = { registerStockInRoutes };
