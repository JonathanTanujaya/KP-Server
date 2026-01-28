const fs = require("fs");
const path = require("path");

// Seed data embedded directly (untuk PostgreSQL yang tidak punya akses ke file system)
const SEED_DATA = {
  areas: [
    { kode: "AREA001", nama: "Jakarta Pusat" },
    { kode: "AREA002", nama: "Jakarta Selatan" },
    { kode: "AREA003", nama: "Jakarta Barat" },
    { kode: "AREA004", nama: "Jakarta Timur" },
    { kode: "AREA005", nama: "Jakarta Utara" },
    { kode: "AREA006", nama: "Tangerang" },
    { kode: "AREA007", nama: "Tangerang Selatan" },
    { kode: "AREA008", nama: "Bekasi" },
    { kode: "AREA009", nama: "Depok" },
    { kode: "AREA010", nama: "Bogor" },
  ],
  kategori: [
    { kode: "KAT001", nama: "Bearing & Filter" },
    { kode: "KAT002", nama: "Body & Kabel" },
    { kode: "KAT003", nama: "Transmisi" },
    { kode: "KAT004", nama: "Oli & Pelumas" },
    { kode: "KAT005", nama: "Elektrikal" },
    { kode: "KAT006", nama: "Ban & Velg" },
    { kode: "KAT007", nama: "Rem" },
    { kode: "KAT008", nama: "Suspensi" },
    { kode: "KAT009", nama: "Mesin" },
    { kode: "KAT010", nama: "Knalpot" },
  ],
  suppliers: [
    { kode: "SUP001", nama: "PT Supplier Jaya", alamat: "Jl. Industri No. 123, Jakarta Utara", telepon: "021-1234567", email: null },
    { kode: "SUP002", nama: "CV Maju Bersama", alamat: "Jl. Maju No. 456, Bandung", telepon: "022-7654321", email: null },
    { kode: "SUP003", nama: "PT Berkah Selalu", alamat: "Jl. Berkah No. 789, Surabaya", telepon: "031-9876543", email: null },
    { kode: "SUP004", nama: "PT Honda Genuine Parts", alamat: "Jl. Sunter Permai No. 10, Jakarta Utara", telepon: "021-6501234", email: null },
    { kode: "SUP005", nama: "PT Yamaha Indonesia Motor", alamat: "Jl. Pulo Gadung No. 55, Jakarta Timur", telepon: "021-4601234", email: null },
  ],
  customers: [
    { kode: "CUST001", nama: "Bengkel Maju Jaya", area_kode: "AREA001", telepon: "021-1111111", kontak_person: "Pak Budi", alamat: "Jl. Kebon Jeruk No. 10" },
    { kode: "CUST002", nama: "Bengkel Sentosa Motor", area_kode: "AREA002", telepon: "021-2222222", kontak_person: "Pak Andi", alamat: "Jl. Fatmawati No. 20" },
    { kode: "CUST003", nama: "Toko Sparepart ABC", area_kode: "AREA003", telepon: "021-3333333", kontak_person: "Bu Siti", alamat: "Jl. Daan Mogot No. 100" },
    { kode: "CUST004", nama: "Bengkel Berkah", area_kode: "AREA004", telepon: "021-4444444", kontak_person: "Pak Joko", alamat: "Jl. Kalimalang No. 50" },
    { kode: "CUST005", nama: "Motor Sport Center", area_kode: "AREA005", telepon: "021-5555555", kontak_person: "Pak Rudi", alamat: "Jl. Pluit Raya No. 30" },
  ],
  barang: [
    { kode: "BRG001", nama: "Bearing 6301", kategori_kode: "KAT001", satuan: "pcs", stok: 100, stok_minimal: 10, harga_beli: 25000, harga_jual: 35000 },
    { kode: "BRG002", nama: "Bearing 6302", kategori_kode: "KAT001", satuan: "pcs", stok: 80, stok_minimal: 10, harga_beli: 30000, harga_jual: 42000 },
    { kode: "BRG003", nama: "Filter Oli Honda", kategori_kode: "KAT001", satuan: "pcs", stok: 50, stok_minimal: 5, harga_beli: 15000, harga_jual: 25000 },
    { kode: "BRG004", nama: "Kabel Gas Vario", kategori_kode: "KAT002", satuan: "pcs", stok: 30, stok_minimal: 5, harga_beli: 35000, harga_jual: 50000 },
    { kode: "BRG005", nama: "Kabel Kopling Beat", kategori_kode: "KAT002", satuan: "pcs", stok: 25, stok_minimal: 5, harga_beli: 28000, harga_jual: 40000 },
    { kode: "BRG006", nama: "Gear Set Supra", kategori_kode: "KAT003", satuan: "set", stok: 20, stok_minimal: 3, harga_beli: 150000, harga_jual: 200000 },
    { kode: "BRG007", nama: "Oli Mesin 1L", kategori_kode: "KAT004", satuan: "liter", stok: 200, stok_minimal: 20, harga_beli: 45000, harga_jual: 60000 },
    { kode: "BRG008", nama: "Oli Gardan 150ml", kategori_kode: "KAT004", satuan: "pcs", stok: 100, stok_minimal: 10, harga_beli: 18000, harga_jual: 28000 },
    { kode: "BRG009", nama: "CDI Racing Vario", kategori_kode: "KAT005", satuan: "pcs", stok: 15, stok_minimal: 2, harga_beli: 85000, harga_jual: 120000 },
    { kode: "BRG010", nama: "Kiprok Tiger", kategori_kode: "KAT005", satuan: "pcs", stok: 10, stok_minimal: 2, harga_beli: 95000, harga_jual: 135000 },
    { kode: "BRG011", nama: "Ban Luar 70/90-17", kategori_kode: "KAT006", satuan: "pcs", stok: 40, stok_minimal: 5, harga_beli: 120000, harga_jual: 165000 },
    { kode: "BRG012", nama: "Ban Dalam 17", kategori_kode: "KAT006", satuan: "pcs", stok: 60, stok_minimal: 10, harga_beli: 25000, harga_jual: 38000 },
    { kode: "BRG013", nama: "Kampas Rem Depan Vario", kategori_kode: "KAT007", satuan: "set", stok: 35, stok_minimal: 5, harga_beli: 35000, harga_jual: 55000 },
    { kode: "BRG014", nama: "Kampas Rem Belakang Beat", kategori_kode: "KAT007", satuan: "set", stok: 40, stok_minimal: 5, harga_beli: 28000, harga_jual: 45000 },
    { kode: "BRG015", nama: "Shock Depan Supra", kategori_kode: "KAT008", satuan: "pcs", stok: 12, stok_minimal: 2, harga_beli: 180000, harga_jual: 250000 },
  ],
};

function requireOwner(session, reply) {
  if (!session) return false;
  if (session.user.role !== "owner") {
    reply.code(403).send({ error: "forbidden" });
    return false;
  }
  return true;
}

function safeTimestampForFilename(d = new Date()) {
  return d
    .toISOString()
    .replace(/[:.]/g, "-")
    .replace("T", "_")
    .replace("Z", "");
}

function looksLikeSqliteFile(buf) {
  if (!Buffer.isBuffer(buf)) return false;
  if (buf.length < 16) return false;
  // SQLite header: "SQLite format 3\000"
  return buf.subarray(0, 16).toString("utf8") === "SQLite format 3\u0000";
}

function looksLikeSqlText(text) {
  const s = String(text || "").trim().toUpperCase();
  if (!s) return false;
  // Heuristics for SQLite/SQL dumps.
  return (
    s.includes("CREATE TABLE") ||
    s.includes("INSERT INTO") ||
    s.includes("BEGIN TRANSACTION") ||
    s.includes("COMMIT") ||
    s.startsWith("PRAGMA ")
  );
}

async function readUploadedPayload(request) {
  // Supports:
  // - multipart/form-data (browser file upload)
  // - raw bytes (application/octet-stream)
  // - raw text (text/plain, application/sql)
  if (typeof request.isMultipart === "function" && request.isMultipart()) {
    const part = await request.file();
    if (!part) return null;
    const buffer = await part.toBuffer();
    return {
      kind: "multipart",
      buffer,
      filename: part.filename,
      mimetype: part.mimetype,
    };
  }

  const body = request.body;
  if (Buffer.isBuffer(body)) {
    return { kind: "buffer", buffer: body, filename: null, mimetype: request.headers["content-type"] };
  }
  if (typeof body === "string") {
    return { kind: "text", text: body, filename: null, mimetype: request.headers["content-type"] };
  }

  return null;
}

function escapeSqliteIdent(name) {
  return `"${String(name || "").replace(/"/g, '""')}"`;
}

async function restoreSqliteFromSql({ db, sqlText }) {
  const text = String(sqlText || "").trim();
  if (!text) {
    const err = new Error("empty sql");
    err.code = "EMPTY_SQL";
    throw err;
  }

  // Drop all objects (tables, triggers, views, indexes) before running the SQL script.
  // Keep it outside an explicit transaction to avoid clashes if the SQL script contains BEGIN/COMMIT.
  db.exec("PRAGMA foreign_keys = OFF;");

  const objects = db.all(
    "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' AND type IN ('table','index','trigger','view')"
  );

  // Drop non-table objects first (usually depends on tables).
  const order = { trigger: 1, view: 2, index: 3, table: 4 };
  objects
    .sort((a, b) => (order[a.type] || 99) - (order[b.type] || 99))
    .forEach((obj) => {
      const type = String(obj.type || "").toUpperCase();
      const name = escapeSqliteIdent(obj.name);
      if (type === "TABLE") {
        db.exec(`DROP TABLE IF EXISTS ${name};`);
      } else if (type === "INDEX") {
        db.exec(`DROP INDEX IF EXISTS ${name};`);
      } else if (type === "TRIGGER") {
        db.exec(`DROP TRIGGER IF EXISTS ${name};`);
      } else if (type === "VIEW") {
        db.exec(`DROP VIEW IF EXISTS ${name};`);
      }
    });

  db.exec("PRAGMA foreign_keys = ON;");
  db.exec(text);
}

function isLoopbackIp(ip) {
  const v = String(ip || "").trim();
  return v === "127.0.0.1" || v === "::1" || v === "::ffff:127.0.0.1";
}

function requireSqlite(fastify, reply) {
  if (fastify.dbProvider && fastify.dbProvider !== "sqlite") {
    reply.code(400).send({ error: "not supported for this db provider" });
    return false;
  }
  return true;
}

async function canRunFirstRunDbSetup({ fastify, db, request, reply }) {
  if (!requireSqlite(fastify, reply)) return false;
  if (!isLoopbackIp(request.ip)) {
    reply.code(403).send({ error: "forbidden" });
    return false;
  }

  const row = await db.get("SELECT COUNT(1) AS userCount FROM m_user");
  const userCount = Number(row?.userCount || 0);
  if (userCount > 0) {
    reply.code(409).send({ error: "bootstrap already completed" });
    return false;
  }

  // Also block if static is exposed publicly (still local by default), but loopback check is primary.
  return true;
}

function touchNoSeedMarker(dbPath) {
  try {
    const markerPath = path.join(path.dirname(dbPath), "stoir.no-seed");
    fs.writeFileSync(markerPath, "1", "utf8");
    return markerPath;
  } catch {
    return null;
  }
}

function registerDbToolsRoutes(fastify, { db }) {
  // Owner-only DB maintenance endpoints.

  // First-run (pre-login) DB setup endpoints.
  // Only allowed on loopback and only when DB has no users (bootstrap not completed).
  fastify.get("/api/setup/db/status", async (request, reply) => {
    if (!(await canRunFirstRunDbSetup({ fastify, db, request, reply }))) return;

    const dbPath = fastify.dbPath;
    let size = null;
    let exists = false;
    try {
      const st = fs.statSync(dbPath);
      exists = st.isFile();
      size = st.size;
    } catch {
      exists = false;
      size = null;
    }

    return reply.send({
      ok: true,
      dbPath,
      exists,
      size,
      canRestore: true,
      canCreateNew: true,
      requiresRestartAfterChange: true,
    });
  });

  // Create a fresh DB by moving the current file aside. Requires app restart to reload sql.js.
  fastify.post("/api/setup/db/new", async (request, reply) => {
    if (!(await canRunFirstRunDbSetup({ fastify, db, request, reply }))) return;

    // Flush latest in-memory DB to disk first.
    try {
      await db.save?.();
    } catch (_) {
      // ignore
    }

    const dbPath = fastify.dbPath;
    const ts = safeTimestampForFilename();
    const oldPath = `${dbPath}.old-${ts}`;

    const noSeedMarkerPath = touchNoSeedMarker(dbPath);
    // Prevent shutdown from saving the old in-memory DB back to disk.
    fastify.skipDbSaveOnClose = true;

    try {
      if (fs.existsSync(dbPath)) {
        fs.renameSync(dbPath, oldPath);
      }
    } catch (err) {
      return reply.code(500).send({
        error: "failed to reset db",
        detail: String(err?.message || err),
      });
    }

    return reply.send({
      ok: true,
      movedOldDbTo: fs.existsSync(oldPath) ? oldPath : null,
      noSeedMarkerPath,
      requiresRestart: true,
      message: "Database baru akan dibuat setelah aplikasi direstart.",
    });
  });

  // Restore DB from raw bytes (pre-login). Requires app restart to reload sql.js database.
  fastify.post(
    "/api/setup/db/restore",
    { bodyLimit: 50 * 1024 * 1024 },
    async (request, reply) => {
      if (!(await canRunFirstRunDbSetup({ fastify, db, request, reply }))) return;

      const payload = await readUploadedPayload(request);
      if (!payload) {
        return reply.code(400).send({ error: "empty upload" });
      }

      // Accept .sql restore (in-memory, no restart needed)
      if (payload.kind === "text") {
        try {
          await restoreSqliteFromSql({ db, sqlText: payload.text });
        } catch (err) {
          return reply.code(400).send({
            error: "invalid sql",
            detail: String(err?.message || err),
          });
        }

        const noSeedMarkerPath = touchNoSeedMarker(fastify.dbPath);
        return reply.send({
          ok: true,
          noSeedMarkerPath,
          requiresRestart: false,
          message: "Restore SQL selesai. Database sudah aktif tanpa restart.",
        });
      }

      const body = payload.buffer;
      if (!Buffer.isBuffer(body) || body.length === 0) {
        return reply.code(400).send({ error: "empty upload" });
      }

      if (!looksLikeSqliteFile(body)) {
        const filename = String(payload.filename || "").toLowerCase();
        const mimetype = String(payload.mimetype || "").toLowerCase();
        const asText = body.toString("utf8");
        if (
          filename.endsWith(".sql") ||
          mimetype.includes("sql") ||
          mimetype.startsWith("text/") ||
          looksLikeSqlText(asText)
        ) {
          try {
            await restoreSqliteFromSql({ db, sqlText: asText });
          } catch (err) {
            return reply.code(400).send({
              error: "invalid sql",
              detail: String(err?.message || err),
            });
          }

          const noSeedMarkerPath = touchNoSeedMarker(fastify.dbPath);
          return reply.send({
            ok: true,
            noSeedMarkerPath,
            requiresRestart: false,
            message: "Restore SQL selesai. Database sudah aktif tanpa restart.",
          });
        }

        return reply.code(400).send({ error: "invalid sqlite file" });
      }

      // Flush latest in-memory DB to disk first.
      try {
        await db.save?.();
      } catch (_) {
        // ignore
      }

      const dbPath = fastify.dbPath;
      const ts = safeTimestampForFilename();

      // Prevent shutdown from saving the old in-memory DB back to disk.
      fastify.skipDbSaveOnClose = true;

      // Safety: make an automatic backup before overwrite.
      let backupPath = null;
      try {
        backupPath = `${dbPath}.bak-${ts}`;
        fs.copyFileSync(dbPath, backupPath);
      } catch {
        backupPath = null;
      }

      const tmpPath = `${dbPath}.restore-tmp-${ts}`;
      fs.writeFileSync(tmpPath, body);

      // Atomic-ish replace on Windows: rename old to .old, move tmp into place.
      const oldPath = `${dbPath}.old-${ts}`;
      try {
        if (fs.existsSync(dbPath)) {
          fs.renameSync(dbPath, oldPath);
        }
        fs.renameSync(tmpPath, dbPath);
      } catch (err) {
        // Best-effort cleanup
        try {
          if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
        } catch (_) {
          // ignore
        }
        return reply.code(500).send({
          error: "failed to restore db",
          detail: String(err?.message || err),
        });
      }

      const noSeedMarkerPath = touchNoSeedMarker(dbPath);

      return reply.send({
        ok: true,
        backupPath,
        noSeedMarkerPath,
        requiresRestart: true,
        message:
          "Restore selesai. Aplikasi akan direstart agar database baru terbaca.",
      });
    }
  );

  fastify.get("/api/admin/db/info", async (request, reply) => {
    if (fastify.dbProvider && fastify.dbProvider !== "sqlite") {
      return reply.send({
        supported: false,
        provider: fastify.dbProvider,
        isPackaged: Boolean(fastify.isPackaged),
      });
    }
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const dbPath = fastify.dbPath;
    let size = null;
    try {
      size = fs.statSync(dbPath).size;
    } catch {
      size = null;
    }

    return reply.send({
      supported: true,
      dbPath,
      size,
      isPackaged: Boolean(fastify.isPackaged),
      requiresRestartAfterRestore: true,
    });
  });

  fastify.get("/api/admin/db/backup", async (request, reply) => {
    if (!requireSqlite(fastify, reply)) return;
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    // Flush latest in-memory DB to disk first.
    try {
      await db.save?.();
    } catch (_) {
      // ignore
    }

    const dbPath = fastify.dbPath;
    const filename = `stoir-backup-${safeTimestampForFilename()}.sqlite`;

    const data = fs.readFileSync(dbPath);

    reply.header("Content-Type", "application/x-sqlite3");
    reply.header("Content-Disposition", `attachment; filename=\"${filename}\"`);
    return reply.send(data);
  });

  // Restore DB from raw bytes. Requires app restart to reload sql.js database.
  fastify.post("/api/admin/db/restore", async (request, reply) => {
    if (fastify.dbProvider && fastify.dbProvider !== "sqlite") {
      // Avoid noisy 400s on frontends that probe this endpoint.
      return reply.send({
        ok: false,
        supported: false,
        provider: fastify.dbProvider,
        error: "not supported for this db provider",
      });
    }
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const payload = await readUploadedPayload(request);
    if (!payload) {
      // Return 200 so dashboard probes don't show console errors.
      return reply.send({ ok: false, error: "empty upload" });
    }

    // Accept .sql restore (in-memory, no restart needed)
    if (payload.kind === "text") {
      try {
        await restoreSqliteFromSql({ db, sqlText: payload.text });
      } catch (err) {
        return reply.code(400).send({
          error: "invalid sql",
          detail: String(err?.message || err),
        });
      }

      const noSeedMarkerPath = touchNoSeedMarker(fastify.dbPath);
      return reply.send({
        ok: true,
        noSeedMarkerPath,
        requiresRestart: false,
        message: "Restore SQL selesai. Database sudah aktif tanpa restart.",
      });
    }

    const body = payload.buffer;
    if (!Buffer.isBuffer(body) || body.length === 0) {
      return reply.send({ ok: false, error: "empty upload" });
    }

    if (!looksLikeSqliteFile(body)) {
      const filename = String(payload.filename || "").toLowerCase();
      const mimetype = String(payload.mimetype || "").toLowerCase();
      const asText = body.toString("utf8");
      if (
        filename.endsWith(".sql") ||
        mimetype.includes("sql") ||
        mimetype.startsWith("text/") ||
        looksLikeSqlText(asText)
      ) {
        try {
          await restoreSqliteFromSql({ db, sqlText: asText });
        } catch (err) {
          return reply.code(400).send({
            error: "invalid sql",
            detail: String(err?.message || err),
          });
        }

        const noSeedMarkerPath = touchNoSeedMarker(fastify.dbPath);
        return reply.send({
          ok: true,
          noSeedMarkerPath,
          requiresRestart: false,
          message: "Restore SQL selesai. Database sudah aktif tanpa restart.",
        });
      }

      return reply.code(400).send({ error: "invalid sqlite file" });
    }

    const dbPath = fastify.dbPath;
    const ts = safeTimestampForFilename();

    // Prevent shutdown from saving the old in-memory DB back to disk.
    fastify.skipDbSaveOnClose = true;

    // Safety: make an automatic backup before overwrite.
    let backupPath = null;
    try {
      backupPath = `${dbPath}.bak-${ts}`;
      fs.copyFileSync(dbPath, backupPath);
    } catch {
      backupPath = null;
    }

    const tmpPath = `${dbPath}.restore-tmp-${ts}`;
    fs.writeFileSync(tmpPath, body);

    // Atomic-ish replace on Windows: rename old to .old, move tmp into place.
    const oldPath = `${dbPath}.old-${ts}`;
    try {
      if (fs.existsSync(dbPath)) {
        fs.renameSync(dbPath, oldPath);
      }
      fs.renameSync(tmpPath, dbPath);
    } catch (err) {
      // Best-effort cleanup
      try {
        if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      } catch (_) {
        // ignore
      }
      return reply.code(500).send({
        error: "failed to restore db",
        detail: String(err?.message || err),
      });
    }

    const noSeedMarkerPath = touchNoSeedMarker(dbPath);

    return reply.send({
      ok: true,
      backupPath,
      noSeedMarkerPath,
      requiresRestart: true,
      message:
        "Restore selesai. Aplikasi harus direstart agar database baru terbaca.",
    });
  });

  // Reset database - works for both SQLite and PostgreSQL
  // For SQLite: rotates the DB file (requires restart)
  // For PostgreSQL: truncates all tables (no restart needed)
  fastify.post("/api/admin/db/reset", async (request, reply) => {
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const isPostgres = fastify.dbProvider === "postgres";

    if (isPostgres) {
      // PostgreSQL: TRUNCATE all tables in correct order (respect FK constraints)
      try {
        // Order matters due to foreign key constraints
        // First: detail/child tables, then header/parent tables, finally master tables
        const truncateOrder = [
          "t_stok_opname_detail",
          "t_stok_opname",
          "t_customer_claim_detail",
          "t_customer_claim",
          "t_stok_keluar_detail",
          "t_stok_keluar",
          "t_stok_masuk_detail",
          "t_stok_masuk",
          "t_kartu_stok",
          "m_barang",
          "m_customer",
          "m_supplier",
          "m_kategori",
          "m_area",
          "m_user",
        ];

        for (const table of truncateOrder) {
          try {
            await db.exec(`TRUNCATE TABLE ${table} CASCADE`);
          } catch (e) {
            // Table might not exist, ignore
            fastify.log.warn({ table, error: e.message }, "Truncate warning");
          }
        }

        return reply.send({
          ok: true,
          provider: "postgres",
          requiresRestart: false,
          message: "Database PostgreSQL berhasil direset. Semua data dihapus.",
        });
      } catch (err) {
        fastify.log.error(err);
        return reply.code(500).send({
          error: "failed to reset postgres db",
          detail: String(err?.message || err),
        });
      }
    }

    // SQLite: original file-based reset logic
    if (fastify.isPackaged) {
      return reply.code(403).send({ error: "dev only" });
    }

    // Flush latest in-memory DB to disk first.
    try {
      await db.save?.();
    } catch (_) {
      // ignore
    }

    const dbPath = fastify.dbPath;
    const ts = safeTimestampForFilename();
    const deletedPath = `${dbPath}.deleted-${ts}`;

    const noSeedMarkerPath = touchNoSeedMarker(dbPath);
    // Prevent shutdown from saving the old in-memory DB back to disk.
    fastify.skipDbSaveOnClose = true;

    try {
      if (fs.existsSync(dbPath)) {
        fs.renameSync(dbPath, deletedPath);
      }
    } catch (err) {
      return reply.code(500).send({
        error: "failed to reset db",
        detail: String(err?.message || err),
      });
    }

    return reply.send({
      ok: true,
      deletedPath: fs.existsSync(deletedPath) ? deletedPath : null,
      noSeedMarkerPath,
      requiresRestart: true,
      message:
        "Database dihapus (dipindahkan). Aplikasi harus direstart untuk memulai dari awal.",
    });
  });

  // Seed database with demo data (works for both SQLite and PostgreSQL)
  fastify.post("/api/admin/db/seed", async (request, reply) => {
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const isPostgres = fastify.dbProvider === "postgres";
    const results = { areas: 0, kategori: 0, suppliers: 0, customers: 0, barang: 0 };

    try {
      // Seed Areas
      for (const row of SEED_DATA.areas) {
        try {
          if (isPostgres) {
            await db.run(
              `INSERT INTO m_area (kode, nama) VALUES (?, ?) ON CONFLICT (kode) DO NOTHING`,
              [row.kode, row.nama]
            );
          } else {
            await db.run(
              `INSERT OR IGNORE INTO m_area (kode, nama) VALUES (?, ?)`,
              [row.kode, row.nama]
            );
          }
          results.areas++;
        } catch (e) {
          fastify.log.warn({ error: e.message }, "Seed area warning");
        }
      }

      // Seed Kategori
      for (const row of SEED_DATA.kategori) {
        try {
          if (isPostgres) {
            await db.run(
              `INSERT INTO m_kategori (kode, nama) VALUES (?, ?) ON CONFLICT (kode) DO NOTHING`,
              [row.kode, row.nama]
            );
          } else {
            await db.run(
              `INSERT OR IGNORE INTO m_kategori (kode, nama) VALUES (?, ?)`,
              [row.kode, row.nama]
            );
          }
          results.kategori++;
        } catch (e) {
          fastify.log.warn({ error: e.message }, "Seed kategori warning");
        }
      }

      // Seed Suppliers
      for (const row of SEED_DATA.suppliers) {
        try {
          if (isPostgres) {
            await db.run(
              `INSERT INTO m_supplier (kode, nama, alamat, telepon, email) VALUES (?, ?, ?, ?, ?) ON CONFLICT (kode) DO NOTHING`,
              [row.kode, row.nama, row.alamat, row.telepon, row.email]
            );
          } else {
            await db.run(
              `INSERT OR IGNORE INTO m_supplier (kode, nama, alamat, telepon, email) VALUES (?, ?, ?, ?, ?)`,
              [row.kode, row.nama, row.alamat, row.telepon, row.email]
            );
          }
          results.suppliers++;
        } catch (e) {
          fastify.log.warn({ error: e.message }, "Seed supplier warning");
        }
      }

      // Seed Customers
      for (const row of SEED_DATA.customers) {
        try {
          if (isPostgres) {
            await db.run(
              `INSERT INTO m_customer (kode, nama, area_kode, telepon, kontak_person, alamat) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (kode) DO NOTHING`,
              [row.kode, row.nama, row.area_kode, row.telepon, row.kontak_person, row.alamat]
            );
          } else {
            await db.run(
              `INSERT OR IGNORE INTO m_customer (kode, nama, area_kode, telepon, kontak_person, alamat) VALUES (?, ?, ?, ?, ?, ?)`,
              [row.kode, row.nama, row.area_kode, row.telepon, row.kontak_person, row.alamat]
            );
          }
          results.customers++;
        } catch (e) {
          fastify.log.warn({ error: e.message }, "Seed customer warning");
        }
      }

      // Seed Barang
      for (const row of SEED_DATA.barang) {
        try {
          if (isPostgres) {
            await db.run(
              `INSERT INTO m_barang (kode_barang, nama_barang, kategori_kode, satuan, stok, stok_minimal, harga_beli, harga_jual) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (kode_barang) DO NOTHING`,
              [row.kode, row.nama, row.kategori_kode, row.satuan, row.stok, row.stok_minimal, row.harga_beli, row.harga_jual]
            );
          } else {
            await db.run(
              `INSERT OR IGNORE INTO m_barang (kode_barang, nama_barang, kategori_kode, satuan, stok, stok_minimal, harga_beli, harga_jual) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
              [row.kode, row.nama, row.kategori_kode, row.satuan, row.stok, row.stok_minimal, row.harga_beli, row.harga_jual]
            );
          }
          results.barang++;
        } catch (e) {
          fastify.log.warn({ error: e.message }, "Seed barang warning");
        }
      }

      return reply.send({
        ok: true,
        provider: isPostgres ? "postgres" : "sqlite",
        seeded: results,
        message: `Data demo berhasil ditambahkan: ${results.areas} area, ${results.kategori} kategori, ${results.suppliers} supplier, ${results.customers} customer, ${results.barang} barang.`,
      });
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({
        error: "failed to seed db",
        detail: String(err?.message || err),
      });
    }
  });
}

module.exports = { registerDbToolsRoutes };
