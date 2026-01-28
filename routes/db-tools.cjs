const fs = require("fs");
const path = require("path");

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
}

module.exports = { registerDbToolsRoutes };
