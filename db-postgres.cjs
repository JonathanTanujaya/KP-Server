const { Pool } = require("pg");

function convertQuestionPlaceholders(sql) {
  // Convert SQLite-style '?' placeholders into PostgreSQL $1, $2, ...
  // This is a small tokenizer to avoid replacing '?' inside string literals.
  let out = "";
  let paramIndex = 0;
  let inSingle = false;
  let inDouble = false;

  for (let i = 0; i < sql.length; i += 1) {
    const ch = sql[i];

    if (ch === "'" && !inDouble) {
      out += ch;
      if (inSingle) {
        // handle doubled '' escape
        if (sql[i + 1] === "'") {
          out += "'";
          i += 1;
        } else {
          inSingle = false;
        }
      } else {
        inSingle = true;
      }
      continue;
    }

    if (ch === '"' && !inSingle) {
      out += ch;
      inDouble = !inDouble;
      continue;
    }

    if (ch === "?" && !inSingle && !inDouble) {
      paramIndex += 1;
      out += `$${paramIndex}`;
      continue;
    }

    out += ch;
  }

  return out;
}

function shouldAppendReturningId(sql) {
  const trimmed = String(sql || "").trim().toUpperCase();
  if (!trimmed.startsWith("INSERT")) return false;
  // If query already has RETURNING, don't touch.
  return !/\bRETURNING\b/i.test(sql);
}

async function ensureSchema(pool) {
  const existsRes = await pool.query(
    "SELECT to_regclass('public.m_user') AS t"
  );
  if (existsRes.rows?.[0]?.t) return;

  const schemaSql = `
CREATE TABLE IF NOT EXISTS m_user (
  id BIGSERIAL PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  nama TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'staff')),
  avatar TEXT,
  must_change_password SMALLINT NOT NULL DEFAULT 0 CHECK (must_change_password IN (0, 1)),
  is_active SMALLINT NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS m_area (
  id BIGSERIAL PRIMARY KEY,
  kode TEXT NOT NULL UNIQUE,
  nama TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS m_kategori (
  id BIGSERIAL PRIMARY KEY,
  kode TEXT NOT NULL UNIQUE,
  nama TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS m_supplier (
  id BIGSERIAL PRIMARY KEY,
  kode TEXT NOT NULL UNIQUE,
  nama TEXT NOT NULL,
  telepon TEXT,
  email TEXT,
  alamat TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS m_customer (
  id BIGSERIAL PRIMARY KEY,
  kode TEXT NOT NULL UNIQUE,
  nama TEXT NOT NULL,
  area_kode TEXT,
  telepon TEXT,
  kontak_person TEXT,
  alamat TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_m_customer_area_kode
    FOREIGN KEY (area_kode) REFERENCES m_area(kode) ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS m_barang (
  id BIGSERIAL PRIMARY KEY,
  kode_barang TEXT NOT NULL UNIQUE,
  nama_barang TEXT NOT NULL,
  kategori_kode TEXT,
  satuan TEXT,
  stok INTEGER NOT NULL DEFAULT 0,
  stok_minimal INTEGER NOT NULL DEFAULT 0,
  harga_beli DOUBLE PRECISION,
  harga_jual DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_m_barang_kategori_kode
    FOREIGN KEY (kategori_kode) REFERENCES m_kategori(kode) ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS t_stok_masuk (
  id BIGSERIAL PRIMARY KEY,
  no_faktur TEXT NOT NULL UNIQUE,
  tanggal DATE NOT NULL,
  supplier_kode TEXT,
  catatan TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_stok_masuk_supplier_kode
    FOREIGN KEY (supplier_kode) REFERENCES m_supplier(kode) ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS t_stok_masuk_detail (
  id BIGSERIAL PRIMARY KEY,
  stok_masuk_id BIGINT NOT NULL,
  barang_kode TEXT NOT NULL,
  qty INTEGER NOT NULL CHECK (qty > 0),
  harga_beli DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_stok_masuk_detail_header
    FOREIGN KEY (stok_masuk_id) REFERENCES t_stok_masuk(id) ON DELETE CASCADE,
  CONSTRAINT fk_t_stok_masuk_detail_barang
    FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_t_stok_masuk_tanggal ON t_stok_masuk(tanggal);
CREATE INDEX IF NOT EXISTS idx_t_stok_masuk_detail_header ON t_stok_masuk_detail(stok_masuk_id);
CREATE INDEX IF NOT EXISTS idx_t_stok_masuk_detail_barang ON t_stok_masuk_detail(barang_kode);

CREATE TABLE IF NOT EXISTS t_stok_keluar (
  id BIGSERIAL PRIMARY KEY,
  no_faktur TEXT NOT NULL UNIQUE,
  tanggal DATE NOT NULL,
  customer_kode TEXT,
  catatan TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_stok_keluar_customer_kode
    FOREIGN KEY (customer_kode) REFERENCES m_customer(kode) ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS t_stok_keluar_detail (
  id BIGSERIAL PRIMARY KEY,
  stok_keluar_id BIGINT NOT NULL,
  barang_kode TEXT NOT NULL,
  qty INTEGER NOT NULL CHECK (qty > 0),
  harga_jual DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_stok_keluar_detail_header
    FOREIGN KEY (stok_keluar_id) REFERENCES t_stok_keluar(id) ON DELETE CASCADE,
  CONSTRAINT fk_t_stok_keluar_detail_barang
    FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_t_stok_keluar_tanggal ON t_stok_keluar(tanggal);
CREATE INDEX IF NOT EXISTS idx_t_stok_keluar_detail_header ON t_stok_keluar_detail(stok_keluar_id);
CREATE INDEX IF NOT EXISTS idx_t_stok_keluar_detail_barang ON t_stok_keluar_detail(barang_kode);

CREATE TABLE IF NOT EXISTS t_stok_opname (
  id BIGSERIAL PRIMARY KEY,
  no_opname TEXT NOT NULL UNIQUE,
  tanggal DATE NOT NULL,
  catatan TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS t_stok_opname_detail (
  id BIGSERIAL PRIMARY KEY,
  stok_opname_id BIGINT NOT NULL,
  barang_kode TEXT NOT NULL,
  stok_sistem INTEGER NOT NULL,
  stok_fisik INTEGER NOT NULL,
  selisih INTEGER NOT NULL,
  keterangan TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_stok_opname_detail_header
    FOREIGN KEY (stok_opname_id) REFERENCES t_stok_opname(id) ON DELETE CASCADE,
  CONSTRAINT fk_t_stok_opname_detail_barang
    FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_t_stok_opname_tanggal ON t_stok_opname(tanggal);
CREATE INDEX IF NOT EXISTS idx_t_stok_opname_detail_header ON t_stok_opname_detail(stok_opname_id);
CREATE INDEX IF NOT EXISTS idx_t_stok_opname_detail_barang ON t_stok_opname_detail(barang_kode);

CREATE TABLE IF NOT EXISTS t_kartu_stok (
  id BIGSERIAL PRIMARY KEY,
  waktu DATE NOT NULL,
  ref_type TEXT NOT NULL,
  ref_no TEXT,
  barang_kode TEXT NOT NULL,
  qty_in INTEGER NOT NULL DEFAULT 0 CHECK (qty_in >= 0),
  qty_out INTEGER NOT NULL DEFAULT 0 CHECK (qty_out >= 0),
  stok_after INTEGER,
  keterangan TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_kartu_stok_barang
    FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_t_kartu_stok_barang_waktu ON t_kartu_stok(barang_kode, waktu);
CREATE INDEX IF NOT EXISTS idx_t_kartu_stok_waktu ON t_kartu_stok(waktu);

CREATE TABLE IF NOT EXISTS t_customer_claim (
  id BIGSERIAL PRIMARY KEY,
  no_claim TEXT NOT NULL UNIQUE,
  tanggal DATE NOT NULL,
  customer_kode TEXT,
  catatan TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_customer_claim_customer_kode
    FOREIGN KEY (customer_kode) REFERENCES m_customer(kode) ON UPDATE CASCADE ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS t_customer_claim_detail (
  id BIGSERIAL PRIMARY KEY,
  customer_claim_id BIGINT NOT NULL,
  barang_kode TEXT NOT NULL,
  qty INTEGER NOT NULL CHECK (qty > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT fk_t_customer_claim_detail_header
    FOREIGN KEY (customer_claim_id) REFERENCES t_customer_claim(id) ON DELETE CASCADE,
  CONSTRAINT fk_t_customer_claim_detail_barang
    FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_t_customer_claim_tanggal ON t_customer_claim(tanggal);
CREATE INDEX IF NOT EXISTS idx_t_customer_claim_detail_header ON t_customer_claim_detail(customer_claim_id);
CREATE INDEX IF NOT EXISTS idx_t_customer_claim_detail_barang ON t_customer_claim_detail(barang_kode);

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_m_user_updated_at ON m_user;
CREATE TRIGGER trg_m_user_updated_at BEFORE UPDATE ON m_user FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_m_area_updated_at ON m_area;
CREATE TRIGGER trg_m_area_updated_at BEFORE UPDATE ON m_area FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_m_kategori_updated_at ON m_kategori;
CREATE TRIGGER trg_m_kategori_updated_at BEFORE UPDATE ON m_kategori FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_m_supplier_updated_at ON m_supplier;
CREATE TRIGGER trg_m_supplier_updated_at BEFORE UPDATE ON m_supplier FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_m_customer_updated_at ON m_customer;
CREATE TRIGGER trg_m_customer_updated_at BEFORE UPDATE ON m_customer FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_m_barang_updated_at ON m_barang;
CREATE TRIGGER trg_m_barang_updated_at BEFORE UPDATE ON m_barang FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_t_stok_masuk_updated_at ON t_stok_masuk;
CREATE TRIGGER trg_t_stok_masuk_updated_at BEFORE UPDATE ON t_stok_masuk FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_t_stok_keluar_updated_at ON t_stok_keluar;
CREATE TRIGGER trg_t_stok_keluar_updated_at BEFORE UPDATE ON t_stok_keluar FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_t_stok_opname_updated_at ON t_stok_opname;
CREATE TRIGGER trg_t_stok_opname_updated_at BEFORE UPDATE ON t_stok_opname FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_t_customer_claim_updated_at ON t_customer_claim;
CREATE TRIGGER trg_t_customer_claim_updated_at BEFORE UPDATE ON t_customer_claim FOR EACH ROW EXECUTE FUNCTION set_updated_at();
`;

  // Best-effort apply in one go.
  await pool.query(schemaSql);
}

function createPgFacade(pool) {
  async function queryOne(sql, params = [], client = null) {
    const text = convertQuestionPlaceholders(sql);
    const runner = client || pool;
    const res = await runner.query({ text, values: params });
    return res.rows?.[0] || null;
  }

  async function queryAll(sql, params = [], client = null) {
    const text = convertQuestionPlaceholders(sql);
    const runner = client || pool;
    const res = await runner.query({ text, values: params });
    return res.rows || [];
  }

  async function exec(sql, params = [], client = null) {
    const text = convertQuestionPlaceholders(sql);
    const runner = client || pool;
    await runner.query({ text, values: params });
  }

  async function run(sql, params = [], client = null) {
    let text = convertQuestionPlaceholders(sql);
    const runner = client || pool;

    if (shouldAppendReturningId(text)) {
      text = `${text} RETURNING id`;
      const res = await runner.query({ text, values: params });
      return {
        changes: res.rowCount || 0,
        lastInsertRowid: res.rows?.[0]?.id ?? null,
      };
    }

    const res = await runner.query({ text, values: params });
    return { changes: res.rowCount || 0 };
  }

  async function transaction(fn) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const tx = {
        get: (sql, params) => queryOne(sql, params, client),
        all: (sql, params) => queryAll(sql, params, client),
        exec: (sql, params) => exec(sql, params, client),
        run: (sql, params) => run(sql, params, client),
      };

      const result = await fn(tx);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {
        // ignore
      }
      throw err;
    } finally {
      client.release();
    }
  }

  return {
    provider: "postgres",
    get: queryOne,
    all: queryAll,
    exec,
    run,
    transaction,
    save: async () => {},
    close: async () => {
      await pool.end();
    },
  };
}

async function initDbPostgres() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required for PostgreSQL");
  }

  const useSsl =
    String(process.env.PGSSLMODE || "").toLowerCase() === "require" ||
    String(process.env.PGSSL || "").toLowerCase() === "true";

  const pool = new Pool({
    connectionString: databaseUrl,
    ...(useSsl ? { ssl: { rejectUnauthorized: false } } : {}),
  });

  await ensureSchema(pool);

  return { db: createPgFacade(pool), dbPath: null, provider: "postgres" };
}

module.exports = { initDbPostgres };
