const fs = require("fs");
const path = require("path");
const initSqlJs = require("sql.js");

const { initDbPostgres } = require("./db-postgres.cjs");

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function migrate(sqlDb) {
  sqlDb.exec(`
    CREATE TABLE IF NOT EXISTS m_user (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      nama TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role IN ('owner', 'admin', 'staff')),
      avatar TEXT,
      must_change_password INTEGER NOT NULL DEFAULT 0 CHECK (must_change_password IN (0, 1)),
      is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TRIGGER IF NOT EXISTS trg_m_user_updated_at
    AFTER UPDATE ON m_user
    FOR EACH ROW
    BEGIN
      UPDATE m_user
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TABLE IF NOT EXISTS m_area (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kode TEXT NOT NULL UNIQUE,
      nama TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS m_kategori (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kode TEXT NOT NULL UNIQUE,
      nama TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS m_supplier (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kode TEXT NOT NULL UNIQUE,
      nama TEXT NOT NULL,
      telepon TEXT,
      email TEXT,
      alamat TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS m_customer (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kode TEXT NOT NULL UNIQUE,
      nama TEXT NOT NULL,
      area_kode TEXT,
      telepon TEXT,
      kontak_person TEXT,
      alamat TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (area_kode) REFERENCES m_area(kode) ON UPDATE CASCADE ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS m_barang (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      kode_barang TEXT NOT NULL UNIQUE,
      nama_barang TEXT NOT NULL,
      kategori_kode TEXT,
      satuan TEXT,
      stok INTEGER NOT NULL DEFAULT 0,
      stok_minimal INTEGER NOT NULL DEFAULT 0,
      harga_beli REAL,
      harga_jual REAL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (kategori_kode) REFERENCES m_kategori(kode) ON UPDATE CASCADE ON DELETE SET NULL
    );

    CREATE TRIGGER IF NOT EXISTS trg_m_area_updated_at
    AFTER UPDATE ON m_area
    FOR EACH ROW
    BEGIN
      UPDATE m_area
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_m_kategori_updated_at
    AFTER UPDATE ON m_kategori
    FOR EACH ROW
    BEGIN
      UPDATE m_kategori
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_m_supplier_updated_at
    AFTER UPDATE ON m_supplier
    FOR EACH ROW
    BEGIN
      UPDATE m_supplier
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_m_customer_updated_at
    AFTER UPDATE ON m_customer
    FOR EACH ROW
    BEGIN
      UPDATE m_customer
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TRIGGER IF NOT EXISTS trg_m_barang_updated_at
    AFTER UPDATE ON m_barang
    FOR EACH ROW
    BEGIN
      UPDATE m_barang
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TABLE IF NOT EXISTS t_stok_masuk (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      no_faktur TEXT NOT NULL UNIQUE,
      tanggal TEXT NOT NULL,
      supplier_kode TEXT,
      catatan TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (supplier_kode) REFERENCES m_supplier(kode) ON UPDATE CASCADE ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS t_stok_masuk_detail (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stok_masuk_id INTEGER NOT NULL,
      barang_kode TEXT NOT NULL,
      qty INTEGER NOT NULL CHECK (qty > 0),
      harga_beli REAL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (stok_masuk_id) REFERENCES t_stok_masuk(id) ON DELETE CASCADE,
      FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
    );

    CREATE INDEX IF NOT EXISTS idx_t_stok_masuk_tanggal ON t_stok_masuk(tanggal);
    CREATE INDEX IF NOT EXISTS idx_t_stok_masuk_detail_header ON t_stok_masuk_detail(stok_masuk_id);
    CREATE INDEX IF NOT EXISTS idx_t_stok_masuk_detail_barang ON t_stok_masuk_detail(barang_kode);

    CREATE TRIGGER IF NOT EXISTS trg_t_stok_masuk_updated_at
    AFTER UPDATE ON t_stok_masuk
    FOR EACH ROW
    BEGIN
      UPDATE t_stok_masuk
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TABLE IF NOT EXISTS t_stok_keluar (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      no_faktur TEXT NOT NULL UNIQUE,
      tanggal TEXT NOT NULL,
      customer_kode TEXT,
      catatan TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (customer_kode) REFERENCES m_customer(kode) ON UPDATE CASCADE ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS t_stok_keluar_detail (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stok_keluar_id INTEGER NOT NULL,
      barang_kode TEXT NOT NULL,
      qty INTEGER NOT NULL CHECK (qty > 0),
      harga_jual REAL,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (stok_keluar_id) REFERENCES t_stok_keluar(id) ON DELETE CASCADE,
      FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
    );

    CREATE INDEX IF NOT EXISTS idx_t_stok_keluar_tanggal ON t_stok_keluar(tanggal);
    CREATE INDEX IF NOT EXISTS idx_t_stok_keluar_detail_header ON t_stok_keluar_detail(stok_keluar_id);
    CREATE INDEX IF NOT EXISTS idx_t_stok_keluar_detail_barang ON t_stok_keluar_detail(barang_kode);

    CREATE TRIGGER IF NOT EXISTS trg_t_stok_keluar_updated_at
    AFTER UPDATE ON t_stok_keluar
    FOR EACH ROW
    BEGIN
      UPDATE t_stok_keluar
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TABLE IF NOT EXISTS t_stok_opname (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      no_opname TEXT NOT NULL UNIQUE,
      tanggal TEXT NOT NULL,
      catatan TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS t_stok_opname_detail (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stok_opname_id INTEGER NOT NULL,
      barang_kode TEXT NOT NULL,
      stok_sistem INTEGER NOT NULL,
      stok_fisik INTEGER NOT NULL,
      selisih INTEGER NOT NULL,
      keterangan TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (stok_opname_id) REFERENCES t_stok_opname(id) ON DELETE CASCADE,
      FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
    );

    CREATE INDEX IF NOT EXISTS idx_t_stok_opname_tanggal ON t_stok_opname(tanggal);
    CREATE INDEX IF NOT EXISTS idx_t_stok_opname_detail_header ON t_stok_opname_detail(stok_opname_id);
    CREATE INDEX IF NOT EXISTS idx_t_stok_opname_detail_barang ON t_stok_opname_detail(barang_kode);

    CREATE TRIGGER IF NOT EXISTS trg_t_stok_opname_updated_at
    AFTER UPDATE ON t_stok_opname
    FOR EACH ROW
    BEGIN
      UPDATE t_stok_opname
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;

    CREATE TABLE IF NOT EXISTS t_kartu_stok (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      waktu TEXT NOT NULL,
      ref_type TEXT NOT NULL,
      ref_no TEXT,
      barang_kode TEXT NOT NULL,
      qty_in INTEGER NOT NULL DEFAULT 0 CHECK (qty_in >= 0),
      qty_out INTEGER NOT NULL DEFAULT 0 CHECK (qty_out >= 0),
      stok_after INTEGER,
      keterangan TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
    );

    CREATE INDEX IF NOT EXISTS idx_t_kartu_stok_barang_waktu ON t_kartu_stok(barang_kode, waktu);
    CREATE INDEX IF NOT EXISTS idx_t_kartu_stok_waktu ON t_kartu_stok(waktu);

    CREATE TABLE IF NOT EXISTS t_customer_claim (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      no_claim TEXT NOT NULL UNIQUE,
      tanggal TEXT NOT NULL,
      customer_kode TEXT,
      catatan TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (customer_kode) REFERENCES m_customer(kode) ON UPDATE CASCADE ON DELETE SET NULL
    );

    CREATE TABLE IF NOT EXISTS t_customer_claim_detail (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_claim_id INTEGER NOT NULL,
      barang_kode TEXT NOT NULL,
      qty INTEGER NOT NULL CHECK (qty > 0),
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      FOREIGN KEY (customer_claim_id) REFERENCES t_customer_claim(id) ON DELETE CASCADE,
      FOREIGN KEY (barang_kode) REFERENCES m_barang(kode_barang) ON UPDATE CASCADE ON DELETE RESTRICT
    );

    CREATE INDEX IF NOT EXISTS idx_t_customer_claim_tanggal ON t_customer_claim(tanggal);
    CREATE INDEX IF NOT EXISTS idx_t_customer_claim_detail_header ON t_customer_claim_detail(customer_claim_id);
    CREATE INDEX IF NOT EXISTS idx_t_customer_claim_detail_barang ON t_customer_claim_detail(barang_kode);

    CREATE TRIGGER IF NOT EXISTS trg_t_customer_claim_updated_at
    AFTER UPDATE ON t_customer_claim
    FOR EACH ROW
    BEGIN
      UPDATE t_customer_claim
      SET updated_at = datetime('now')
      WHERE id = OLD.id;
    END;
  `);

  // Lightweight migrations for existing DB files.
  // sql.js throws on duplicate columns; ignore if already migrated.
  try {
    sqlDb.exec("ALTER TABLE m_customer ADD COLUMN kontak_person TEXT");
  } catch {
    // ignore
  }

  try {
    sqlDb.exec("ALTER TABLE m_supplier ADD COLUMN email TEXT");
  } catch {
    // ignore
  }

  // Backfill so older DBs don't show empty Kontak Person everywhere.
  // Best-effort: default to customer name if kontak_person is missing.
  try {
    sqlDb.exec(
      "UPDATE m_customer SET kontak_person = nama WHERE (kontak_person IS NULL OR TRIM(kontak_person) = '') AND nama IS NOT NULL"
    );
  } catch {
    // ignore
  }
}

function rowsToObjects(columns, valuesRows) {
  return valuesRows.map((values) => {
    const row = {};
    for (let i = 0; i < columns.length; i += 1) {
      row[columns[i]] = values[i];
    }
    return row;
  });
}

function readJsonArrayIfExists(filePath) {
  if (!fs.existsSync(filePath)) return null;
  const content = fs.readFileSync(filePath, "utf8");
  const data = JSON.parse(content);
  return Array.isArray(data) ? data : null;
}

function getScalar(sqlDb, sql, params = []) {
  const results = sqlDb.exec(sql, params);
  if (!results || results.length === 0) return undefined;
  const first = results[0];
  if (!first.values || first.values.length === 0) return undefined;
  return first.values[0][0];
}

function seedIfEmpty(sqlDb, { seedDir }) {
  if (!seedDir || !fs.existsSync(seedDir)) return;
  const userCount = Number(getScalar(sqlDb, "SELECT COUNT(*) FROM m_user") ?? 0);
  const areaCount = Number(getScalar(sqlDb, "SELECT COUNT(*) FROM m_area") ?? 0);
  const kategoriCount = Number(
    getScalar(sqlDb, "SELECT COUNT(*) FROM m_kategori") ?? 0
  );
  const supplierCount = Number(
    getScalar(sqlDb, "SELECT COUNT(*) FROM m_supplier") ?? 0
  );
  const customerCount = Number(
    getScalar(sqlDb, "SELECT COUNT(*) FROM m_customer") ?? 0
  );
  const barangCount = Number(
    getScalar(sqlDb, "SELECT COUNT(*) FROM m_barang") ?? 0
  );

  // IMPORTANT: do not seed users. Owner is created via bootstrap flow.
  void userCount;

  if (areaCount === 0) {
    const areas = readJsonArrayIfExists(path.join(seedDir, "m_area.json"));
    if (areas) {
      const stmt = sqlDb.prepare(
        "INSERT OR IGNORE INTO m_area (kode, nama) VALUES (?, ?)"
      );
      try {
        for (const row of areas) {
          stmt.run([row.kode_area, row.nama_area]);
        }
      } finally {
        stmt.free();
      }
    }
  }

  if (kategoriCount === 0) {
    const kategori = readJsonArrayIfExists(path.join(seedDir, "m_kategori.json"));
    if (kategori) {
      const stmt = sqlDb.prepare(
        "INSERT OR IGNORE INTO m_kategori (kode, nama) VALUES (?, ?)"
      );
      try {
        for (const row of kategori) {
          stmt.run([row.kode_kategori, row.nama_kategori]);
        }
      } finally {
        stmt.free();
      }
    }
  }

  if (supplierCount === 0) {
    const suppliers = readJsonArrayIfExists(path.join(seedDir, "m_supplier.json"));
    if (suppliers) {
      const stmt = sqlDb.prepare(
        "INSERT OR IGNORE INTO m_supplier (kode, nama, telepon, email, alamat) VALUES (?, ?, ?, ?, ?)"
      );
      try {
        for (const row of suppliers) {
          stmt.run([
            row.kode_supplier,
            row.nama_supplier,
            row.telepon ?? null,
            row.email ?? null,
            row.alamat ?? null,
          ]);
        }
      } finally {
        stmt.free();
      }
    }
  }

  if (customerCount === 0) {
    const customers = readJsonArrayIfExists(path.join(seedDir, "m_customer.json"));
    if (customers) {
      const stmt = sqlDb.prepare(
        "INSERT OR IGNORE INTO m_customer (kode, nama, area_kode, telepon, kontak_person, alamat) VALUES (?, ?, ?, ?, ?, ?)"
      );
      try {
        for (const row of customers) {
          stmt.run([
            row.kode_customer,
            row.nama_customer,
            row.kode_area ?? null,
            row.telepon ?? null,
            row.kontak_person ?? null,
            row.alamat ?? null,
          ]);
        }
      } finally {
        stmt.free();
      }
    }
  }

  if (barangCount === 0) {
    const items = readJsonArrayIfExists(path.join(seedDir, "m_barang.json"));
    if (items) {
      const stmt = sqlDb.prepare(
        "INSERT OR IGNORE INTO m_barang (kode_barang, nama_barang, kategori_kode, satuan, stok, stok_minimal, harga_beli, harga_jual) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
      );
      try {
        for (const row of items) {
          stmt.run([
            row.kode_barang,
            row.nama_barang,
            row.kategori_id ?? row.kategori_kode ?? null,
            row.satuan ?? null,
            Number(row.stok ?? 0),
            Number(row.stok_minimal ?? 0),
            row.harga_beli ?? null,
            row.harga_jual ?? null,
          ]);
        }
      } finally {
        stmt.free();
      }
    }
  }

  // Top-up demo master data to make the app feel "real" (puluhan sampai 100 row).
  const targetMasterRows = 100;

  const pad3 = (n) => String(n).padStart(3, "0");
  const clampInt = (v, min, max) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return min;
    return Math.min(max, Math.max(min, Math.trunc(n)));
  };
  const pick = (arr, idx) => arr[idx % arr.length];

  const categoryCodes = (
    sqlDb.exec("SELECT kode FROM m_kategori ORDER BY kode ASC")?.[0]?.values ||
    []
  ).map((r) => r[0]);

  const ensureSuppliers = () => {
    const count = Number(
      getScalar(sqlDb, "SELECT COUNT(*) FROM m_supplier") ?? 0
    );
    if (count >= targetMasterRows) return;

    const stmt = sqlDb.prepare(
      "INSERT OR IGNORE INTO m_supplier (kode, nama, telepon, email, alamat) VALUES (?, ?, ?, ?, ?)"
    );
    try {
      for (let i = count + 1; i <= targetMasterRows; i += 1) {
        const kode = `SUP${pad3(i)}`;
        stmt.run([
          kode,
          `Supplier ${pad3(i)}`,
          `08${pad3(i)}-${pad3(i)}${pad3(i)}`,
          null,
          `Jl. Gudang No. ${i}, Kota Demo`,
        ]);
      }
    } finally {
      stmt.free();
    }
  };

  const ensureCustomers = () => {
    const count = Number(
      getScalar(sqlDb, "SELECT COUNT(*) FROM m_customer") ?? 0
    );
    if (count >= targetMasterRows) return;

    const areaCodes = (
      sqlDb.exec("SELECT kode FROM m_area ORDER BY kode ASC")?.[0]?.values || []
    ).map((r) => r[0]);

    const stmt = sqlDb.prepare(
      "INSERT OR IGNORE INTO m_customer (kode, nama, area_kode, telepon, kontak_person, alamat) VALUES (?, ?, ?, ?, ?, ?)"
    );
    try {
      for (let i = count + 1; i <= targetMasterRows; i += 1) {
        const kode = `CST${pad3(i)}`;
        const area = areaCodes.length ? pick(areaCodes, i) : null;
        stmt.run([
          kode,
          `Customer ${pad3(i)}`,
          area,
          `08${pad3(i)}-${pad3(i)}${pad3(i)}`,
          `Kontak ${pad3(i)}`,
          `Jl. Pelanggan No. ${i}, Kota Demo`,
        ]);
      }
    } finally {
      stmt.free();
    }
  };

  const ensureItems = () => {
    const count = Number(
      getScalar(sqlDb, "SELECT COUNT(*) FROM m_barang") ?? 0
    );
    if (count >= targetMasterRows) return;

    const units = ["pcs", "set", "liter", "koli"];
    const brands = ["Honda", "Yamaha", "Suzuki", "Kawasaki", "Universal"];
    const parts = [
      "Bearing",
      "Oli Mesin",
      "Busi",
      "Kampas Rem",
      "Filter Udara",
      "Filter Oli",
      "V-Belt",
      "Rantai",
      "Gear",
      "Seal Kit",
      "Kabel Gas",
      "Kabel Kopling",
      "Lampu Sein",
      "Aki",
      "Knalpot",
    ];

    const stmt = sqlDb.prepare(
      "INSERT OR IGNORE INTO m_barang (kode_barang, nama_barang, kategori_kode, satuan, stok, stok_minimal, harga_beli, harga_jual) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    );
    try {
      for (let i = count + 1; i <= targetMasterRows; i += 1) {
        const kode = `BRG${pad3(i)}`;
        const part = pick(parts, i);
        const brand = pick(brands, i * 7);
        const nama = `${part} ${brand} ${pad3(i)}`;
        const satuan = pick(units, i * 3);
        const kategori = categoryCodes.length ? pick(categoryCodes, i) : null;

        const stokMinimal = clampInt((i % 15) + 5, 3, 25);
        // Start with some stock so alerts/low/empty can be formed after transactions.
        const stokAwal = clampInt((i % 9) * 7, 0, 70);
        const hargaBeli = clampInt(10000 + (i % 25) * 5000, 5000, 250000);
        const hargaJual = hargaBeli + clampInt(hargaBeli * 0.35, 2000, 150000);

        stmt.run([
          kode,
          nama,
          kategori,
          satuan,
          stokAwal,
          stokMinimal,
          hargaBeli,
          hargaJual,
        ]);
      }
    } finally {
      stmt.free();
    }
  };

  ensureSuppliers();
  ensureCustomers();
  ensureItems();

  // Seed transactional history to support dashboard/reports (stok masuk/keluar, stok alert, barang habis/rendah, dll).
  const stokMasukExisting = Number(
    getScalar(sqlDb, "SELECT COUNT(*) FROM t_stok_masuk") ?? 0
  );
  const stokKeluarExisting = Number(
    getScalar(sqlDb, "SELECT COUNT(*) FROM t_stok_keluar") ?? 0
  );
  const kartuExisting = Number(
    getScalar(sqlDb, "SELECT COUNT(*) FROM t_kartu_stok") ?? 0
  );

  if (
    stokMasukExisting === 0 &&
    stokKeluarExisting === 0 &&
    kartuExisting === 0
  ) {
    const masukJson =
      readJsonArrayIfExists(path.join(seedDir, "t_stok_masuk.json")) || [];
    const keluarJson =
      readJsonArrayIfExists(path.join(seedDir, "t_stok_keluar.json")) || [];

    // Make sure master data for referenced codes exists.
    const ensureSupplierStmt = sqlDb.prepare(
      "INSERT OR IGNORE INTO m_supplier (kode, nama, telepon, email, alamat) VALUES (?, ?, ?, ?, ?)"
    );
    const ensureCustomerStmt = sqlDb.prepare(
      "INSERT OR IGNORE INTO m_customer (kode, nama, area_kode, telepon, kontak_person, alamat) VALUES (?, ?, ?, ?, ?, ?)"
    );
    const ensureItemStmt = sqlDb.prepare(
      "INSERT OR IGNORE INTO m_barang (kode_barang, nama_barang, kategori_kode, satuan, stok, stok_minimal, harga_beli, harga_jual) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    );

    const stockGetStmt = sqlDb.prepare(
      "SELECT stok, stok_minimal, COALESCE(harga_beli, 0) AS harga_beli, COALESCE(harga_jual, 0) AS harga_jual, satuan FROM m_barang WHERE kode_barang = ?"
    );
    const stockSetStmt = sqlDb.prepare(
      "UPDATE m_barang SET stok = ? WHERE kode_barang = ?"
    );

    const insMasukHdr = sqlDb.prepare(
      "INSERT OR IGNORE INTO t_stok_masuk (no_faktur, tanggal, supplier_kode, catatan) VALUES (?, ?, ?, ?)"
    );
    const insMasukDet = sqlDb.prepare(
      "INSERT INTO t_stok_masuk_detail (stok_masuk_id, barang_kode, qty, harga_beli) VALUES (?, ?, ?, ?)"
    );
    const insKeluarHdr = sqlDb.prepare(
      "INSERT OR IGNORE INTO t_stok_keluar (no_faktur, tanggal, customer_kode, catatan) VALUES (?, ?, ?, ?)"
    );
    const insKeluarDet = sqlDb.prepare(
      "INSERT INTO t_stok_keluar_detail (stok_keluar_id, barang_kode, qty, harga_jual) VALUES (?, ?, ?, ?)"
    );
    const insKartu = sqlDb.prepare(
      "INSERT INTO t_kartu_stok (waktu, ref_type, ref_no, barang_kode, qty_in, qty_out, stok_after, keterangan) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    );

    const insClaimHdr = sqlDb.prepare(
      "INSERT OR IGNORE INTO t_customer_claim (no_claim, tanggal, customer_kode, catatan) VALUES (?, ?, ?, ?)"
    );
    const insClaimDet = sqlDb.prepare(
      "INSERT INTO t_customer_claim_detail (customer_claim_id, barang_kode, qty) VALUES (?, ?, ?)"
    );
    const insOpnameHdr = sqlDb.prepare(
      "INSERT OR IGNORE INTO t_stok_opname (no_opname, tanggal, catatan) VALUES (?, ?, ?)"
    );
    const insOpnameDet = sqlDb.prepare(
      "INSERT INTO t_stok_opname_detail (stok_opname_id, barang_kode, stok_sistem, stok_fisik, selisih, keterangan) VALUES (?, ?, ?, ?, ?, ?)"
    );

    const getIdByNo = (table, col, val) =>
      Number(
        getScalar(sqlDb, `SELECT id FROM ${table} WHERE ${col} = ?`, [val]) ?? 0
      );

    const parseItemsArray = (row) => {
      const items = Array.isArray(row?.items) ? row.items : [];
      // Some JSON uses "jumlah", some uses "qty".
      return items
        .map((it) => ({
          kode: String(it.kode_barang ?? "").trim(),
          nama: String(it.nama_barang ?? "").trim(),
          qty: Number(it.jumlah ?? it.qty ?? 0),
          harga: it.harga ?? null,
        }))
        .filter((it) => it.kode && Number(it.qty) > 0);
    };

    const ensureItemExists = (kode, namaMaybe) => {
      stockGetStmt.bind([kode]);
      const hasRow = stockGetStmt.step();
      stockGetStmt.reset();
      if (hasRow) return;

      const i = Number(kode.replace(/\D/g, "")) || 1;
      const nama = namaMaybe || `Barang ${kode}`;
      const kategori = categoryCodes.length ? pick(categoryCodes, i) : null;
      const satuan = pick(["pcs", "set", "liter"], i);
      const stokMinimal = clampInt((i % 15) + 5, 3, 25);
      const hargaBeli = clampInt(10000 + (i % 25) * 5000, 5000, 250000);
      const hargaJual = hargaBeli + clampInt(hargaBeli * 0.35, 2000, 150000);

      ensureItemStmt.run([
        kode,
        nama,
        kategori,
        satuan,
        0,
        stokMinimal,
        hargaBeli,
        hargaJual,
      ]);
    };

    const getStock = (kode) => {
      stockGetStmt.bind([kode]);
      const ok = stockGetStmt.step();
      const row = ok ? stockGetStmt.getAsObject() : null;
      stockGetStmt.reset();
      return row;
    };

    const setStock = (kode, stok) => {
      stockSetStmt.run([stok, kode]);
    };

    const applyIn = ({ no, tanggal, supplier, catatan, items }) => {
      ensureSupplierStmt.run([supplier, supplier || "-", null, null, null]);
      insMasukHdr.run([no, tanggal, supplier || null, catatan || null]);
      const headerId = getIdByNo("t_stok_masuk", "no_faktur", no);
      if (!headerId) return;

      for (const it of items) {
        ensureItemExists(it.kode, it.nama);
        const row = getStock(it.kode);
        const before = Number(row?.stok ?? 0);
        const after = before + clampInt(it.qty, 1, 1000);
        const hargaBeli =
          it.harga ?? (row ? Number(row.harga_beli ?? 0) : null);

        insMasukDet.run([
          headerId,
          it.kode,
          clampInt(it.qty, 1, 1000),
          hargaBeli,
        ]);
        setStock(it.kode, after);
        insKartu.run([
          tanggal,
          "IN",
          no,
          it.kode,
          clampInt(it.qty, 1, 1000),
          0,
          after,
          catatan || null,
        ]);
      }
    };

    const applyOut = ({ no, tanggal, customer, catatan, items }) => {
      ensureCustomerStmt.run([
        customer,
        customer || "-",
        null,
        null,
        null,
        null,
      ]);
      insKeluarHdr.run([no, tanggal, customer || null, catatan || null]);
      const headerId = getIdByNo("t_stok_keluar", "no_faktur", no);
      if (!headerId) return;

      for (const it of items) {
        ensureItemExists(it.kode, it.nama);
        const row = getStock(it.kode);
        const before = Number(row?.stok ?? 0);
        const want = clampInt(it.qty, 1, 1000);
        const qty = before >= want ? want : before; // never go negative
        if (!qty) continue;
        const after = before - qty;
        const hargaJual =
          it.harga ?? (row ? Number(row.harga_jual ?? 0) : null);

        insKeluarDet.run([headerId, it.kode, qty, hargaJual]);
        setStock(it.kode, after);
        insKartu.run([
          tanggal,
          "OUT",
          no,
          it.kode,
          0,
          qty,
          after,
          catatan || null,
        ]);
      }
    };

    const applyClaimOut = ({
      no,
      tanggal,
      customer,
      catatan,
      kodeBarang,
      qty,
    }) => {
      ensureCustomerStmt.run([
        customer,
        customer || "-",
        null,
        null,
        null,
        null,
      ]);
      insClaimHdr.run([no, tanggal, customer || null, catatan || null]);
      const headerId = getIdByNo("t_customer_claim", "no_claim", no);
      if (!headerId) return;

      ensureItemExists(kodeBarang, `Barang ${kodeBarang}`);
      const row = getStock(kodeBarang);
      const before = Number(row?.stok ?? 0);
      const want = clampInt(qty, 1, 1000);
      const realQty = before >= want ? want : before;
      if (!realQty) return;
      const after = before - realQty;

      insClaimDet.run([headerId, kodeBarang, realQty]);
      setStock(kodeBarang, after);
      insKartu.run([
        tanggal,
        "CLAIM_OUT",
        no,
        kodeBarang,
        0,
        realQty,
        after,
        catatan || null,
      ]);
    };

    const applyOpname = ({
      no,
      tanggal,
      catatan,
      kodeBarang,
      stokFisik,
      keterangan,
    }) => {
      insOpnameHdr.run([no, tanggal, catatan || null]);
      const headerId = getIdByNo("t_stok_opname", "no_opname", no);
      if (!headerId) return;

      ensureItemExists(kodeBarang, `Barang ${kodeBarang}`);
      const row = getStock(kodeBarang);
      const stokSistem = Number(row?.stok ?? 0);
      const fisik = clampInt(stokFisik, 0, 5000);
      const selisih = fisik - stokSistem;
      const qtyIn = selisih > 0 ? selisih : 0;
      const qtyOut = selisih < 0 ? Math.abs(selisih) : 0;

      insOpnameDet.run([
        headerId,
        kodeBarang,
        stokSistem,
        fisik,
        selisih,
        keterangan || null,
      ]);
      setStock(kodeBarang, fisik);
      insKartu.run([
        tanggal,
        "ADJ",
        no,
        kodeBarang,
        qtyIn,
        qtyOut,
        fisik,
        keterangan || catatan || null,
      ]);
    };

    // Build up to 100 headers each. If JSON already has more, cap it.
    const targetHeaders = 100;
    const masukRows = masukJson.slice(0, targetHeaders);
    const keluarRows = keluarJson.slice(0, targetHeaders);

    // If JSON is shorter, generate extra demo rows.
    const allItemCodes = (
      sqlDb.exec(
        "SELECT kode_barang FROM m_barang ORDER BY kode_barang ASC"
      )?.[0]?.values || []
    ).map((r) => r[0]);
    const allSupplierCodes = (
      sqlDb.exec("SELECT kode FROM m_supplier ORDER BY kode ASC")?.[0]
        ?.values || []
    ).map((r) => r[0]);
    const allCustomerCodes = (
      sqlDb.exec("SELECT kode FROM m_customer ORDER BY kode ASC")?.[0]
        ?.values || []
    ).map((r) => r[0]);

    const isoDateFromOffset = (offsetDays) => {
      const d = new Date();
      d.setDate(d.getDate() - offsetDays);
      return d.toISOString().slice(0, 10);
    };

    for (let i = masukRows.length + 1; i <= targetHeaders; i += 1) {
      const kodeBarang = pick(allItemCodes, i * 11);
      const supplier = pick(allSupplierCodes, i * 7);
      masukRows.push({
        no_faktur: `PB-DEMO-${pad3(i)}`,
        // Older than sales, but always in the past.
        tanggal: isoDateFromOffset(targetHeaders - i + 21),
        kode_supplier: supplier,
        catatan: "Pembelian demo",
        items: [
          {
            kode_barang: kodeBarang,
            nama_barang: `Barang ${kodeBarang}`,
            jumlah: (i % 25) + 5,
            harga: null,
          },
        ],
      });
    }

    for (let i = keluarRows.length + 1; i <= targetHeaders; i += 1) {
      const kodeBarang = pick(allItemCodes, i * 13);
      const customer = pick(allCustomerCodes, i * 5);
      keluarRows.push({
        no_faktur: `SL-DEMO-${pad3(i)}`,
        // More recent than purchases, always in the past.
        tanggal: isoDateFromOffset(targetHeaders - i + 1),
        kode_customer: customer,
        catatan: "Penjualan demo",
        items: [
          {
            kode_barang: kodeBarang,
            nama_barang: `Barang ${kodeBarang}`,
            jumlah: (i % 10) + 1,
            harga: null,
          },
        ],
      });
    }

    try {
      sqlDb.exec("BEGIN");

      // Apply purchases first (so stock exists before selling).
      for (const row of masukRows) {
        const no = String(row.no_faktur ?? "").trim();
        if (!no) continue;
        const tanggal =
          String(row.tanggal ?? "").slice(0, 10) || isoDateFromOffset(120);
        const supplier =
          String(row.kode_supplier ?? row.supplier_kode ?? "").trim() || null;
        const catatan = row.catatan ?? null;
        const items = parseItemsArray(row);
        if (!items.length) continue;
        applyIn({ no, tanggal, supplier, catatan, items });
      }

      // Apply sales.
      for (const row of keluarRows) {
        const no = String(row.no_faktur ?? "").trim();
        if (!no) continue;
        const tanggal =
          String(row.tanggal ?? "").slice(0, 10) || isoDateFromOffset(60);
        const customer =
          String(row.kode_customer ?? row.customer_kode ?? "").trim() || null;
        const catatan = row.catatan ?? null;
        const items = parseItemsArray(row);
        if (!items.length) continue;
        applyOut({ no, tanggal, customer, catatan, items });
      }

      // Add a few claims and opnames so those features have data.
      for (let i = 1; i <= 20; i += 1) {
        const kodeBarang = pick(allItemCodes, i * 17);
        const customer = pick(allCustomerCodes, i * 9);
        applyClaimOut({
          no: `CLM-DEMO-${pad3(i)}`,
          tanggal: isoDateFromOffset(15 - (i % 10)),
          customer,
          catatan: "Customer claim demo",
          kodeBarang,
          qty: (i % 4) + 1,
        });
      }

      for (let i = 1; i <= 15; i += 1) {
        const kodeBarang = pick(allItemCodes, i * 19);
        const row = getStock(kodeBarang);
        const stokSistem = Number(row?.stok ?? 0);
        const drift = (i % 2 === 0 ? 1 : -1) * ((i % 5) + 1);
        const fisik = Math.max(0, stokSistem + drift);
        applyOpname({
          no: `OPN-DEMO-${pad3(i)}`,
          tanggal: isoDateFromOffset(7 - (i % 7)),
          catatan: "Opname demo",
          kodeBarang,
          stokFisik: fisik,
          keterangan:
            drift === 0
              ? "Sesuai"
              : drift > 0
              ? "Selisih lebih"
              : "Selisih kurang",
        });
      }

      sqlDb.exec("COMMIT");
    } catch (err) {
      try {
        sqlDb.exec("ROLLBACK");
      } catch (_) {
        // ignore
      }
      throw err;
    } finally {
      ensureSupplierStmt.free();
      ensureCustomerStmt.free();
      ensureItemStmt.free();
      stockGetStmt.free();
      stockSetStmt.free();
      insMasukHdr.free();
      insMasukDet.free();
      insKeluarHdr.free();
      insKeluarDet.free();
      insKartu.free();
      insClaimHdr.free();
      insClaimDet.free();
      insOpnameHdr.free();
      insOpnameDet.free();
    }
  }
}

function createDbFacade({ sqlDb, dbPath, save }) {
  function all(sql, params = []) {
    const results = sqlDb.exec(sql, params);
    if (!results || results.length === 0) return [];
    // sql.js exec can return multiple statements; take the first
    const { columns, values } = results[0];
    return rowsToObjects(columns, values);
  }

  function get(sql, params = []) {
    const rows = all(sql, params);
    return rows[0];
  }

  return {
    dbPath,
    all,
    get,
    exec(sql) {
      sqlDb.exec(sql);
      save();
    },
    run(sql, params = []) {
      sqlDb.run(sql, params);
      const changesRow = get(
        "SELECT changes() AS changes, last_insert_rowid() AS lastInsertRowid"
      );
      // Persist after each write for non-transactional writes.
      save();
      return changesRow;
    },
    async transaction(fn) {
      sqlDb.exec("BEGIN");
      const tx = {
        all,
        get,
        run(sql, params = []) {
          sqlDb.run(sql, params);
          return get(
            "SELECT changes() AS changes, last_insert_rowid() AS lastInsertRowid"
          );
        },
        exec(sql) {
          sqlDb.exec(sql);
        },
      };

      try {
        const result = await fn(tx);
        sqlDb.exec("COMMIT");
        save();
        return result;
      } catch (err) {
        try {
          sqlDb.exec("ROLLBACK");
        } catch (_) {
          // ignore rollback errors
        }
        throw err;
      }
    },
    save: async () => save(),
    async close(opts = {}) {
      const shouldSave =
        opts && Object.prototype.hasOwnProperty.call(opts, "save")
          ? Boolean(opts.save)
          : true;
      if (shouldSave) save();
      sqlDb.close();
    },
  };
}

async function initDb({ dataDir }) {
  // If DATABASE_URL is present, prefer PostgreSQL.
  if (process.env.DATABASE_URL) {
    return initDbPostgres();
  }

  if (!dataDir) {
    throw new Error("dataDir is required");
  }

  ensureDir(dataDir);
  const dbPath = path.join(dataDir, "stoir.sqlite");

  const sqlJsDir = path.dirname(require.resolve("sql.js/dist/sql-wasm.js"));
  const SQL = await initSqlJs({
    locateFile: (file) => path.join(sqlJsDir, file),
  });

  const fileBuffer = fs.existsSync(dbPath) ? fs.readFileSync(dbPath) : null;
  const sqlDb = fileBuffer
    ? new SQL.Database(new Uint8Array(fileBuffer))
    : new SQL.Database();

  // Ensure FK constraints are enforced (disabled by default in SQLite).
  sqlDb.exec("PRAGMA foreign_keys = ON;");

  migrate(sqlDb);

  // Seed from existing dummy JSON (dev convenience) on first run only.
  // This is disabled when a marker file exists to allow "start from scratch" flows.
  const defaultSeedDir = path.join(
    __dirname,
    "..",
    "..",
    "src",
    "data",
    "dummy"
  );
  const noSeedMarkerPath = path.join(dataDir, "stoir.no-seed");
  if (!fs.existsSync(noSeedMarkerPath)) {
    seedIfEmpty(sqlDb, { seedDir: defaultSeedDir });
  }

  const save = () => {
    const data = sqlDb.export();
    fs.writeFileSync(dbPath, Buffer.from(data));
  };

  // Ensure schema is persisted even on first run
  save();

  const db = createDbFacade({ sqlDb, dbPath, save });
  return { db, dbPath, provider: "sqlite" };
}

module.exports = { initDb };
