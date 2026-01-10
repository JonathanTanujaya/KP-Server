function registerLedgerRoutes(fastify, { db }) {
  fastify.get('/api/ledger', async (request) => {
    const kode_barang = request.query?.kode_barang ? String(request.query.kode_barang).trim() : null;
    const from = request.query?.from ? String(request.query.from).trim() : null;
    const to = request.query?.to ? String(request.query.to).trim() : null;
    const limit = Math.min(2000, Math.max(1, Number(request.query?.limit ?? 500)));

    const where = [];
    const params = [];

    if (kode_barang) {
      where.push('k.barang_kode = ?');
      params.push(kode_barang);
    }
    if (from) {
      where.push('k.waktu >= ?');
      params.push(from);
    }
    if (to) {
      where.push('k.waktu <= ?');
      params.push(to);
    }

    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

    return db.all(
      `SELECT k.id,
              k.waktu,
              k.ref_type,
              k.ref_no,
              k.barang_kode AS kode_barang,
              b.nama_barang,
              b.satuan,
              k.qty_in,
              k.qty_out,
              k.stok_after,
              k.keterangan
       FROM t_kartu_stok k
       JOIN m_barang b ON b.kode_barang = k.barang_kode
       ${whereSql}
       ORDER BY k.waktu DESC, k.id DESC
       LIMIT ?`,
      [...params, limit]
    );
  });
}

module.exports = { registerLedgerRoutes };
