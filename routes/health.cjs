function registerHealthRoutes(fastify) {
  fastify.get('/api/health', async () => {
    return {
      ok: true,
      name: 'stoir-inventory',
      ts: new Date().toISOString(),
      db: {
        provider: fastify.dbProvider || null,
        // dbPath is only meaningful for sqlite; null for postgres
        dbPath: fastify.dbPath || null,
      },
    };
  });
}

module.exports = { registerHealthRoutes };
