function registerHealthRoutes(fastify) {
  fastify.get('/api/health', async () => {
    return {
      ok: true,
      name: 'stoir-inventory',
      ts: new Date().toISOString(),
    };
  });
}

module.exports = { registerHealthRoutes };
