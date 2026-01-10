const path = require("path");

const { createServer } = require("./app.cjs");

async function main() {
  const host = process.env.HOST || "0.0.0.0";
  const port = Number(process.env.PORT || 3000);

  const dataDir =
    process.env.DATA_DIR || path.join(process.cwd(), "data");

  const fastify = await createServer({
    host,
    port,
    isPackaged: false,
    distDir: null,
    dataDir,
  });

  const shutdown = async (signal) => {
    try {
      fastify.log.info({ signal }, "Shutting down");
      await fastify.close();
      process.exit(0);
    } catch (err) {
      fastify.log.error(err);
      process.exit(1);
    }
  };

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
