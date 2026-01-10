 const path = require("path");
const fastifyFactory = require("fastify");
const cors = require("@fastify/cors");
const multipart = require("@fastify/multipart");
const fastifyStatic = require("@fastify/static");

const { registerHealthRoutes } = require("./routes/health.cjs");
const { registerAreaRoutes } = require("./routes/areas.cjs");
const { registerCategoryRoutes } = require("./routes/categories.cjs");
const { registerSupplierRoutes } = require("./routes/suppliers.cjs");
const { registerCustomerRoutes } = require("./routes/customers.cjs");
const { registerItemRoutes } = require("./routes/items.cjs");
const { registerStockInRoutes } = require("./routes/stock-in.cjs");
const { registerStockOutRoutes } = require("./routes/stock-out.cjs");
const { registerStockOpnameRoutes } = require("./routes/stock-opname.cjs");
const { registerLedgerRoutes } = require("./routes/ledger.cjs");
const { registerCustomerClaimRoutes } = require("./routes/customer-claims.cjs");
const { registerAuthRoutes } = require("./routes/auth.cjs");
const { registerUserRoutes } = require("./routes/users.cjs");
const { registerReportRoutes } = require("./routes/reports.cjs");
const { registerDbToolsRoutes } = require("./routes/db-tools.cjs");
const { initDb } = require("./db.cjs");

async function createServer({ host, port, isPackaged, distDir, dataDir }) {
  const fastify = fastifyFactory({
    logger: true,
  });

  fastify.decorate("isPackaged", Boolean(isPackaged));
  fastify.decorate("skipDbSaveOnClose", false);

  const { db, dbPath, provider } = await initDb({ dataDir });
  fastify.log.info({ provider, dbPath }, "Database initialized");
  fastify.decorate("dbPath", dbPath);
  fastify.decorate("dbProvider", provider || "sqlite");

  // Allow binary restore uploads without multipart.
  fastify.addContentTypeParser(
    "application/octet-stream",
    { parseAs: "buffer" },
    (req, body, done) => {
      done(null, body);
    }
  );

  // Allow SQL/text uploads without multipart.
  for (const ct of ["text/plain", "application/sql", "application/x-sql"]) {
    fastify.addContentTypeParser(
      ct,
      { parseAs: "string" },
      (req, body, done) => {
        done(null, body);
      }
    );
  }

  // Allow browser file uploads (multipart/form-data) for db restore.
  await fastify.register(multipart, {
    limits: {
      fileSize: 50 * 1024 * 1024,
    },
  });

  fastify.addHook("onClose", async () => {
    try {
      await db.close({ save: !fastify.skipDbSaveOnClose });
    } catch (err) {
      fastify.log.error(err);
    }
  });

  await fastify.register(cors, {
    origin: true,
    methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  });

  // Auth must be registered before user routes.
  registerAuthRoutes(fastify, { db });
  registerUserRoutes(fastify, { db });

  registerHealthRoutes(fastify);
  registerAreaRoutes(fastify, { db });
  registerCategoryRoutes(fastify, { db });
  registerSupplierRoutes(fastify, { db });
  registerCustomerRoutes(fastify, { db });
  registerItemRoutes(fastify, { db });
  registerStockInRoutes(fastify, { db });
  registerStockOutRoutes(fastify, { db });
  registerStockOpnameRoutes(fastify, { db });
  registerLedgerRoutes(fastify, { db });
  registerCustomerClaimRoutes(fastify, { db });
  registerReportRoutes(fastify, { db });
  registerDbToolsRoutes(fastify, { db });

  if (isPackaged) {
    await fastify.register(fastifyStatic, {
      root: distDir,
      index: "index.html",
    });

    // SPA fallback
    fastify.setNotFoundHandler((req, reply) => {
      if (req.raw.url && req.raw.url.startsWith("/api/")) {
        reply.code(404).send({ error: "Not found" });
        return;
      }

      reply.type("text/html").sendFile("index.html");
    });
  }

  await fastify.listen({ host, port });
  return fastify;
}

module.exports = { createServer };
