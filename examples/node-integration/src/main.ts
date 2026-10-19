import { files, initSchema, pool } from "./db.js";
import { createApp } from "./server.js";

await initSchema();
const port = Number(process.env.PORT ?? 3000);
const server = createApp().listen(port, () =>
  console.log(
    `Listening on http://localhost:${port}  (open it to try an upload)`,
  ),
);

async function shutdown() {
  server.close();
  await files.close(); // ZeroBucket never closes YOUR pool, so close it yourself:
  await pool.end();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
