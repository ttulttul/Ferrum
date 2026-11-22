// Creates a fresh statev2.sqlite with the reference server's own migrations and
// prints its schema and migration ledger as JSON, for src/statev2/schema.sql.
// Usage: node scripts/reference_statev2_schema.mjs <reference checkout> <empty dir> <out.json>
import { pathToFileURL } from "node:url";
import * as NodePath from "node:path";
import { DatabaseSync } from "node:sqlite";
import { createRequire } from "node:module";

const [checkout, directory, output] = process.argv.slice(2);
const server = NodePath.join(checkout, "apps/server");
const load = (relative) => import(pathToFileURL(NodePath.join(server, relative)).href);
const requireFromServer = createRequire(NodePath.join(server, "package.json"));
const fromServer = (specifier) => import(pathToFileURL(requireFromServer.resolve(specifier)).href);

const Effect = await fromServer("effect/Effect");
const Layer = await fromServer("effect/Layer");
const SqlClient = await fromServer("effect/sql/SqlClient");
const NodeServices = await fromServer("@effect/platform-node/NodeServices");
const Sqlite = await load("src/persistence/Sqlite.ts");

const dbPath = NodePath.join(directory, "statev2.sqlite");
await Effect.runPromise(
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`SELECT 1`;
  }).pipe(Effect.provide(Sqlite.layerFromPath(dbPath).pipe(Layer.provide(NodeServices.layer))), Effect.scoped),
);

const db = new DatabaseSync(dbPath, { readOnly: true });
const objects = db
  .prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY rowid")
  .all()
  .map((row) => ({ ...row }));
const migrations = db
  .prepare("SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id")
  .all()
  .map((row) => ({ ...row }));
const pragmas = Object.fromEntries(
  ["journal_mode", "auto_vacuum", "page_size", "user_version", "application_id"].map((name) => [
    name,
    Object.values(db.prepare(`PRAGMA ${name}`).get())[0],
  ]),
);
const tables = objects.filter((o) => o.type === "table" && !o.name.startsWith("sqlite_")).map((o) => o.name);
const rows = {};
for (const table of tables) {
  if (table === "effect_sql_migrations") continue;
  const count = Number(db.prepare(`SELECT COUNT(*) AS n FROM "${table}"`).get().n);
  if (count > 0) rows[table] = db.prepare(`SELECT * FROM "${table}"`).all().map((r) => ({ ...r }));
}
db.close();
(await import("node:fs")).writeFileSync(output, JSON.stringify({ pragmas, migrations, objects, rows }, null, 1) + "\n");
