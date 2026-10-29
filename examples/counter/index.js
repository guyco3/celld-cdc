// A counter actor that writes through both Durable Object storage APIs, so
// celld-cdc captures a KV table (`_cf_KV`) and a SQL table (`clicks`).
//
//   GET  /?name=a        increment the KV counter `n`
//   POST /click?name=a   insert a row into `clicks`, and bump `n` by `delta`
//   POST /reset?name=a   delete every click and the `n` key
//   GET  /state?name=a   read both back
export class Counter {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS clicks(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts INTEGER NOT NULL,
      delta INTEGER NOT NULL,
      note TEXT
    )`);
  }

  async fetch(request) {
    const url = new URL(request.url);
    const storage = this.ctx.storage;
    switch (`${request.method} ${url.pathname}`) {
      case "GET /": {
        const n = ((await storage.get("n")) ?? 0) + 1;
        await storage.put("n", n);
        return Response.json({ n });
      }
      case "POST /click": {
        const delta = Number(url.searchParams.get("delta") ?? 1);
        const note = url.searchParams.get("note");
        this.sql.exec(
          "INSERT INTO clicks(ts, delta, note) VALUES (?, ?, ?)",
          Date.now(), delta, note,
        );
        const n = ((await storage.get("n")) ?? 0) + delta;
        await storage.put("n", n);
        await storage.put("last", { at: Date.now(), delta, note });
        return Response.json({ n });
      }
      case "POST /reset": {
        this.sql.exec("DELETE FROM clicks");
        await storage.delete(["n", "last"]);
        return Response.json({ n: 0 });
      }
      case "GET /state": {
        const clicks = this.sql.exec("SELECT * FROM clicks ORDER BY id").toArray();
        return Response.json({ n: (await storage.get("n")) ?? 0, clicks });
      }
      default:
        return new Response("not found", { status: 404 });
    }
  }
}

export default {
  async fetch(request, env) {
    const name = new URL(request.url).searchParams.get("name") ?? "default";
    return env.COUNTER.get(env.COUNTER.idFromName(name)).fetch(request);
  },
};
