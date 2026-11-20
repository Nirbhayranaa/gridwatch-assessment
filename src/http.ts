import { createServer } from 'node:http';
import type { IncomingMessage } from 'node:http';
import { createHmac, timingSafeEqual } from 'node:crypto';
import type { Pool } from 'pg';
import { diagnostics, ingest, summary } from './store.ts';
import { InvalidEvent, object } from './normalize.ts';

// shortcut: local assessment credentials only; replace with OIDC and a secrets manager before deployment.
export const connections = {
  in: { operator: 'acme-charge', connection: 'in', secret: 'local-in-webhook-secret', token: 'local-acme-token' },
  de: { operator: 'stadtwerke-ev', connection: 'de', secret: 'local-de-webhook-secret', token: 'local-stadtwerke-token' },
};

class HttpError extends Error {
  status: number;
  constructor(status: number, message: string) { super(message); this.status = status; }
}

async function readBody(req: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 1_048_576) throw new HttpError(413, 'body exceeds 1 MB');
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

export function sign(body: string | Buffer, secret: string): string {
  return createHmac('sha256', secret).update(body).digest('hex');
}

export function server(pool: Pool, ready: () => boolean = () => true) {
  const app = createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', 'http://localhost');
      let body: unknown;
      let status = 200;
      if (req.method === 'GET' && url.pathname === '/health') {
        await pool.query('SELECT 1');
        body = { ready: ready(), mode: 'local-fixture-demo' };
        status = ready() ? 200 : 503;
      } else if (req.method === 'POST' && /^\/webhooks\/(in|de)$/.test(url.pathname)) {
        const connection = connections[url.pathname.split('/')[2] as keyof typeof connections];
        const raw = await readBody(req);
        const signature = req.headers['x-csms-signature'];
        const expected = Buffer.from(sign(raw, connection.secret), 'hex');
        if (typeof signature !== 'string' || !/^[a-f0-9]{64}$/i.test(signature) ||
            !timingSafeEqual(Buffer.from(signature,'hex'), expected)) throw new HttpError(401, 'invalid webhook signature');
        let parsed;
        try { parsed = JSON.parse(raw.toString('utf8')); }
        catch { throw new HttpError(400, 'invalid JSON'); }
        const envelope = object(parsed);
        if (typeof envelope.delivery_id !== 'string' || !envelope.delivery_id ||
            !Array.isArray(envelope.events) || envelope.events.length > 1000) throw new HttpError(400, 'invalid webhook envelope');
        body = await ingest(pool, connection, envelope.events);
      } else if (req.method === 'GET' && ['/v1/site-summary','/v1/ingestion'].includes(url.pathname)) {
        const connection = Object.values(connections).find(c => req.headers.authorization === `Bearer ${c.token}`);
        if (!connection) throw new HttpError(401, 'valid bearer token required');
        if (url.searchParams.has('operator_id')) throw new HttpError(400, 'operator comes from your token, not a query parameter');
        body = url.pathname === '/v1/site-summary' ? await summary(pool, connection.operator) : await diagnostics(pool, connection.operator);
      } else throw new HttpError(404, 'route not found');
      res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify(body));
    } catch (error) {
      const status = error instanceof HttpError ? error.status : error instanceof InvalidEvent ? 400 : 503;
      if (status === 503) console.error(JSON.stringify({ event: 'request_failed', message: 'database or internal error' }));
      if (!res.destroyed) {
        res.writeHead(status, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ error: status === 503 ? 'request could not be committed; retry safely' : (error as Error).message }));
      }
    }
  });
  app.requestTimeout = 10_000;
  app.headersTimeout = 5_000;
  return app;
}
