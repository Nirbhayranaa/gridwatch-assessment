import { Pool } from 'pg';
import { initialize } from './store.ts';
import { server } from './http.ts';
import { pilot, replay } from './demo.ts';

if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
const pool = new Pool({ connectionString: process.env.DATABASE_URL, connectionTimeoutMillis: 3000, query_timeout: 10_000, max: 10 });
pool.on('error', () => { console.error('idle database connection failed'); });
let ready = false;
const app = server(pool, () => ready);
try {
  await initialize(pool,pilot);
  await new Promise<void>(resolve => app.listen(8080,'0.0.0.0',resolve));
  await replay(pool,'http://127.0.0.1:8080');
  ready = true;
  console.log(JSON.stringify({ event: 'demo_ready', port: 8080, note: 'historical source timestamps retained' }));
} catch (error) {
  console.error(error);
  app.close();
  await pool.end();
  process.exitCode = 1;
}
process.on('SIGTERM', () => {
  app.close(() => { void pool.end(); });
});
