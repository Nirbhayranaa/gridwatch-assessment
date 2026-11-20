# GridWatch

This submission has three parts:

- Part A: `docs/architecture.md`
- Part B: `docs/architecture-review.md`
- Part C: the runnable service in this repository

## What I built

I chose one path: **CSMS data → validation and normalization → PostgreSQL → a status summary for each site**.

The hard part is keeping the result right when events repeat, arrive late, contain bad values, or fail halfway through a scan. The code handles those cases rather than adding a dashboard or general CRUD APIs.

The service uses TypeScript, Node.js, and PostgreSQL. Node's HTTP server and test runner cover the small API and tests; `pg` is the only runtime dependency.

## Run it

You need Docker with Docker Compose, an internet connection for the first build, and port 8080 free. No cloud account, host Node.js installation, or host Terraform installation is needed.

From this directory:

```sh
docker compose up
```

Compose starts PostgreSQL, runs Terraform to create the application user and two databases, then starts the app. The second database is reserved for tests. The app creates its tables, loads the supplied site catalogue, and replays the supplied events. Wait for `demo_ready` in the app logs.

Check readiness:

```sh
curl http://localhost:8080/health
```

Get Acme's site summary:

```sh
curl -H 'Authorization: Bearer local-acme-token' http://localhost:8080/v1/site-summary
```

Get the German operator's summary:

```sh
curl -H 'Authorization: Bearer local-stadtwerke-token' http://localhost:8080/v1/site-summary
```

See stored event counts, rejected records, meter resets, and completed poll boundaries:

```sh
curl -H 'Authorization: Bearer local-acme-token' http://localhost:8080/v1/ingestion
curl -H 'Authorization: Bearer local-stadtwerke-token' http://localhost:8080/v1/ingestion
```

The API gets the operator from the token. An `operator_id` query parameter is rejected. The port is bound to localhost; the database has no published host port.

## What the demo shows

All nine JSON payloads in the supplied CSMS document are copied into `fixtures/csms.json`. `fixtures/pilot.json` is the supplied pilot excerpt. Neither has invented coordinates or additional sites.

The poll adapter returns ordinary HTTP response objects for the supplied 429, 500, paginated success, and malformed-page cases. The demo then sends the webhook examples through the actual signed HTTP endpoint, including duplicate deliveries, old status events, and the meter reset.

Only the fixture transport uses a simulated clock for request waits. It still reserves request slots 30 seconds apart, honors a longer `Retry-After`, and follows the same retry and checkpoint code. This avoids waiting through the canned delays at every startup. Receive times, source timestamps, and API freshness checks use real time. No live vendor service is contacted.

After replay, the latest reported state is:

| Site | Connectors | Available | Charging | Unknown |
|---|---:|---:|---:|---:|
| S-IN-0007 | 3 | 2 | 0 | 1 |
| S-IN-0012 | 1 | 0 | 0 | 1 |
| S-DE-0003 | 2 | 0 | 0 | 2 |

The source events are dated June 2026. `available` describes the latest reported status; it is not a claim that the charger is available now. The response includes `stale`, `latest_status_at`, `latest_observation_at`, and `fresh_available`. With the old fixture data, `fresh_available` is zero. The initial freshness threshold is 90 seconds, an explicit local assumption.

Expected stored totals are 11 Acme events, two German events, two rejected German records, and one Acme meter-reset event. Running the replay again does not add duplicate facts.

## How correctness is handled

- Events use a stable fingerprint based on the operator, connection, charger, connector, normalized timestamp, type, and business values. Delivery IDs and JSON field order do not affect it.
- Database uniqueness handles repeated or concurrent deliveries. An event and its current-state change commit together. A failed transaction is rolled back and the webhook returns an error, allowing a safe retry.
- Status and meter readings have separate clocks. Older events stay in history but do not replace newer values. Equal-time conflicting values are marked unknown until a newer observation arrives. A current poll can confirm unchanged status without duplicating history; an older status cannot refresh a newer one.
- Invalid events are saved with a reason; valid events in the same batch still commit. A record for another operator or poll site is rejected. Timestamps require an explicit offset, and events more than five minutes in the future are rejected.
- A PostgreSQL advisory lock gives one poll worker ownership of a site. A shared `next_allowed_at` survives worker failure. Each request reserves its slot before contacting the vendor. A crash closes the dedicated lock connection.
- A scan keeps a fixed `since` value and advances its completed boundary only after the final page commits. If page two fails, page one can be replayed safely. The five-minute overlap is an initial allowance, not a guarantee that every late vendor event can be recovered.
- Session events are retained, with valid Wh differences or an explicit `meter_reset` flag. There is no invoicing path in this slice. A fault event is stored as a fault; it does not automatically turn an otherwise unknown connector into `Faulted`.

## Coordinates

The catalogue supplies addresses only. Each site keeps that address, nullable coordinates, and `location_status: unresolved`. Database checks require a complete, valid coordinate pair before a location can be marked verified. The summary works without coordinates.

I stopped before adding geocoding or nearest-charger searches. Those need a trusted location source and review of uncertain matches. This slice does not guess site locations or present unresolved addresses as map points.

## Run the tests

```sh
docker compose --profile test run --rm --build tests
```

The tests use a separate `gridwatch_test` database and real PostgreSQL transactions. They cover retries and pagination, failure after the first page, competing poll owners, malformed events, late readings, equal-time conflicts, repeated signed webhooks, tenant checks, and a database failure injected halfway through a batch. Smaller tests check units, timestamps, signatures through the API, and both forms of `Retry-After`.

For a local Node.js 24+ development setup, run `npm ci` and `npm run check`. `npm test` also needs `DATABASE_URL` pointing to a dedicated database whose name ends in `_test`; tests refuse to reset any other database.

### Checks run locally

- `docker compose up -d --wait` started a healthy app and PostgreSQL database.
- Terraform created the role and two databases. A repeat apply reported no changes.
- TypeScript checking and all 16 tests passed against PostgreSQL.
- An app restart kept the same 13 stored events and two rejected records. The API still returned the expected site totals, stale-data flags, and unresolved locations.

These checks use the supplied examples. Production load and regional failover have not been tested.

## Infrastructure and restart behavior

`infra/main.tf` provisions the local PostgreSQL role and the app/test databases. Compose runs it automatically. Provider checksums and Node dependencies are locked. Terraform state and database data use separate named volumes; state is not part of the submission. Running Terraform again should report no changes. Schema creation and demo replay are safe to repeat.

```sh
docker compose restart app
docker compose stop
```

Both retain data. To remove containers while keeping data, use `docker compose down`. To start completely fresh, `docker compose down -v` removes **both the demo database and Terraform state**. Do not use it if you need to keep the demo data.

All passwords, bearer tokens, and webhook secrets are deliberately public local-demo values. They are not production credentials. The app owns its local tables and applies tenant checks in the service; production OIDC, database row-level security, and managed secrets remain work for the production design.

## Decisions and where I stopped

I kept one database and direct transactions for this local slice. Kafka, separate production stores, browser push, billing, and regional failover are described in Part A. Adding them here would not improve the proof that the supplied feed can be processed correctly.

The catalogue contains three sites and four chargers, not the full pilot mentioned in the notes. No performance or high-availability claim is based on this sample. The archive is represented by retained raw events in PostgreSQL for this slice; production object storage and retention jobs are not implemented.

Next I would connect a real vendor HTTP transport with timeouts, confirm history and pagination rules, add reviewed geocoding, then test the production-rate pipeline and recovery plan. The current demo runs a finite replay on startup; it is not a background production polling service.
