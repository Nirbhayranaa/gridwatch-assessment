# GridWatch Architecture Review

**Part B — Nirbhay**

Review of the supplied **GridWatch Platform — Architecture Draft v0.3**. Section numbers below refer to that draft.

## Overall view

I would ask for changes before approving this design. The main parts fit the problem, but several important claims do not hold up. The biggest risks are showing one operator's data to another, billing a session twice, and promising updates faster than the vendor can deliver them.

I have ranked the issues by how much harm they could cause, how many customers they affect, and how soon they could happen:

- **Critical:** Could expose customer data or create duplicate bills during normal use.
- **High:** Could lose data, stop the service, give wrong results, or break the agreed service targets.
- **Medium:** Adds unnecessary work and makes the system harder to run as usage grows.

The expected load is 50,000 chargers reporting about every ten seconds. That is 5,000 readings per second before extra events, repeated messages, and retries.

## Critical issues

### 1 A user could see another operator's data

**Draft sections 6, 4.2, 5.1, and 8.** The API uses the operator ID sent by the browser to choose which data to return. A user can change that ID in the URL. Logging in through Keycloak does not, by itself, prove that the user may access the requested operator.

The telemetry table also has no operator ID, despite the claim that every row has one. Redis uses only the charger ID as its cache key.

**Fix:** Check the user's allowed operators on the server. Use PostgreSQL row-level security to restrict which rows the application can read or change. Include the operator ID in related database keys so records cannot be linked across operators. Include operator and vendor details in cache keys too. Test this by asking for another operator's data through the API, map, and exports. The application's database account must not be able to skip these checks.

### 2 The same session can be billed twice

**Draft section 7; examples 2b and 2c in the stubs.** The vendor can send the same session-stop event more than once, with a different delivery ID. The draft explicitly says no duplicate check is needed, so it allows a second invoice for the same session.

The session and billing workers also run separately. If billing needs a saved session or its start-time price, it may run before those details have been saved.

**Fix:** Create an invoice only when the required session details are available. Add a database rule that allows only one invoice for each operator, vendor connection, session, and invoice revision. A repeated event must reuse the existing result. Start and stop events must work in either order; missing details should wait for review. New invoice revisions should be deliberate corrections.

## High-priority issues

### 3 The vendor feed cannot support the promised update times

**Draft sections 1, 3, and 5.** Polling happens every 30 seconds, webhooks can be held for 60 seconds, and the browser refreshes every ten seconds. These delays cannot guarantee that a change appears within two or five seconds.

**Fix:** Agree exactly when each freshness measurement starts and ends. Ask the vendor for a delivery-time guarantee that supports the target, or change the customer commitment. Push saved changes to the browser and show when the data was last observed. Measure the vendor's delay separately from our own processing time. Vendor outages can make the delay longer than the normal polling or batching interval.

### 4 The database could fill in about 28 hours

**Draft sections 4.2 and 10.** The draft calculates 864 GB of new data per day, then says a 1 TB disk will last about a year.

Using decimal units, `1,000 GB / 864 GB per day = 1.16 days`, or about 28 hours. That leaves no allowance for indexes, database logs, or spare space. A year of this data would be roughly 315 TB before those additions.

**Fix:** Decide how much detailed history needs to stay online. Split telemetry into daily table partitions and move older data to an archive. Add indexes for searches by operator, connector, and time. Include copies, backups, logs, and repeated events in the capacity plan. Test the proposed 4 vCPU/8 GB database with ingestion and dashboard requests running together.

### 5 The recovery promises are stronger than the design

**Draft sections 3, 8, 9, and 13.** The backup database receives changes after the primary confirms them. If the primary fails before a change is copied, that change can be lost. This does not support RPO 0, which means no confirmed data loss.

Calling an engineer to switch databases also does not prove a five-minute RTO, which means time to restore service. Kafka redundancy is still an open question, and a Redis restart logs out every user.

**Fix:** If no confirmed database writes may be lost, wait for a standby to save them before confirming success, and use a safe failover process. Otherwise, state the accepted loss window. Define Kafka's copies and write acknowledgements—for example, three copies across availability zones, requiring at least two in-sync copies before accepting a write. Do not confirm incoming data before it is safely stored. Keep the service usable when the cache fails, and test recovery times. A whole-region outage needs its own recovery plan.

### 6 Billing needs clear energy and day-boundary rules

**Draft section 7; example 2d in the stubs.** The meter difference is in Wh, but the price is per kWh. The German sample gives 41,420 Wh, which is 41.420 kWh. The reset example gives a negative difference and needs a clear handling rule.

At 02:00 IST, Germany's previous local day is also still running. That job cannot close the full German business day if reports are meant to follow local dates.

**Fix:** Divide valid session Wh by 1,000 before applying the price. Use exact decimal calculations for money and save the price version used. Hold resets or missing readings for review. Agree each operator's reporting day, cutoff, rounding, and correction rules. Use timezones such as `Asia/Kolkata` and `Europe/Berlin`, including Germany's clock changes. Use INR or EUR prices directly unless currency conversion has been agreed.

### 7 Common vendor failures are not handled

**Draft section 3; examples 1c–1e and 2c in the stubs.** The draft does not explain what happens after rate limits, server errors, incomplete pages, or bad records. An old `Charging` event can arrive after a newer `Available` event. Applying them in arrival order would show the wrong status. Saving the next page position too early can also skip data after a crash.

**Fix:** Follow `Retry-After` on rate limits and increase the wait between retries after temporary failures. Keep bad records with a reason while processing the valid ones. Save page progress only after accepting the data safely, and share polling ownership and request times across workers. Update current status only with newer information. Save each worker's progress with its database changes. Check the webhook's body signature, called HMAC, as required by the stubs; checking only a shared-secret header is not enough.

### 8 The map assumes location data that does not exist

**Draft sections 4.1 and 5.1.** The vendor supplies addresses, not coordinates. The design includes latitude and longitude columns but never explains how they are filled.

The nearest-charger calculation also treats latitude and longitude as equal distance units. In southern Germany, that formula can choose a charger about 1.1 km north instead of one about 0.9 km east. It also does not check whether an available connector was observed recently.

**Fix:** Convert addresses to coordinates during site setup. Save the source and confidence, and ask an operator to check uncertain matches. Leave unresolved sites out of location searches. Use PostGIS geography and a spatial index to search and rank distances in metres. Require a recently observed available connector. For density, count chargers within agreed region boundaries and divide by area in km². Save those regional totals so each request does not repeat the calculation.

### 9 The monitoring cannot tell whether the data is fresh

**Draft section 12.** CPU, database connections, and HTTP error rates can look normal while the dashboard shows a status that is a minute old. Those checks do not prove the freshness targets are being met.

**Fix:** Record when an event happened, arrived, was safely stored, updated the current view, and appeared in the browser. Measure the full delay and our own processing delay separately. Count late and missing updates as failures. Track data age, queued work, and clock errors. Compare against vendor history where possible to find missing events; received messages alone cannot prove that everything arrived.

## Medium-priority issue

### 10 Every refresh repeats work for all of an operator's sites

**Draft sections 2 and 5.** The browser downloads every site and redraws the map every ten seconds. These requests share a database with incoming telemetry. For example, 1,000 viewers would make 100 full refreshes per second, potentially covering thousands of sites each. This is an example load, not a measured database limit.

**Fix:** Load only the area and detail visible on the map. Group nearby sites when zoomed out, and send status changes separately from location data. Read prepared totals for dashboard tiles. Limit pending updates for slow browsers and let them reload current state when needed. Agree the expected number of users and test that load alongside ingestion. Separate historical work if it slows live requests.

## Before I would approve the design

I would want these checks to pass:

- A user cannot read or change another operator's data through the API, map, cache, or exports.
- Repeated events, late events, and meter resets give the right final state without duplicate bills.
- The system handles production-rate input and the expected dashboard users, including during a component failure.
- Backup restores and failover tests show how long recovery takes and whether any data is lost.

I would also confirm the vendor's delivery times, available history, and page limits. The team needs agreed billing calendars, price and currency rules, location providers, recovery locations, data retention periods, and a budget.

The supplied JSON has three sites and four chargers. It is enough to check the examples, but larger tests need the full pilot data or clearly labeled generated data.

I would approve the revised design once these issues are addressed, the checks pass, and the remaining rules are agreed.
