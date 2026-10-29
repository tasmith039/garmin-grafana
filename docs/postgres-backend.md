### PostgreSQL backend (proof of concept)

garmin-grafana can store its data in PostgreSQL instead of InfluxDB. TimescaleDB is optional. This
is the "wrapper that keeps the line-protocol-style writes" idea from
[discussion #56](https://github.com/arpanghosh8453/garmin-grafana/discussions/56): the fetcher still
builds the same points, and `postgres_backend.py` stores them the way InfluxDB would.

##### How data is stored

- One table per measurement (`"HeartRateIntraday"`, `"SleepSummary"`, ...), created on first write,
  with a `time` column, one text column per tag and one column per field. New tags and fields add
  columns as they appear, so the schema follows the data like InfluxDB's does.
- A point is identified by its time and tag values. Writing it again overwrites the fields it carries
  and keeps the others, which is how InfluxDB merges re-fetched days. No duplicates, no setup.
- Integer columns widen to double precision when a decimal arrives. A value that can't be stored in a
  column's type is skipped with a warning, the way InfluxDB rejects it.
- At startup the fetcher creates the tables the dashboards query (`postgres_dashboard_tables.json`),
  so panels for data your account doesn't have show "No data" rather than an error.
- `POSTGRES_TIMESCALE=true` makes each table a TimescaleDB hypertable (one-year chunks) when the
  extension is available. Plain PostgreSQL is enough for this data volume.

##### Settings

| Variable | Default | |
|---|---|---|
| `DATABASE_BACKEND` | `influxdb` | `postgres` to use this backend |
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `garmin` / (empty) | |
| `POSTGRES_DATABASE` | `garmin_stats` | must exist |
| `POSTGRES_SSLMODE` | `prefer` | libpq sslmode |
| `POSTGRES_TIMESCALE` | `false` | use hypertables |

##### Moving an existing InfluxDB 1.x database

`influxdb_to_postgres.py` copies every measurement through the same backend, using the usual
`INFLUXDB_*` and `POSTGRES_*` variables. It's safe to run again (points are upserted). With
`--verify` it compares the number of values in every field afterwards.

```
docker compose run --rm -e DATABASE_BACKEND=postgres garmin-fetch-data python /app/garmin_grafana/influxdb_to_postgres.py --verify
```

##### Dashboards

`Grafana_Dashboard/convert_to_postgres.py` generates `*-Postgres.json` versions of the dashboards for
Grafana's PostgreSQL data source. Query-builder panels are translated mechanically; hand-written
InfluxQL needs a translation in the script's `RAW` table. It keeps InfluxDB's series names (target
aliases, `measurement.mean`, ...), so panel overrides and transformations keep working, and it
groups `tz('$TimeZone')` queries in that time zone with `date_bin`.

```
python Grafana_Dashboard/convert_to_postgres.py Grafana_Dashboard/Garmin-Grafana-Dashboard.json Grafana_Dashboard/Garmin-Strength-Training-Dashboard.json
```

##### What was tested (proof of concept)

Against a copy of a real database: 17 measurements, about 1.06 million points, 10 months (Dec 2025 – Oct 2026).

- **Migration:** every field's value count matched, and the sum, min and max of all 139 fields
  (and the latest value of text fields) were identical. It took 49 seconds. A second run changed
  nothing.
- **Fetcher:** imported with `DATABASE_BACKEND=postgres` (the real startup path, including its
  connection-test point). Then, through `write_points_to_influxdb()`:
  - a re-write overwrites only the fields given
  - naive and ISO `Z` times land on the same UTC point
  - a new tag (`User_ID`) starts a separate series
  - integer-to-decimal widening works
  - a wrong-type value is skipped and the rest of the point kept
  - the strength-set purge removes only its activity
  - the last-sync lookup works
  - empty dashboard tables take the type of their first real value
- **Main dashboard:** all 142 panel queries were run through Grafana 13.2.5 against both databases
  over the last 30 and 365 days. 134 returned identical values and series names, 8 were empty in
  both, and 0 failed. They also render identically.
- **Storage:** plain PostgreSQL 148 MB, TimescaleDB 190 MB, InfluxDB 54 MB (InfluxDB compresses;
  TimescaleDB's compression is under its non-OSI license).

##### Not done yet

- The Strength Training dashboard is almost all hand-written InfluxQL: 21 queries still need SQL in `RAW`.
- The "Battery Level" panel queries `"%"` (a GarminHomeAssistant placeholder) and is left disabled.
- `influxdb_exporter.py` (CSV export) still reads InfluxDB only.
- No InfluxDB 3 → PostgreSQL migration (InfluxDB 3 speaks InfluxQL, so it may only need its client).
- Not yet run as a live, long-running fetcher against Garmin.
