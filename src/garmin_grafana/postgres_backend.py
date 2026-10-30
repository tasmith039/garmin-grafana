"""PostgreSQL (optionally TimescaleDB) storage for garmin-grafana.

Takes the same points the InfluxDB writers get ({'measurement', 'time', 'tags', 'fields'}) and keeps
InfluxDB's behaviour, so nothing upstream of write_points() has to change:

- One table per measurement, created on first write, with a `time` column, one text column per tag
  and one column per field. New tags and fields add columns as they appear (schemaless like Influx).
- A point is identified by its time and tag values. Writing it again overwrites the fields it
  carries and leaves the others alone, which is how InfluxDB merges re-fetched data.
- Missing values (None) are left out, as the InfluxDB clients do: a field set to None keeps its
  stored value, and a tag set to None counts as absent.
- Field types follow the first value seen; an integer column widens to double precision when a
  float arrives, and values that can't be stored in a column's type are skipped with a warning
  (InfluxDB rejects those writes).

With POSTGRES_TIMESCALE=true and the timescaledb extension available, new tables become hypertables.
"""
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone

import psycopg
from psycopg import sql

TYPES = {bool: "boolean", int: "bigint", float: "double precision", str: "text"}


def _pg_type(value):
    return TYPES.get(type(value), "text")


def _as_time(value):
    """Point times as written by garmin_fetch: aware or naive (UTC) datetimes, ISO strings, epoch ns."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1e9, tz=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class PostgresClient:
    def __init__(self, host, port, user, password, database, sslmode="prefer", timescale=False):
        self.params = dict(host=host, port=port, user=user, password=password, dbname=database, sslmode=sslmode)
        self._connect()
        self.timescale = timescale and self._has_timescale()

    def _connect(self):
        self.conn = psycopg.connect(**self.params, autocommit=True)
        self.columns = {}  # table -> {column: type}
        self.tags = {}     # table -> [tag columns, in key order]

    def _retry(self, fn, *args):
        """Run fn, and once more on a new connection if the old one was lost (database restarted, idle
        connection dropped). Everything here is safe to repeat: writes are upserts."""
        try:
            return fn(*args)
        except (psycopg.OperationalError, psycopg.InterfaceError):
            if not self.conn.closed and not self.conn.broken:
                raise
            logging.warning("Lost the PostgreSQL connection, reconnecting")
            self._connect()  # also forgets the cached schema, in case a change didn't finish
            return fn(*args)

    # ------------------------------------------------------------------ schema

    def _has_timescale(self):
        try:
            self.conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
            return True
        except psycopg.Error as err:
            logging.warning(f"TimescaleDB not available, using plain PostgreSQL tables: {err}")
            return False

    def _load(self, table):
        rows = self.conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() "
            "AND table_name = %s", (table,)).fetchall()
        self.columns[table] = {name: kind for name, kind in rows}
        comment = self.conn.execute(
            "SELECT obj_description(to_regclass(%s), 'pg_class')", (sql.Identifier(table).as_string(self.conn),)
        ).fetchone()[0] or ""
        self.tags[table] = [t for t in comment.removeprefix("tags:").split(",") if t] if comment.startswith("tags:") else []

    def _ensure(self, table, tags, fields):
        if table not in self.columns:
            self._load(table)
        cols = self.columns[table]
        created = not cols
        if created:
            self.conn.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {} (time timestamptz NOT NULL)").format(sql.Identifier(table)))
            if self.timescale:
                # One chunk per year: a person's Garmin data is a few MB a year, and small chunks only add overhead.
                self.conn.execute("SELECT create_hypertable(%s, 'time', chunk_time_interval => interval '1 year', "
                                  "if_not_exists => true, migrate_data => true)",
                                  (sql.Identifier(table).as_string(self.conn),))  # regclass text: quoted, it's mixed case
            cols["time"] = "timestamp with time zone"
        new_tags = [t for t in tags if t not in self.tags[table]]
        for tag in new_tags:
            self.conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} text NOT NULL DEFAULT ''").format(
                sql.Identifier(table), sql.Identifier(tag)))
            cols[tag] = "text"
        for name, value in fields.items():
            if value is None:
                continue
            want = _pg_type(value)
            have = cols.get(name)
            if have is None:
                self.conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                    sql.Identifier(table), sql.Identifier(name), sql.SQL(want)))
                cols[name] = want
            elif have == "bigint" and want == "double precision":
                self.conn.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE double precision").format(
                    sql.Identifier(table), sql.Identifier(name)))
                cols[name] = want
            elif have != want and not self._fits(table, name, value) and not self.conn.execute(
                    sql.SQL("SELECT EXISTS (SELECT 1 FROM {} WHERE {} IS NOT NULL)").format(
                        sql.Identifier(table), sql.Identifier(name))).fetchone()[0]:
                # A column with no values yet (e.g. created by ensure_tables) takes its first value's type.
                self.conn.execute(sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE {} USING NULL").format(
                    sql.Identifier(table), sql.Identifier(name), sql.SQL(want)))
                cols[name] = want
        if created or new_tags:
            # The series key is time plus every tag; it's what ON CONFLICT matches, so it grows with the tags.
            self.tags[table] = self.tags[table] + new_tags
            key = sql.SQL(", ").join(map(sql.Identifier, ["time", *self.tags[table]]))
            index = sql.Identifier(f"{table}_series_key")
            self.conn.execute(sql.SQL("DROP INDEX IF EXISTS {}").format(index))
            self.conn.execute(sql.SQL("CREATE UNIQUE INDEX {} ON {} ({})").format(index, sql.Identifier(table), key))
            self.conn.execute(sql.SQL("COMMENT ON TABLE {} IS {}").format(
                sql.Identifier(table), sql.Literal("tags:" + ",".join(self.tags[table]))))

    def _fits(self, table, name, value):
        have = self.columns[table].get(name)
        if have == "text" or value is None:
            return True
        if have == "boolean":
            return isinstance(value, bool)
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    # ------------------------------------------------------------------ InfluxDB-style API

    def write_points(self, points):
        """Upsert points; same time + tags overwrites the fields given, like InfluxDB."""
        return self._retry(self._write_points, points)

    def _write_points(self, points):
        batches = defaultdict(list)
        for p in points:
            table = p["measurement"]
            tags = {k: str(v) for k, v in (p.get("tags") or {}).items() if v is not None}
            fields = {k: v for k, v in (p.get("fields") or {}).items() if v is not None}
            self._ensure(table, tags, fields)
            for name, value in list(fields.items()):
                if not self._fits(table, name, value):
                    logging.warning(f"Skipping {table}.{name}={value!r}: column is {self.columns[table][name]}")
                    del fields[name]
            if not fields:
                continue
            batches[(table, tuple(sorted(fields)))].append((_as_time(p["time"]), tags, fields))
        with self.conn.transaction():
            for (table, names), rows in batches.items():
                key = ["time", *self.tags[table]]
                cols = key + list(names)
                stmt = sql.SQL("INSERT INTO {t} ({c}) VALUES ({v}) ON CONFLICT ({k}) DO UPDATE SET {u}").format(
                    t=sql.Identifier(table), c=sql.SQL(", ").join(map(sql.Identifier, cols)),
                    v=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
                    k=sql.SQL(", ").join(map(sql.Identifier, key)),
                    u=sql.SQL(", ").join(sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(n)) for n in names))
                with self.conn.cursor() as cur:
                    # tags as of the whole batch: a later point may have added one an earlier point lacks
                    cur.executemany(stmt, [[t, *(tg.get(k, "") for k in self.tags[table]), *(f[n] for n in names)]
                                           for t, tg, f in rows])
        return True

    def ensure_tables(self, schema):
        """Create the tables (and columns) the dashboards query before any data arrives, so panels for
        data an account doesn't have show "No data" instead of a "relation does not exist" error.
        schema: {measurement: {"tags": [names], "fields": [names]}} (postgres_dashboard_tables.json).
        Fields start as double precision; the first real value written retypes an empty column."""
        self._retry(self._ensure_tables, schema)

    def _ensure_tables(self, schema):
        for table, cols in schema.items():
            self._ensure(table, dict.fromkeys(cols.get("tags", []), ""), dict.fromkeys(cols.get("fields", []), 0.0))

    def schema(self, measurement):
        """(tag columns, field columns) of a measurement's table; both empty if it doesn't exist."""
        if measurement not in self.columns:
            self._load(measurement)
        tags = list(self.tags[measurement])
        return tags, [c for c in self.columns[measurement] if c != "time" and c not in tags]

    def last_time(self, measurement):
        """Newest point's time in a measurement, or None."""
        return self._retry(self._last_time, measurement)

    def _last_time(self, measurement):
        try:
            return self.conn.execute(sql.SQL("SELECT max(time) FROM {}").format(sql.Identifier(measurement))).fetchone()[0]
        except psycopg.errors.UndefinedTable:
            return None

    def delete_series(self, measurement, tags):
        """Delete every point of a measurement whose tags match (InfluxDB's DROP SERIES ... WHERE)."""
        return self._retry(self._delete_series, measurement, tags)

    def _delete_series(self, measurement, tags):
        if measurement not in self.columns:
            self._load(measurement)
        if not self.columns[measurement]:
            return
        where = sql.SQL(" AND ").join(sql.SQL("{} = {}").format(sql.Identifier(k), sql.Literal(str(v))) for k, v in tags.items())
        self.conn.execute(sql.SQL("DELETE FROM {} WHERE {}").format(sql.Identifier(measurement), where))

    def close(self):
        self.conn.close()
