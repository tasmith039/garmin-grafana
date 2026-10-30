"""Integration tests for the PostgreSQL backend, against a real (disposable) PostgreSQL.

Each run creates its own database and drops it afterwards. Point it at any PostgreSQL 14+ server
whose user may create databases, for example:

  docker run -d --rm --name garmin-pg-test -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17
  POSTGRES_TEST_PORT=55432 POSTGRES_TEST_PASSWORD=test uv run python -m unittest discover -s tests -v
  docker stop garmin-pg-test

Without POSTGRES_TEST_PASSWORD (or POSTGRES_TEST_HOST) set, the database tests are skipped; the
schema check against the fetcher's code runs either way.
"""
import ast
import importlib
import json
import os
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC))

SERVER = dict(host=os.getenv("POSTGRES_TEST_HOST", "localhost"), port=int(os.getenv("POSTGRES_TEST_PORT", 5432)),
              user=os.getenv("POSTGRES_TEST_USER", "postgres"), password=os.getenv("POSTGRES_TEST_PASSWORD", ""))
HAVE_SERVER = bool(os.getenv("POSTGRES_TEST_PASSWORD") or os.getenv("POSTGRES_TEST_HOST"))
DASHBOARD_TABLES = json.loads((SRC / "garmin_grafana" / "postgres_dashboard_tables.json").read_text())


def fetcher_schema():
    """{measurement: (tag keys, field keys)} from the point dicts written literally in garmin_fetch.py."""
    tree = ast.parse((SRC / "garmin_grafana" / "garmin_fetch.py").read_text())
    found = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        d = {k.value: v for k, v in zip(node.keys, node.values) if isinstance(k, ast.Constant)}
        if not isinstance(d.get("measurement"), ast.Constant):
            continue
        tags, fields = found.setdefault(d["measurement"].value, (set(), set()))
        for key, keys in (("tags", tags), ("fields", fields)):
            if isinstance(d.get(key), ast.Dict):
                keys.update(k.value for k in d[key].keys if isinstance(k, ast.Constant))
    return found


class DashboardTablesMatchFetcher(unittest.TestCase):
    """postgres_dashboard_tables.json is created at startup, before any data: a column it makes a tag
    must be a tag in the fetcher too, or the fetcher's writes to that table fail."""

    def test_tags_and_fields_agree(self):
        fetcher = fetcher_schema()
        for table, cols in DASHBOARD_TABLES.items():
            tags, fields = fetcher.get(table, (set(), set()))
            for tag in cols["tags"]:
                self.assertIn(tag, tags, f"{table}.{tag} is a tag in the startup schema but not in garmin_fetch.py")
            for field in cols["fields"]:
                self.assertNotIn(field, tags, f"{table}.{field} is a field in the startup schema but a tag in garmin_fetch.py")


@unittest.skipUnless(HAVE_SERVER, "set POSTGRES_TEST_PASSWORD (and POSTGRES_TEST_HOST/PORT/USER) to run")
class PostgresTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.psycopg = psycopg
        cls.dbname = f"garmin_test_{uuid.uuid4().hex[:8]}"
        with psycopg.connect(**SERVER, dbname="postgres", autocommit=True) as admin:
            admin.execute(f'CREATE DATABASE "{cls.dbname}"')

    @classmethod
    def tearDownClass(cls):
        with cls.psycopg.connect(**SERVER, dbname="postgres", autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{cls.dbname}" WITH (FORCE)')

    def client(self):
        from garmin_grafana.postgres_backend import PostgresClient
        c = PostgresClient(**SERVER, database=self.dbname)
        self.addCleanup(c.close)
        return c

    def kill_connection(self, client):
        """Ends the client's server connection, as a database restart would."""
        pid = client.conn.info.backend_pid
        with self.psycopg.connect(**SERVER, dbname="postgres", autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (pid,))

    def rows(self, client, sql):
        return client.conn.execute(sql).fetchall()


class Backend(PostgresTestCase):
    T = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    def test_rewrite_overwrites_only_given_fields(self):
        c = self.client()
        c.write_points([{"measurement": "Rewrite", "time": self.T, "tags": {"Device": "w"}, "fields": {"a": 1, "b": 2}}])
        c.write_points([{"measurement": "Rewrite", "time": self.T, "tags": {"Device": "w"}, "fields": {"b": 3}}])
        self.assertEqual(self.rows(c, 'SELECT a, b FROM "Rewrite"'), [(1, 3)])

    def test_missing_field_values_are_left_out(self):
        c = self.client()
        c.write_points([{"measurement": "Sparse", "time": self.T, "tags": {}, "fields": {"spo2": 95.0}}])
        # None for a field the table has, and for one it has never seen
        c.write_points([{"measurement": "Sparse", "time": self.T, "tags": {},
                         "fields": {"spo2": None, "skinTemp": None, "hr": 50}}])
        self.assertEqual(self.rows(c, 'SELECT spo2, hr FROM "Sparse"'), [(95.0, 50)])
        self.assertEqual(c.schema("Sparse")[1], ["spo2", "hr"])

    def test_all_fields_missing_writes_nothing(self):
        c = self.client()
        c.write_points([{"measurement": "Empty", "time": self.T, "tags": {}, "fields": {"x": None}}])
        self.assertIsNone(c.last_time("Empty"))

    def test_missing_tag_value_is_no_tag(self):
        c = self.client()
        c.write_points([{"measurement": "NoneTag", "time": self.T, "tags": {"Device": None}, "fields": {"x": 1}}])
        c.write_points([{"measurement": "NoneTag", "time": self.T, "tags": {}, "fields": {"y": 2}}])
        self.assertEqual(self.rows(c, 'SELECT x, y FROM "NoneTag"'), [(1, 2)])

    def test_tag_first_seen_later_in_a_batch(self):
        c = self.client()
        c.write_points([
            {"measurement": "LateTag", "time": self.T, "tags": {"Device": "w"}, "fields": {"x": 1}},
            {"measurement": "LateTag", "time": self.T, "tags": {"Device": "w", "User_ID": "u"}, "fields": {"x": 2}},
        ])
        self.assertEqual(sorted(self.rows(c, 'SELECT "User_ID", x FROM "LateTag"')), [("", 1), ("u", 2)])

    def test_new_tag_starts_a_new_series(self):
        c = self.client()
        c.write_points([{"measurement": "Series", "time": self.T, "tags": {"Device": "w"}, "fields": {"x": 1}}])
        c.write_points([{"measurement": "Series", "time": self.T, "tags": {"Device": "w", "User_ID": "u"}, "fields": {"x": 2}}])
        c.write_points([{"measurement": "Series", "time": self.T, "tags": {"Device": "w"}, "fields": {"x": 3}}])
        self.assertEqual(sorted(self.rows(c, 'SELECT "User_ID", x FROM "Series"')), [("", 3), ("u", 2)])

    def test_time_formats_land_on_the_same_point(self):
        c = self.client()
        for t in ["2026-01-02T03:04:05Z", "2026-01-02T03:04:05+00:00", datetime(2026, 1, 2, 3, 4, 5),
                  int(self.T.timestamp()) * 10**9]:
            c.write_points([{"measurement": "Times", "time": t, "tags": {}, "fields": {"x": 1}}])
        self.assertEqual(self.rows(c, 'SELECT count(*) FROM "Times"'), [(1,)])

    def test_integer_column_widens_to_float(self):
        c = self.client()
        c.write_points([{"measurement": "Widen", "time": self.T, "tags": {}, "fields": {"x": 60}}])
        c.write_points([{"measurement": "Widen", "time": "2026-01-03T00:00:00Z", "tags": {}, "fields": {"x": 60.5}}])
        self.assertEqual(sorted(r[0] for r in self.rows(c, 'SELECT x FROM "Widen"')), [60.0, 60.5])

    def test_wrong_type_value_is_skipped_rest_kept(self):
        c = self.client()
        c.write_points([{"measurement": "Types", "time": self.T, "tags": {}, "fields": {"x": 1.5}}])
        with self.assertLogs(level="WARNING"):
            c.write_points([{"measurement": "Types", "time": self.T, "tags": {}, "fields": {"x": "oops", "y": 2}}])
        self.assertEqual(self.rows(c, 'SELECT x, y FROM "Types"'), [(1.5, 2)])

    def test_delete_series_removes_only_matching_tags(self):
        c = self.client()
        c.write_points([{"measurement": "Sets", "time": self.T, "tags": {"ActivityID": str(a)}, "fields": {"Reps": 5}}
                        for a in (1, 2)])
        c.delete_series("Sets", {"ActivityID": "1"})
        self.assertEqual(self.rows(c, 'SELECT "ActivityID" FROM "Sets"'), [("2",)])

    def test_empty_precreated_column_takes_first_values_type(self):
        c = self.client()
        c.ensure_tables({"Pre": {"tags": [], "fields": ["name"]}})
        c.write_points([{"measurement": "Pre", "time": self.T, "tags": {}, "fields": {"name": "Run"}}])
        self.assertEqual(self.rows(c, 'SELECT name FROM "Pre"'), [("Run",)])

    def test_reconnects_after_losing_the_connection(self):
        c = self.client()
        c.write_points([{"measurement": "Reconnect", "time": self.T, "tags": {}, "fields": {"x": 1}}])
        self.kill_connection(c)
        with self.assertLogs(level="WARNING"):
            c.write_points([{"measurement": "Reconnect", "time": "2026-01-03T00:00:00Z", "tags": {}, "fields": {"x": 2}}])
        self.assertEqual(self.rows(c, 'SELECT count(*) FROM "Reconnect"'), [(2,)])
        self.kill_connection(c)
        self.assertEqual(c.last_time("Reconnect"), datetime(2026, 1, 3, tzinfo=timezone.utc))


class FakeGarmin:
    """The few Garmin Connect calls these tests make, answered with made-up data."""
    display_name = "tester"

    def get_activities_by_date(self, start, end):
        return [{"activityId": 1001, "activityName": "Morning Run", "activityType": {"typeKey": "running"},
                 "startTimeGMT": "2026-01-02 07:00:00", "duration": 1800.0, "distance": 5000.0, "averageHR": 150.0,
                 "hrTimeInZone_1": 60.0, "description": None, "locationName": None, "hasPolyline": False}]

    def get_activity_hr_in_timezones(self, activity_id):
        return [{"zoneNumber": n, "zoneLowBoundary": 90 + 15 * n} for n in range(1, 6)]

    def __init__(self):
        self.sleep = {"sleepEndTimestampGMT": 1767337200000, "sleepTimeSeconds": 27000, "averageSpO2Value": 95.0}
        self.skin = None

    def get_sleep_data(self, date_str):
        return {"dailySleepDTO": dict(self.sleep), "avgSkinTempDeviationC": self.skin}


class Fetcher(PostgresTestCase):
    """garmin_fetch.py itself, imported with DATABASE_BACKEND=postgres: the real startup (connection-test
    point and startup schema), then its own point builders writing to a fresh database."""

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        os.environ.update(DATABASE_BACKEND="postgres", POSTGRES_HOST=SERVER["host"], POSTGRES_PORT=str(SERVER["port"]),
                          POSTGRES_USER=SERVER["user"], POSTGRES_PASSWORD=SERVER["password"], POSTGRES_DATABASE=cls.dbname)
        cls.fetch = importlib.import_module("garmin_grafana.garmin_fetch")
        cls.fetch.garmin_obj = FakeGarmin()

    @classmethod
    def tearDownClass(cls):
        cls.fetch.influxdbclient.close()
        super().tearDownClass()

    def setUp(self):
        self.db = self.fetch.influxdbclient

    def test_startup_creates_the_dashboard_tables(self):
        for table in DASHBOARD_TABLES:
            self.assertTrue(self.db.schema(table)[1], f"{table} not created")

    def test_activity_summary_after_startup_schema(self):
        self.fetch.write_points_to_influxdb(self.fetch.get_activity_summary("2026-01-02")[0])
        self.assertEqual(sorted(self.rows(self.db, 'SELECT "activityName", "activityType" FROM "ActivitySummary"')),
                         [("END", "No Activity"), ("Morning Run", "running")])

    def test_sleep_summary_missing_values_keep_stored_ones(self):
        before = self.fetch.database_write_failures
        self.fetch.garmin_obj.sleep["averageSpO2Value"], self.fetch.garmin_obj.skin = 95.0, -0.2
        self.fetch.write_points_to_influxdb(self.fetch.get_sleep_data("2026-01-02"))
        self.fetch.garmin_obj.sleep["averageSpO2Value"], self.fetch.garmin_obj.skin = None, None  # watch didn't report them
        self.fetch.write_points_to_influxdb(self.fetch.get_sleep_data("2026-01-02"))
        self.assertEqual(self.rows(self.db, 'SELECT "averageSpO2Value", "avgSkinTempDeviationC" FROM "SleepSummary"'),
                         [(95.0, -0.2)])
        self.assertEqual(self.fetch.database_write_failures, before)

    def test_writes_continue_after_losing_the_connection(self):
        self.kill_connection(self.db)
        before = self.fetch.database_write_failures
        self.fetch.write_points_to_influxdb([{"measurement": "HeartRateIntraday", "time": "2026-01-02T08:00:00Z",
                                              "tags": {}, "fields": {"HeartRate": 70}}])
        self.assertEqual(self.fetch.database_write_failures, before)
        self.assertEqual(self.db.last_time("HeartRateIntraday"), datetime(2026, 1, 2, 8, tzinfo=timezone.utc))

    def test_failed_write_is_counted(self):
        before = self.fetch.database_write_failures
        broken = {"measurement": "HeartRateIntraday", "time": "2026-01-02T09:00:00Z", "tags": {}, "fields": {"HeartRate": 71}}
        real, self.fetch.influxdbclient = self.fetch.influxdbclient, BrokenClient()
        try:
            self.fetch.write_points_to_influxdb([broken])
        finally:
            self.fetch.influxdbclient = real
        self.assertEqual(self.fetch.database_write_failures, before + 1)


class BrokenClient:
    def write_points(self, points):
        import psycopg
        raise psycopg.OperationalError("database is down")


if __name__ == "__main__":
    unittest.main()
