"""When scheduled tasks run next."""
from datetime import datetime, timezone

import pytest

from app.cron import ScheduleError, describe, next_run, parse_cron, validate

UTC = timezone.utc


def at(*args):
    return datetime(*args, tzinfo=UTC)


def test_interval_from_now_and_from_last_run():
    spec = validate({"kind": "interval", "every": 15, "unit": "minutes", "timezone": "UTC"})
    assert next_run(spec, at(2026, 1, 1, 10, 0)) == at(2026, 1, 1, 10, 15)
    # keeps its rhythm after a run
    assert next_run(spec, at(2026, 1, 1, 10, 16), last_run=at(2026, 1, 1, 10, 15)) == at(2026, 1, 1, 10, 30)


def test_interval_minimum():
    with pytest.raises(ScheduleError, match="every 5 seconds"):
        validate({"kind": "interval", "every": 2, "unit": "seconds"})


def test_daily_weekdays_in_a_time_zone():
    spec = validate({"kind": "daily", "at": "09:30", "days": ["mon", "tue", "wed", "thu", "fri"],
                     "timezone": "Asia/Kolkata"})
    # Friday 2026-01-02 10:00 IST has passed 09:30 -> next is Monday 09:30 IST = 04:00 UTC
    assert next_run(spec, at(2026, 1, 2, 4, 30)) == at(2026, 1, 5, 4, 0)
    assert describe(spec) == "weekdays at 09:30 (Asia/Kolkata)"


def test_daily_keeps_local_time_across_daylight_saving():
    spec = validate({"kind": "daily", "at": "09:00", "timezone": "Europe/London"})
    assert next_run(spec, at(2026, 3, 1, 12, 0)) == at(2026, 3, 2, 9, 0)   # winter: UTC+0
    assert next_run(spec, at(2026, 4, 1, 12, 0)) == at(2026, 4, 2, 8, 0)   # summer: UTC+1


@pytest.mark.parametrize("expr, after, expected", [
    ("*/15 * * * *", at(2026, 1, 1, 10, 7), at(2026, 1, 1, 10, 15)),
    ("0 9 * * mon-fri", at(2026, 1, 2, 9, 0), at(2026, 1, 5, 9, 0)),       # Fri 09:00 -> Mon 09:00
    ("30 2 1 * *", at(2026, 1, 15), at(2026, 2, 1, 2, 30)),
    ("0 0 29 2 *", at(2026, 3, 1), at(2028, 2, 29, 0, 0)),                 # next leap day
    ("0 12 * * 7", at(2026, 1, 1), at(2026, 1, 4, 12, 0)),                 # 7 = Sunday
    ("0 6 1 * mon", at(2026, 1, 2), at(2026, 1, 5, 6, 0)),                 # day OR weekday when both set
])
def test_cron(expr, after, expected):
    spec = validate({"kind": "cron", "cron": expr, "timezone": "UTC"})
    assert next_run(spec, after) == expected


@pytest.mark.parametrize("expr", ["* * * *", "60 * * * *", "* 24 * * *", "5-1 * * * *", "*/0 * * * *", "x * * * *"])
def test_bad_cron(expr):
    with pytest.raises(ScheduleError):
        parse_cron(expr)


def test_once_runs_once():
    spec = validate({"kind": "once", "run_at": "2026-06-01T08:00", "timezone": "UTC"})
    assert next_run(spec, at(2026, 5, 1)) == at(2026, 6, 1, 8, 0)
    assert next_run(spec, at(2026, 7, 1)) is None
    assert next_run(spec, at(2026, 5, 1), last_run=at(2026, 6, 1, 8, 0)) is None


def test_unknown_zone():
    with pytest.raises(ScheduleError, match="time zone"):
        validate({"kind": "interval", "every": 1, "unit": "hours", "timezone": "Mars/Base"})
