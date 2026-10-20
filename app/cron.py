"""When a scheduled task runs next: every N seconds/minutes/hours, daily at a time, a cron expression, or once.

Schedules are evaluated in their own time zone (default: the server's), so "daily at 09:00" stays at 09:00 across
daylight-saving changes.
"""
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

KINDS = ("interval", "daily", "cron", "once")
UNITS = {"seconds": 1, "minutes": 60, "hours": 3600}
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
MIN_INTERVAL_SECONDS = 5


class ScheduleError(ValueError):
    pass


def local_zone() -> str:
    """The server's IANA time zone name (e.g. Asia/Kolkata), or UTC."""
    if os.getenv("TZ"):
        return os.getenv("TZ")
    try:
        path = os.path.realpath("/etc/localtime")
        if "zoneinfo/" in path:
            return path.split("zoneinfo/", 1)[1]
    except OSError:
        pass
    return "UTC"


def zone(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(name or local_zone())
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ScheduleError(f"Unknown time zone '{name}'.") from e


# ---------- cron: minute hour day-of-month month day-of-week ----------

_CRON_RANGES = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 6))
_CRON_NAMES = {3: ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
               4: ["sun", "mon", "tue", "wed", "thu", "fri", "sat"]}


def parse_cron(expr: str) -> list[set[int]]:
    """Five fields; each is *, a number, a range a-b, a list a,b,c, or a step */n or a-b/n. Names allowed for
    months (jan) and weekdays (mon); 7 also means Sunday."""
    fields = (expr or "").split()
    if len(fields) != 5:
        raise ScheduleError("A cron expression has 5 fields: minute hour day-of-month month day-of-week "
                            "(e.g. '0 9 * * mon-fri').")
    out = []
    for i, field in enumerate(fields):
        lo, hi = _CRON_RANGES[i]
        values: set[int] = set()
        for part in field.lower().split(","):
            rng, _, step = part.partition("/")
            try:
                step_n = int(step) if step else 1
                if rng == "*":
                    a, b = lo, hi
                elif "-" in rng:
                    a, b = (_cron_value(x, i) for x in rng.split("-", 1))
                else:
                    a = b = _cron_value(rng, i)
                    if step:
                        b = hi
            except ValueError as e:
                raise ScheduleError(f"Can't read '{part}' in the cron expression.") from e
            if step_n < 1 or not (lo <= a <= (7 if i == 4 else hi)) or not (lo <= b <= (7 if i == 4 else hi)) or a > b:
                raise ScheduleError(f"'{part}' is out of range in the cron expression.")
            values.update(v % 7 if i == 4 else v for v in range(a, b + 1, step_n))
        out.append(values)
    return out


def _cron_value(text: str, field: int) -> int:
    names = _CRON_NAMES.get(field)
    if names and text[:3] in names:
        return names.index(text[:3]) + (1 if field == 3 else 0)
    return int(text)


def _cron_next(expr: str, after: datetime) -> datetime:
    minutes, hours, mdays, months, wdays = parse_cron(expr)
    restricted_mday, restricted_wday = len(mdays) < 31, len(wdays) < 7
    t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
    limit = after + timedelta(days=366 * 5)
    while t < limit:
        if t.month not in months:
            t = (t.replace(day=1, hour=0, minute=0) + timedelta(days=32)).replace(day=1)
            continue
        cron_wday = (t.weekday() + 1) % 7  # cron: 0 = Sunday
        day_ok = (t.day in mdays or cron_wday in wdays) if (restricted_mday and restricted_wday) \
            else (t.day in mdays and cron_wday in wdays)
        if not day_ok:
            t = t.replace(hour=0, minute=0) + timedelta(days=1)
            continue
        if t.hour not in hours:
            t = t.replace(minute=0) + timedelta(hours=1)
            continue
        if t.minute not in minutes:
            t += timedelta(minutes=1)
            continue
        return t
    raise ScheduleError("This cron expression never matches a date.")


# ---------- all kinds ----------

def validate(spec: dict) -> dict:
    """Check a schedule and return it cleaned up."""
    kind = spec.get("kind")
    if kind not in KINDS:
        raise ScheduleError("Choose how often the task runs.")
    tz = spec.get("timezone") or local_zone()
    zone(tz)
    out = {"kind": kind, "timezone": tz}
    if kind == "interval":
        unit = spec.get("unit") or "minutes"
        if unit not in UNITS:
            raise ScheduleError("The interval unit must be seconds, minutes or hours.")
        try:
            every = int(spec.get("every") or 0)
        except (TypeError, ValueError) as e:
            raise ScheduleError("The interval must be a whole number.") from e
        if every * UNITS[unit] < MIN_INTERVAL_SECONDS:
            raise ScheduleError(f"Run at most every {MIN_INTERVAL_SECONDS} seconds.")
        out.update(every=every, unit=unit)
    elif kind == "daily":
        at = (spec.get("at") or "").strip()
        try:
            datetime.strptime(at, "%H:%M")
        except ValueError as e:
            raise ScheduleError("Give the time of day as HH:MM, e.g. 09:30.") from e
        days = [d for d in (spec.get("days") or DAYS) if d in DAYS]
        if not days:
            raise ScheduleError("Pick at least one day of the week.")
        out.update(at=at, days=[d for d in DAYS if d in days])
    elif kind == "cron":
        parse_cron(spec.get("cron") or "")
        out["cron"] = " ".join(spec["cron"].split())
    else:
        try:
            run_at = datetime.fromisoformat(str(spec.get("run_at") or ""))
        except ValueError as e:
            raise ScheduleError("Give the date and time to run, e.g. 2026-11-01T09:00.") from e
        out["run_at"] = run_at.replace(tzinfo=None).isoformat(timespec="minutes")
    return out


def next_run(spec: dict, after: datetime | None = None, last_run: datetime | None = None) -> datetime | None:
    """The next time (UTC) the task should run after `after` (default now). None = never again."""
    after = (after or datetime.now(timezone.utc)).astimezone(timezone.utc)
    tz = zone(spec.get("timezone"))
    kind = spec["kind"]
    if kind == "interval":
        step = timedelta(seconds=int(spec["every"]) * UNITS[spec.get("unit") or "minutes"])
        return (last_run + step) if last_run and last_run + step > after else after + step
    if kind == "once":
        at = datetime.fromisoformat(spec["run_at"]).replace(tzinfo=tz).astimezone(timezone.utc)
        return at if at > after and not last_run else None
    local = after.astimezone(tz)
    if kind == "daily":
        hh, mm = (int(x) for x in spec["at"].split(":"))
        for i in range(8):
            day = (local + timedelta(days=i)).date()
            candidate = datetime(day.year, day.month, day.day, hh, mm, tzinfo=tz)
            if candidate > local and DAYS[candidate.weekday()] in spec["days"]:
                return candidate.astimezone(timezone.utc)
        return None
    return _cron_next(spec["cron"], local.replace(tzinfo=None)).replace(tzinfo=tz).astimezone(timezone.utc)


def describe(spec: dict) -> str:
    kind = spec.get("kind")
    tz = spec.get("timezone") or ""
    if kind == "interval":
        n, unit = int(spec.get("every") or 0), spec.get("unit") or "minutes"
        return f"every {n} {unit[:-1] if n == 1 else unit}"
    if kind == "daily":
        days = spec.get("days") or DAYS
        when = "every day" if len(days) == 7 else "weekdays" if days == list(DAYS[:5]) else \
            ", ".join(d.capitalize() for d in days)
        return f"{when} at {spec.get('at')} ({tz})"
    if kind == "cron":
        return f"cron '{spec.get('cron')}' ({tz})"
    if kind == "once":
        return f"once at {str(spec.get('run_at', '')).replace('T', ' ')} ({tz})"
    return "?"
