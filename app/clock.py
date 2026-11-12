from __future__ import annotations

import datetime as _dt


def iso_ms(epoch_seconds: float) -> str:
    """RFC 3339 UTC with millisecond precision, e.g. 2026-11-13T05:02:01.123Z."""
    moment = _dt.datetime.fromtimestamp(epoch_seconds, _dt.timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def now_iso() -> str:
    return iso_ms(_dt.datetime.now(_dt.timezone.utc).timestamp())
