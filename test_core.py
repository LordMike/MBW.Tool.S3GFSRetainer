from __future__ import annotations

from main import RetentionPolicy, core_logic


def test_decide_gfs_exact_output_daily_only():
    keys = [
        # 8 distinct days; keep_daily=7 => keep days 08..02 (newest 7), remove day 01
        "ha/backup-2026-01-08T010000Z.tar",
        "ha/backup-2026-01-07T010000Z.tar",
        "ha/backup-2026-01-06T010000Z.tar",
        "ha/backup-2026-01-05T010000Z.tar",
        "ha/backup-2026-01-04T010000Z.tar",
        "ha/backup-2026-01-03T010000Z.tar",
        "ha/backup-2026-01-02T010000Z.tar",
        "ha/backup-2026-01-01T010000Z.tar",
    ]

    policy = RetentionPolicy(
        keep_hourly=0,
        keep_daily=7,
        keep_weekly=0,
        keep_monthly=0,
    )

    actual = core_logic(keys, policy)

    expected = [
        ("ha/backup-2026-01-08T010000Z.tar", "keep", "daily"),
        ("ha/backup-2026-01-07T010000Z.tar", "keep", "daily"),
        ("ha/backup-2026-01-06T010000Z.tar", "keep", "daily"),
        ("ha/backup-2026-01-05T010000Z.tar", "keep", "daily"),
        ("ha/backup-2026-01-04T010000Z.tar", "keep", "daily"),
        ("ha/backup-2026-01-03T010000Z.tar", "keep", "daily"),
        ("ha/backup-2026-01-02T010000Z.tar", "keep", "daily"),
        ("ha/backup-2026-01-01T010000Z.tar", "remove", "none"),
    ]

    assert actual == expected
