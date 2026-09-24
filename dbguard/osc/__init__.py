"""Online schema change for DBGuard (`dbgctl osc`), see docs/OSC.md.

Shadow table, AFTER triggers, chunked copy by primary key, per-chunk CRC32 checksum and an
atomic RENAME TABLE, in the style of pt-online-schema-change. When the ALTER qualifies for
MySQL 8.4's ALGORITHM=INSTANT the tool runs that instead and says so.
"""
