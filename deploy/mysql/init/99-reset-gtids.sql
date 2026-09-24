-- Anything the image's own init wrote to the binlog (timezone tables, root setup) is wiped,
-- so every node of a set starts with an empty, identical gtid_executed. Bootstrap relies on it.
RESET BINARY LOGS AND GTIDS;
