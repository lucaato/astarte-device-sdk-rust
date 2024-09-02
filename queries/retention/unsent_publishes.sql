SELECT
    retention_publish.t_millis,
    retention_publish.counter AS "counter: u32",
    retention_publish.interface,
    retention_publish.path,
    retention_publish.sent,
    retention_publish.payload,
    retention_mapping.reliability AS "reliability: u8",
    retention_mapping.major_version AS "major_version: i32",
    retention_mapping.expiry_sec
FROM retention_publish
INNER JOIN retention_mapping USING (interface, path)
WHERE
    retention_publish.sent = FALSE
    AND (retention_publish.expiry_t_secs IS NULL OR retention_publish.expiry_t_secs >= ?)
ORDER BY t_millis ASC, counter ASC
LIMIT ?;