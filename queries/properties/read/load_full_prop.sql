SELECT
    interface,
    path,
    value,
    type,
    interface_major,
    ownership,
    state
FROM propcache
WHERE
    interface = ?
    AND path = ?;

