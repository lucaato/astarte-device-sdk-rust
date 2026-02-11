UPDATE propcache
SET
    sent_timestamp = ?
WHERE
    interface = ?
    AND path = ?;
