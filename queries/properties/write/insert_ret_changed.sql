-- if this query does not return nothing it means that either
-- the state is not the expected one (someone is already modifying the value)
-- the value is the same passed (no need to update the property)
INSERT INTO propcache (
    interface,
    path,
    value,
    type,
    interface_major,
    ownership,
    state
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT DO UPDATE SET
    value = excluded.value,
    type = excluded.type,
    interface_major = excluded.interface_major,
    state = excluded.state
WHERE
    state = ?
    AND value != excluded.value,
RETURNING
    interface, path, value, type, interface_major, ownership;
