CREATE TABLE event_store (
    id INTEGER NOT NULL,
    event_id CHAR(36) NOT NULL,
    event_name VARCHAR(255) NOT NULL,
    event_payload CLOB NOT NULL,
    aggregate_id CHAR(36) NOT NULL,
    aggregate_version INTEGER NOT NULL,
    took_place_at CHAR(31) NOT NULL,
    metadata CLOB NOT NULL,
    PRIMARY KEY(id)
);
CREATE UNIQUE INDEX lookup_idx ON event_store (aggregate_id, aggregate_version);
