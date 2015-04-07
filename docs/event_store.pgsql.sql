CREATE TABLE event_store (
    id INT NOT NULL,
    event_id UUID NOT NULL,
    event_name VARCHAR(255) NOT NULL,
    event_payload TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    aggregate_version INT NOT NULL,
    took_place_at VARCHAR(31) NOT NULL,
    metadata TEXT NOT NULL,
    PRIMARY KEY(id)
);
CREATE UNIQUE INDEX lookup_idx ON event_store (aggregate_id, aggregate_version);
CREATE SEQUENCE event_store_id_seq INCREMENT BY 1 MINVALUE 1 START 1;
