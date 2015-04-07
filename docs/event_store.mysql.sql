CREATE TABLE event_store (
    id INT AUTO_INCREMENT NOT NULL,
    event_id CHAR(36) NOT NULL,
    event_name VARCHAR(255) NOT NULL,
    event_payload TEXT NOT NULL,
    aggregate_id CHAR(36) NOT NULL,
    aggregate_version INT NOT NULL,
    took_place_at CHAR(31) NOT NULL,
    metadata TEXT NOT NULL,
    UNIQUE INDEX lookup_idx (aggregate_id, aggregate_version),
    PRIMARY KEY(id)
) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci ENGINE = InnoDB;
