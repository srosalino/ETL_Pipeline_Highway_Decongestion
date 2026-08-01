CREATE DATABASE IF NOT EXISTS tolldata;

USE tolldata;

CREATE TABLE IF NOT EXISTS livetolldata (
    event_timestamp DATETIME NOT NULL,
    vehicle_id BIGINT NOT NULL,
    vehicle_type VARCHAR(15) NOT NULL,
    toll_plaza_id SMALLINT NOT NULL
);