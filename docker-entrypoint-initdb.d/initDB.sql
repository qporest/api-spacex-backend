CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION postgis;

\c spacex


CREATE TABLE satelites (
	id uuid PRIMARY KEY,
	name TEXT
);

CREATE TABLE satelite_positions (
	recorded_at TIMESTAMP,
	satelite_id INT,
	longitude NUMERIC,
	latitude NUMERIC,
	CONSTRAINT fk_satelite
		FOREIGN KEY(satelite_id)
			REFERENCES satelites(id)
);

SELECT create_hypertable("satelite_positions", "recorded_at", "satelite_id", 3, create_default_indexes=>FALSE);
CREATE INDEX ON satelite_positions (satelite_id, recorded_at DESC);