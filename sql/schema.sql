-- DSS150P Lab 4: PostgreSQL schema for the curated layer and run records.
-- Safe to run more than once (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS curated_tracks (
    track_id          TEXT        PRIMARY KEY,
    source_row_key    INTEGER     NOT NULL UNIQUE,
    track_genre       TEXT        NOT NULL,
    danceability      DOUBLE PRECISION NOT NULL CHECK (danceability BETWEEN 0 AND 1),
    energy            DOUBLE PRECISION NOT NULL CHECK (energy BETWEEN 0 AND 1),
    valence           DOUBLE PRECISION NOT NULL CHECK (valence BETWEEN 0 AND 1),
    acousticness      DOUBLE PRECISION NOT NULL CHECK (acousticness BETWEEN 0 AND 1),
    liveness          DOUBLE PRECISION NOT NULL CHECK (liveness BETWEEN 0 AND 1),
    loudness          DOUBLE PRECISION NOT NULL,
    tempo             DOUBLE PRECISION NOT NULL,
    key               SMALLINT    NOT NULL CHECK (key BETWEEN 0 AND 11),
    mode              SMALLINT    NOT NULL CHECK (mode IN (0, 1)),
    time_signature    SMALLINT    NOT NULL,
    instrumentalness  DOUBLE PRECISION NOT NULL CHECK (instrumentalness BETWEEN 0 AND 1),
    speechiness       DOUBLE PRECISION NOT NULL CHECK (speechiness BETWEEN 0 AND 1),
    popularity        SMALLINT    NOT NULL CHECK (popularity BETWEEN 0 AND 100),
    popularity_tier   TEXT        NOT NULL CHECK (popularity_tier IN ('Low', 'Mid', 'High')),
    record_hash       CHAR(64)    NOT NULL,
    pipeline_run_id   TEXT        NOT NULL,
    processed_at_utc  TIMESTAMPTZ NOT NULL,
    loaded_at_utc     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    pipeline_run_id   TEXT        NOT NULL,
    stage             TEXT        NOT NULL,
    status            TEXT        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    started_at_utc    TIMESTAMPTZ NOT NULL,
    finished_at_utc   TIMESTAMPTZ,
    rows_read         INTEGER,
    rows_inserted     INTEGER,
    rows_updated      INTEGER,
    rows_unchanged    INTEGER,
    error_message     TEXT,
    PRIMARY KEY (pipeline_run_id, stage)
);