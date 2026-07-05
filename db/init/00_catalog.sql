-- Backing store for the Iceberg REST catalog. The catalog is tiny but
-- write-contended (every table commit updates it) — its default embedded
-- SQLite store falls over as soon as two jobs commit at once, so it gets
-- a real database like everything else that matters.
CREATE USER iceberg WITH PASSWORD 'iceberg';
CREATE DATABASE iceberg OWNER iceberg;
