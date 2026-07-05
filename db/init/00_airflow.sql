-- Airflow's metadata database, colocated on the same Postgres instance to
-- save RAM. In production Airflow would get its own database server.
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
