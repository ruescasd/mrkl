-- psql -U postgres -f .\src\scripts\database.sql
CREATE DATABASE merkle_db;
CREATE USER "user" WITH PASSWORD 'user';
GRANT ALL PRIVILEGES ON DATABASE merkle_db TO "user";

-- Connect to the merkle_db database to set schema permissions
\c merkle_db

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO "user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "user";
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "user";
