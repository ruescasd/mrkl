psql postgres://postgres:admin@localhost:5432 -f .\src\scripts\database.sql
cargo run --bin setup -- --reset
cargo run --bin main --release