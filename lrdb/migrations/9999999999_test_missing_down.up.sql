-- Test migration with missing down file
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
