-- Test migration with missing down file
CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
-- Add another line to test no duplicate comments
-- Add a comment to test no duplicate success comments
