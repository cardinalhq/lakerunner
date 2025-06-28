-- 1751057788_initial.up.sql

--
-- types
--

CREATE TYPE signal_enum AS ENUM (
  'logs',
  'metrics',
  'traces'
);

CREATE TYPE action_enum AS ENUM (
  'compact',
  'rollup'
);
