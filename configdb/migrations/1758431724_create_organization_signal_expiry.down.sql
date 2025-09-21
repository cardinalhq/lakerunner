-- Copyright (C) 2025 CardinalHQ, Inc
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as
-- published by the Free Software Foundation, version 3.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU Affero General Public License for more details.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

-- Drop functions
DROP FUNCTION IF EXISTS expire_published_by_ingest_cutoff(regclass, uuid, integer, integer);
DROP FUNCTION IF EXISTS find_org_partition(regclass, uuid);

-- Drop tables
DROP TABLE IF EXISTS expiry_run_tracking;
DROP TABLE IF EXISTS organization_signal_expiry;

