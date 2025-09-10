// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package config

var (
	KafkaTopics = []string{
		"lakerunner.objstore.ingest.logs",
		"lakerunner.objstore.ingest.metrics",
		"lakerunner.objstore.ingest.traces",
		"lakerunner.segments.logs.compact",
		"lakerunner.segments.metrics.compact",
		"lakerunner.segments.metrics.rollup",
		"lakerunner.segments.traces.compact",
	}
)
