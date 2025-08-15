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

package debug

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"

	_ "modernc.org/sqlite" // Import SQLite driver
)

func GetSQLiteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sqlite",
		Short: "SQLite database management commands",
		Long:  `Commands for managing SQLite databases`,
		RunE:  runSqlite,
	}

	cmd.Flags().String("sqlite", "", "SQLite database filename")
	if err := cmd.MarkFlagRequired("sqlite"); err != nil {
		panic(err)
	}

	cmd.Flags().String("parquet", "", "Parquet filename")
	if err := cmd.MarkFlagRequired("parquet"); err != nil {
		panic(err)
	}

	return cmd
}

type CardinalHQFields struct {
	MetricType    string // _cardinalhq.metric_type
	Tid           int64  // _cardinalhq.tid
	TelemetryType string // _cardinalhq.telemetry_type
	CustomerID    string // _cardinalhq.customer_id
	Name          string // _cardinalhq.name
	Timestamp     int64  // _cardinalhq.timestamp
	CollectorID   string // _cardinalhq.collector_id
}

type RollupFields struct {
	Avg   float64 // rollup_avg
	Count float64 // rollup_count
	Max   float64 // rollup_max
	Min   float64 // rollup_min
	P25   float64 // rollup_p25
	P50   float64 // rollup_p50
	P75   float64 // rollup_p75
	P90   float64 // rollup_p90
	P95   float64 // rollup_p95
	P99   float64 // rollup_p99
	Sum   float64 // rollup_sum
}

type Item struct {
	Cardinal CardinalHQFields
	Rollup   RollupFields
	Sketch   []byte
	Other    map[string]string
}

func itemFromParquet(rec map[string]any) (Item, error) {
	metricType, ok := rec["_cardinalhq.metric_type"].(string)
	if !ok {
		return Item{}, fmt.Errorf("expected _cardinalhq.metric_type as string, got %T", rec["_cardinalhq.metric_type"])
	}

	tid, ok := rec["_cardinalhq.tid"].(int64)
	if !ok {
		return Item{}, fmt.Errorf("expected _cardinalhq.tid as int64, got %T", rec["_cardinalhq.tid"])
	}

	telemetryType, ok := rec["_cardinalhq.telemetry_type"].(string)
	if !ok {
		return Item{}, fmt.Errorf("expected _cardinalhq.telemetry_type as string, got %T", rec["_cardinalhq.telemetry_type"])
	}

	customerID, ok := rec["_cardinalhq.customer_id"].(string)
	if !ok {
		return Item{}, fmt.Errorf("expected _cardinalhq.customer_id as string, got %T", rec["_cardinalhq.customer_id"])
	}

	name, ok := rec["_cardinalhq.name"].(string)
	if !ok {
		return Item{}, fmt.Errorf("expected _cardinalhq.name as string, got %T", rec["_cardinalhq.name"])
	}

	timestamp, ok := rec["_cardinalhq.timestamp"].(int64)
	if !ok {
		return Item{}, fmt.Errorf("expected _cardinalhq.timestamp as int64, got %T", rec["_cardinalhq.timestamp"])
	}

	collectorID, ok := rec["_cardinalhq.collector_id"].(string)
	if !ok {
		return Item{}, fmt.Errorf("expected _cardinalhq.collector_id as string, got %T", rec["_cardinalhq.collector_id"])
	}

	rollupAvg, ok := rec["rollup_avg"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_avg as float64, got %T", rec["rollup_avg"])
	}

	rollupCount, ok := rec["rollup_count"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_count as float64, got %T", rec["rollup_count"])
	}

	rollupMax, ok := rec["rollup_max"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_max as float64, got %T", rec["rollup_max"])
	}

	rollupMin, ok := rec["rollup_min"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_min as float64, got %T", rec["rollup_min"])
	}

	rollupP25, ok := rec["rollup_p25"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_p25 as float64, got %T", rec["rollup_p25"])
	}

	rollupP50, ok := rec["rollup_p50"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_p50 as float64, got %T", rec["rollup_p50"])
	}

	rollupP75, ok := rec["rollup_p75"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_p75 as float64, got %T", rec["rollup_p75"])
	}

	rollupP90, ok := rec["rollup_p90"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_p90 as float64, got %T", rec["rollup_p90"])
	}

	rollupP95, ok := rec["rollup_p95"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_p95 as float64, got %T", rec["rollup_p95"])
	}

	rollupP99, ok := rec["rollup_p99"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_p99 as float64, got %T", rec["rollup_p99"])
	}

	rollupSum, ok := rec["rollup_sum"].(float64)
	if !ok {
		return Item{}, fmt.Errorf("expected rollup_sum as float64, got %T", rec["rollup_sum"])
	}

	var sketch []byte
	switch v := rec["sketch"].(type) {
	case []byte:
		sketch = v
	case string:
		sketch = []byte(v)
	default:
		return Item{}, fmt.Errorf("expected sketch as []byte or string, got %T", rec["sketch"])
	}

	tagsMap := make(map[string]string)
	for k, v := range rec {
		if strings.HasPrefix(k, "resource.") ||
			strings.HasPrefix(k, "metric.") ||
			strings.HasPrefix(k, "scope.") ||
			strings.HasPrefix(k, "datapoint.") {
			if v == nil {
				continue
			}
			if s, ok := v.(string); ok {
				tagsMap[k] = s
			}
		}
	}

	item := Item{
		Cardinal: CardinalHQFields{
			MetricType:    metricType,
			Tid:           tid,
			TelemetryType: telemetryType,
			CustomerID:    customerID,
			Name:          name,
			Timestamp:     timestamp,
			CollectorID:   collectorID,
		},
		Rollup: RollupFields{
			Avg:   rollupAvg,
			Count: rollupCount,
			Max:   rollupMax,
			Min:   rollupMin,
			P25:   rollupP25,
			P50:   rollupP50,
			P75:   rollupP75,
			P90:   rollupP90,
			P95:   rollupP95,
			P99:   rollupP99,
			Sum:   rollupSum,
		},
		Sketch: sketch,
		Other:  tagsMap,
	}

	return item, nil
}

func createTable(db *sql.DB) error {
	dropTableSQL := `DROP TABLE IF EXISTS items;`
	if _, err := db.Exec(dropTableSQL); err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}

	createTableSQL := `
CREATE TABLE IF NOT EXISTS items (
  "_cardinalhq.metric_type"    TEXT    NOT NULL,
  "_cardinalhq.tid"            BIGINT  NOT NULL,
  "_cardinalhq.telemetry_type" TEXT    NOT NULL,
  "_cardinalhq.customer_id"    TEXT    NOT NULL,
  "_cardinalhq.name"           TEXT    NOT NULL,
  "_cardinalhq.timestamp"      BIGINT  NOT NULL,
  "_cardinalhq.collector_id"   TEXT    NOT NULL,

  "rollup_avg"   DOUBLE    NOT NULL,
  "rollup_count" DOUBLE    NOT NULL,
  "rollup_max"   DOUBLE    NOT NULL,
  "rollup_min"   DOUBLE    NOT NULL,
  "rollup_p25"   DOUBLE    NOT NULL,
  "rollup_p50"   DOUBLE    NOT NULL,
  "rollup_p75"   DOUBLE    NOT NULL,
	"rollup_p90"   DOUBLE    NOT NULL,
  "rollup_p95"   DOUBLE    NOT NULL,
  "rollup_p99"   DOUBLE    NOT NULL,
  "rollup_sum"   DOUBLE    NOT NULL,

  "sketch" BLOB    NOT NULL,
  "tags"   TEXT    NOT NULL
);
`
	if _, err := db.Exec(createTableSQL); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	crateIndexSQL := `
CREATE INDEX IF NOT EXISTS idx_items_tid ON items("_cardinalhq.tid", "_cardinalhq.name", "_cardinalhq.timestamp");
`
	if _, err := db.Exec(crateIndexSQL); err != nil {
		return fmt.Errorf("creating index: %w", err)
	}

	return nil
}

const insertSQL = `
INSERT INTO items (
  "_cardinalhq.metric_type",
  "_cardinalhq.tid",
  "_cardinalhq.telemetry_type",
  "_cardinalhq.customer_id",
  "_cardinalhq.name",
  "_cardinalhq.timestamp",
  "_cardinalhq.collector_id",
  "rollup_avg",
  "rollup_count",
  "rollup_max",
  "rollup_min",
  "rollup_p25",
  "rollup_p50",
  "rollup_p75",
	"rollup_p90",
  "rollup_p95",
  "rollup_p99",
  "rollup_sum",
  "sketch",
  "tags"
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`

func insertItem(db *sql.DB, item Item) error {
	tagsJSON, err := json.Marshal(item.Other)
	if err != nil {
		return fmt.Errorf("failed to marshal tags for insert: %w", err)
	}

	_, err = db.Exec(insertSQL,
		item.Cardinal.MetricType,    // 1
		item.Cardinal.Tid,           // 2
		item.Cardinal.TelemetryType, // 3
		item.Cardinal.CustomerID,    // 4
		item.Cardinal.Name,          // 5
		item.Cardinal.Timestamp,     // 6
		item.Cardinal.CollectorID,   // 7
		item.Rollup.Avg,             // 8
		item.Rollup.Count,           // 9
		item.Rollup.Max,             //10
		item.Rollup.Min,             //11
		item.Rollup.P25,             //12
		item.Rollup.P50,             //13
		item.Rollup.P75,             //14
		item.Rollup.P90,             //15
		item.Rollup.P95,             //16
		item.Rollup.P99,             //17
		item.Rollup.Sum,             //18
		item.Sketch,                 //19
		string(tagsJSON),            //20
	)
	if err != nil {
		return fmt.Errorf("inserting item: %w", err)
	}
	return nil
}

func runSqlite(cmd *cobra.Command, args []string) error {
	parquetFilename, err := cmd.Flags().GetString("parquet")
	if err != nil {
		return fmt.Errorf("failed to get parquet flag: %w", err)
	}

	sqliteFilename, err := cmd.Flags().GetString("sqlite")
	if err != nil {
		return fmt.Errorf("failed to get sqlite flag: %w", err)
	}

	fh, err := filecrunch.LoadSchemaForFile(parquetFilename)
	if err != nil {
		slog.Error("Failed to load schema for file", slog.Any("error", err))
		return err
	}
	slog.Info("Loaded schema for file", slog.String("file", parquetFilename))

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return err
	}

	if err := createTable(db); err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	reader := parquet.NewReader(fh.File, fh.Schema)
	defer reader.Close()

	for {
		rec := map[string]any{}
		if err := reader.Read(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading parquet: %w", err)
		}

		item, err := itemFromParquet(rec)
		if err != nil {
			return err
		}

		if err := insertItem(db, item); err != nil {
			return err
		}
	}

	rows, err := db.Query("SELECT * FROM items;")
	if err != nil {
		return fmt.Errorf("querying items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			metricTypeIn    string
			tidIn           int64
			telemetryTypeIn string
			customerIDIn    string
			nameIn          string
			timestampIn     int64
			collectorIDIn   string
			rollupAvgIn     float64
			rollupCountIn   float64
			rollupMaxIn     float64
			rollupMinIn     float64
			rollupP25In     float64
			rollupP50In     float64
			rollupP75In     float64
			rollupP90In     float64
			rollupP95In     float64
			rollupP99In     float64
			rollupSumIn     float64
			sketchIn        []byte
			tagsIn          string
		)

		if err := rows.Scan(
			&metricTypeIn,
			&tidIn,
			&telemetryTypeIn,
			&customerIDIn,
			&nameIn,
			&timestampIn,
			&collectorIDIn,
			&rollupAvgIn,
			&rollupCountIn,
			&rollupMaxIn,
			&rollupMinIn,
			&rollupP25In,
			&rollupP50In,
			&rollupP75In,
			&rollupP90In,
			&rollupP95In,
			&rollupP99In,
			&rollupSumIn,
			&sketchIn,
			&tagsIn,
		); err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}

		item := Item{
			Cardinal: CardinalHQFields{
				MetricType:    metricTypeIn,
				Tid:           tidIn,
				TelemetryType: telemetryTypeIn,
				CustomerID:    customerIDIn,
				Name:          nameIn,
				Timestamp:     timestampIn,
				CollectorID:   collectorIDIn,
			},
			Rollup: RollupFields{
				Avg:   rollupAvgIn,
				Count: rollupCountIn,
				Max:   rollupMaxIn,
				Min:   rollupMinIn,
				P25:   rollupP25In,
				P50:   rollupP50In,
				P75:   rollupP75In,
				P90:   rollupP90In,
				P95:   rollupP95In,
				P99:   rollupP99In,
				Sum:   rollupSumIn,
			},
			Sketch: sketchIn,
			Other:  make(map[string]string),
		}

		if err := json.Unmarshal([]byte(tagsIn), &item.Other); err != nil {
			return fmt.Errorf("failed to unmarshal tags JSON: %w", err)
		}
	}

	// copy the SQLite database to a file
	if sqliteFilename != "" {
		_, err := db.Exec("VACUUM INTO ?", sqliteFilename)
		if err != nil {
			return fmt.Errorf("failed to vacuum into file %s: %w", sqliteFilename, err)
		}
		slog.Info("SQLite database vacuumed into file", slog.String("filename", sqliteFilename))
	}

	return nil
}
