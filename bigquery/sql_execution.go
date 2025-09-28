package bigquery

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"google.golang.org/api/iterator"
	"strings"
	"time"
)

type EvidenceCheck struct {
	Name     string `json:"name"`
	Passed   bool   `json:"passed"`
	Severity string `json:"severity"` // "hard" or "soft"
	Info     string `json:"info,omitempty"`
}

type ParameterDTO struct {
	Name  string `json:"name"`
	Type  string `json:"type,omitempty"`
	Value string `json:"value,omitempty"` // redacted/sanitized string form
}

type SchemaFieldDTO struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Mode bool   `json:"mode"`
}

type TableSchemaSnapshot struct {
	TableID     string           `json:"table_id"` // project.dataset.table
	ETag        string           `json:"etag,omitempty"`
	LastModUnix int64            `json:"last_mod_unix"` // seconds
	Fields      []SchemaFieldDTO `json:"fields"`
}

type EvidenceBundle struct {
	QuestionID         string                `json:"question_id,omitempty"`
	SQLHash            string                `json:"sql_hash"`
	ReferencedTables   []string              `json:"referenced_tables"`
	SchemaSnapshot     []TableSchemaSnapshot `json:"schema_snapshot"`
	SchemaSnapshotHash string                `json:"schema_snapshot_hash"`

	Parameters     []ParameterDTO `json:"parameters,omitempty"`
	ExecutionMs    int64          `json:"execution_ms"`
	BytesProcessed int64          `json:"bytes_processed"`
	BytesBilled    int64          `json:"bytes_billed"`
	RowCount       int            `json:"row_count"`

	// BigQuery plan as generic rows (from QueryStatistics or EXPLAIN fallback)
	QueryPlan []map[string]any `json:"query_plan,omitempty"`

	// Optional CTE lineage retained for forward-compat; may be empty in this path.
	Lineage CTELineage `json:"lineage"`

	Checks []EvidenceCheck `json:"checks"`
}

type ResultSet struct {
	Rows     []map[string]any `json:"rows"`
	Evidence EvidenceBundle   `json:"evidence"`
	ErrorMsg string           `json:"error_msg,omitempty"`
}

// ExecuteSQL runs a SELECT/WITH query with default dataset, gathers rows (sample),
// and returns a ResultSet with a hardened EvidenceBundle. If any required check
// fails (or is skipped), the function refuses the answer and returns ErrorMsg.
func (s *AnalystServer) ExecuteSQL(ctx context.Context, dataset, sql string) (*ResultSet, error) {
	rs := &ResultSet{}
	checks := make([]EvidenceCheck, 0, 8)
	fail := func(name, info string) {
		checks = append(checks, EvidenceCheck{Name: name, Passed: false, Severity: "hard", Info: info})
	}
	pass := func(name, info string) {
		checks = append(checks, EvidenceCheck{Name: name, Passed: true, Severity: "hard", Info: info})
	}

	// 0) Safety
	if err := rejectNonSelect(sql); err != nil {
		fail("safety_gate", err.Error())
		rs.ErrorMsg = err.Error()
		rs.Evidence.Checks = checks
		return rs, nil
	}
	pass("safety_gate", "query is SELECT/WITH")

	// 1) Compute SQL hash (before any rewriting)
	sqlHash := hashSQL(sql)

	// 2) Optional: dry run for quick compile & rough bytes
	dryBytes, dryErr := s.dryRunBytes(ctx, dataset, sql)
	if dryErr != nil {
		fail("dry_run_compile", dryErr.Error())
		rs.ErrorMsg = fmt.Sprintf("dry-run failed: %v", dryErr)
		rs.Evidence.Checks = checks
		return rs, nil
	}
	pass("dry_run_compile", fmt.Sprintf("estimated bytes processed: %d", dryBytes))

	// 3) Actual execution (with default dataset)
	q := s.BQ.Query(sql)
	q.DefaultProjectID = s.ProjectID
	if dataset != "" {
		q.DefaultDatasetID = dataset
	}
	q.DisableQueryCache = true

	startWall := time.Now()
	job, err := q.Run(ctx)
	if err != nil {
		fail("job_run", err.Error())
		rs.ErrorMsg = fmt.Sprintf("job run failed: %v", err)
		rs.Evidence.Checks = checks
		return rs, nil
	}

	// Wait for completion and get stats
	st, err := job.Wait(ctx)
	if err != nil {
		fail("job_wait", err.Error())
		rs.ErrorMsg = fmt.Sprintf("job wait failed: %v", err)
		rs.Evidence.Checks = checks
		return rs, nil
	}
	if st.Err() != nil {
		fail("job_status", st.Err().Error())
		rs.ErrorMsg = st.Err().Error()
		rs.Evidence.Checks = checks
		return rs, nil
	}

	execMs := time.Since(startWall).Milliseconds()

	// Stats & referenced tables
	var bytesProcessed, bytesBilled int64
	var refs []string
	var planRows []map[string]any

	if st.Statistics != nil {
		bytesProcessed = st.Statistics.TotalBytesProcessed

		if qs, ok := st.Statistics.Details.(*bq.QueryStatistics); ok && qs != nil {
			bytesBilled = qs.TotalBytesBilled

			// Referenced tables
			for _, t := range qs.ReferencedTables {
				refs = append(refs, fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID))
			}
			refs = uniqueStrings(refs)

			// Query plan from job stats if available; else fallback to EXPLAIN.
			if len(qs.QueryPlan) > 0 {
				planRows = planRowsFromQueryStats(qs)
				pass("query_plan", "plan from job statistics")
			} else {
				// Try EXPLAIN fallback
				if rows, err := s.ExplainSQL(ctx, sql); err == nil && len(rows) > 0 {
					planRows = rows
					pass("query_plan", "plan from EXPLAIN fallback")
				} else {
					fail("query_plan", "no plan available from job statistics or EXPLAIN")
				}
			}
		}
	}

	// 4) Schema snapshot for referenced tables
	snapshots, snapHash, snapErr := s.schemaSnapshot(ctx, refs)
	if snapErr != nil {
		fail("schema_snapshot", snapErr.Error())
	} else {
		pass("schema_snapshot", fmt.Sprintf("tables=%d", len(snapshots)))
	}

	// 5) Read a sample of rows (up to N)
	iter, err := job.Read(ctx)
	if err != nil {
		fail("read_results", err.Error())
		rs.ErrorMsg = fmt.Sprintf("read results failed: %v", err)
		rs.Evidence.Checks = checks
		return rs, nil
	}
	const maxRows = 200
	rows, rowCnt, readErr := readSampleRows(iter, maxRows)
	if readErr != nil {
		fail("scan_rows", readErr.Error())
		rs.ErrorMsg = fmt.Sprintf("scan rows failed: %v", readErr)
		rs.Evidence.Checks = checks
		return rs, nil
	}
	pass("scan_rows", fmt.Sprintf("row_count=%d", rowCnt))

	// Hard-stop if any required check failed
	for _, c := range checks {
		if !c.Passed && c.Severity == "hard" {
			rs.ErrorMsg = "One or more validation checks failed; refusing to return data."
			rs.Evidence = EvidenceBundle{
				SQLHash:            sqlHash,
				ReferencedTables:   refs,
				SchemaSnapshot:     snapshots,
				SchemaSnapshotHash: snapHash,
				Parameters:         nil,
				ExecutionMs:        execMs,
				BytesProcessed:     bytesProcessed,
				BytesBilled:        bytesBilled,
				RowCount:           0,
				QueryPlan:          planRows,
				Lineage:            CTELineage{}, // not used in this path
				Checks:             checks,
			}
			return rs, nil
		}
	}

	// OK: return rows with evidence
	rs.Rows = rows
	rs.Evidence = EvidenceBundle{
		SQLHash:            sqlHash,
		ReferencedTables:   refs,
		SchemaSnapshot:     snapshots,
		SchemaSnapshotHash: snapHash,
		Parameters:         nil, // wire in if you support parameterized queries
		ExecutionMs:        execMs,
		BytesProcessed:     bytesProcessed,
		BytesBilled:        bytesBilled,
		RowCount:           rowCnt,
		QueryPlan:          planRows,
		Lineage:            CTELineage{},
		Checks:             checks,
	}
	return rs, nil
}

// --- helpers ---

func (s *AnalystServer) dryRunBytes(ctx context.Context, dataset, sql string) (int64, error) {
	q := s.BQ.Query(sql)
	q.DefaultProjectID = s.ProjectID
	if dataset != "" {
		q.DefaultDatasetID = dataset
	}
	q.DryRun = true
	q.DisableQueryCache = true
	job, err := q.Run(ctx)
	if err != nil {
		return 0, err
	}
	st, err := job.Status(ctx)
	if err != nil {
		return 0, err
	}
	if st.Err() != nil {
		return 0, st.Err()
	}
	if st.Statistics == nil {
		return 0, nil
	}
	return st.Statistics.TotalBytesProcessed, nil
}

func planRowsFromQueryStats(qs *bq.QueryStatistics) []map[string]any {
	// qs.QueryPlan is driver structs; convert to JSON-ish maps that are easy to render.
	out := make([]map[string]any, 0, len(qs.QueryPlan))
	for _, st := range qs.QueryPlan {
		row := map[string]any{
			"name":                 st.Name,
			"id":                   st.ID,
			"start_time":           st.StartTime.UnixMilli(),
			"end_time":             st.EndTime.UnixMilli(),
			"parallel_inputs":      st.ParallelInputs,
			"completed_parallel":   st.CompletedParallelInputs,
			"wait_ratio_avg":       st.WaitRatioAvg,
			"read_ratio_avg":       st.ReadRatioAvg,
			"compute_ratio_avg":    st.ComputeRatioAvg,
			"write_ratio_avg":      st.WriteRatioAvg,
			"records_read":         st.RecordsRead,
			"records_written":      st.RecordsWritten,
			"shuffle_output_bytes": st.ShuffleOutputBytes,
		}
		// Steps (if available)
		var steps []map[string]any
		for _, step := range st.Steps {
			m := map[string]any{"kind": step.Kind}
			if len(step.Substeps) > 0 {
				m["substeps"] = step.Substeps
			}
			steps = append(steps, m)
		}
		if len(steps) > 0 {
			row["steps"] = steps
		}
		out = append(out, row)
	}
	return out
}

func (s *AnalystServer) schemaSnapshot(ctx context.Context, refs []string) ([]TableSchemaSnapshot, string, error) {
	if len(refs) == 0 {
		return nil, "", fmt.Errorf("no referenced tables")
	}
	snaps := make([]TableSchemaSnapshot, 0, len(refs))
	for _, fq := range refs {
		// fq = project.dataset.table
		p, d, t := splitFQTable(fq)
		if p == "" || d == "" || t == "" {
			return nil, "", fmt.Errorf("bad table id: %q", fq)
		}
		md, err := s.BQ.DatasetInProject(p, d).Table(t).Metadata(ctx)
		if err != nil {
			return nil, "", fmt.Errorf("schema fetch failed for %s: %w", fq, err)
		}
		fields := make([]SchemaFieldDTO, 0, len(md.Schema))
		for _, f := range md.Schema {
			fields = append(fields, SchemaFieldDTO{
				Name: f.Name,
				Type: string(f.Type),
				Mode: f.Repeated,
			})
		}
		snaps = append(snaps, TableSchemaSnapshot{
			TableID:     fq,
			ETag:        md.ETag,
			LastModUnix: md.LastModifiedTime.Unix(),
			Fields:      fields,
		})
	}
	// hash the snapshot deterministically
	js, _ := json.Marshal(snaps)
	sum := sha256.Sum256(js)
	return snaps, hex.EncodeToString(sum[:]), nil
}

func readSampleRows(it *bq.RowIterator, max int) ([]map[string]any, int, error) {
	schema := it.Schema
	rows := make([]map[string]any, 0, max)
	count := 0
	for count < max {
		var vals []bq.Value
		err := it.Next(&vals)
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, 0, err
		}
		rows = append(rows, rowToMap(schema, vals))
		count++
	}
	return rows, count, nil
}

func rowToMap(schema bq.Schema, vals []bq.Value) map[string]any {
	m := make(map[string]any, len(schema))
	for i, f := range schema {
		if i >= len(vals) {
			break
		}
		m[f.Name] = sanitizeValue(vals[i])
	}
	return m
}

func sanitizeValue(v any) any {
	switch t := v.(type) {
	case []byte:
		// represent bytes as hex (shortened if large)
		s := hex.EncodeToString(t)
		if len(s) > 128 {
			return s[:128] + "…"
		}
		return s
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case bq.NullInt64:
		if t.Valid {
			return t.Int64
		}
		return nil
	case bq.NullFloat64:
		if t.Valid {
			return t.Float64
		}
		return nil
	case bq.NullString:
		if t.Valid {
			return t.StringVal
		}
		return nil
	case bq.NullBool:
		if t.Valid {
			return t.Bool
		}
		return nil
	default:
		return t
	}
}

func hashSQL(sql string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(sql)))
	return hex.EncodeToString(sum[:])
}

func splitFQTable(fq string) (project, dataset, table string) {
	parts := strings.Split(fq, ".")
	if len(parts) == 3 {
		return parts[0], parts[1], parts[2]
	}
	if len(parts) == 2 {
		return "", parts[0], parts[1]
	}
	return "", "", ""
}
