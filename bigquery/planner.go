// joinplan.go
package bigquery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	bq "cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
)

type BQGraph struct {
	Nodes map[string]*Table  // key: "dataset.table"
	Edges map[string][]*Edge // out-edges keyed by "dataset.table"
}

type Table struct {
	ID      string            // "dataset.table"
	Columns map[string]string // col -> BigQuery data type (STRING, INT64, etc.)
}

type EdgeKind string

const (
	EdgeFK   EdgeKind = "FK"
	EdgeHeur EdgeKind = "HEURISTIC"
)

type Edge struct {
	From, To   string // table IDs ("dataset.table")
	Kind       EdgeKind
	Cols       [][2]string // pairs: (from_col, to_col)
	Confidence float64     // 0..1
	Constraint string      // constraint name if FK
	Note       string      // freeform (e.g., "name+value similarity")
}

// BuildGraph constructs a graph from FK metadata across all datasets in the project.
func BuildGraph(ctx context.Context, projectID string) (*BQGraph, error) {
	client, err := bq.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	g := &BQGraph{Nodes: map[string]*Table{}, Edges: map[string][]*Edge{}}

	// List all datasets in project
	it := client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if err := harvestDataset(ctx, client, projectID, ds.DatasetID, g); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// BuildGraphForDatasets confines harvesting to the given dataset IDs.
func BuildGraphForDatasets(ctx context.Context, projectID string, datasetIDs []string) (*BQGraph, error) {
	client, err := bq.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer func(client *bq.Client) {
		err := client.Close()
		if err != nil {
			slog.Error("bigquery client close", "error", err)
		}
	}(client)

	g := &BQGraph{Nodes: map[string]*Table{}, Edges: map[string][]*Edge{}}
	for _, ds := range datasetIDs {
		if err := harvestDataset(ctx, client, projectID, ds, g); err != nil {
			return nil, err
		}
	}
	return g, nil
}

func harvestDataset(ctx context.Context, client *bq.Client, proj, ds string, g *BQGraph) error {
	if err := loadColumns(ctx, client, proj, ds, g); err != nil {
		return fmt.Errorf("columns %s: %w", ds, err)
	}
	if err := loadFKEdges(ctx, client, proj, ds, g); err != nil {
		return fmt.Errorf("fk %s: %w", ds, err)
	}
	return nil
}

func loadColumns(ctx context.Context, c *bq.Client, proj, ds string, g *BQGraph) error {
	qText := fmt.Sprintf(
		"SELECT table_name, column_name, data_type "+
			"FROM %s.INFORMATION_SCHEMA.COLUMNS "+
			"ORDER BY table_name, ordinal_position",
		dsIdent(proj, ds),
	)

	it, err := c.Query(qText).Read(ctx)
	if err != nil {
		return err
	}

	for {
		var row []bq.Value
		if err := it.Next(&row); errors.Is(err, iterator.Done) {
			break
		} else if err != nil {
			return err
		}
		tableName := toStr(row[0])
		colName := toStr(row[1])
		dataType := toStr(row[2])

		tid := fmt.Sprintf("%s.%s", ds, tableName)
		t, ok := g.Nodes[tid]
		if !ok {
			t = &Table{ID: tid, Columns: map[string]string{}}
			g.Nodes[tid] = t
		}
		t.Columns[colName] = dataType
	}
	return nil
}

func loadFKEdges(ctx context.Context, c *bq.Client, proj, ds string, g *BQGraph) error {
	dsIS := dsIdent(proj, ds)
	qText := fmt.Sprintf(
		"WITH tc AS (\n"+
			"  SELECT constraint_name, table_name\n"+
			"  FROM   %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS\n"+
			"  WHERE  constraint_type = 'FOREIGN KEY'\n"+
			"),\n"+
			"kcu AS (\n"+
			"  SELECT constraint_name, table_name AS fk_table, column_name AS fk_col, ordinal_position\n"+
			"  FROM   %s.INFORMATION_SCHEMA.KEY_COLUMN_USAGE\n"+
			"),\n"+
			"ccu AS (\n"+
			"  SELECT constraint_name, table_name AS pk_table, column_name AS pk_col\n"+
			"  FROM   %s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE\n"+
			")\n"+
			"SELECT\n"+
			"  tc.constraint_name,\n"+
			"  kcu.fk_table,\n"+
			"  kcu.fk_col,\n"+
			"  ccu.pk_table,\n"+
			"  ccu.pk_col,\n"+
			"  kcu.ordinal_position\n"+
			"FROM tc\n"+
			"JOIN kcu USING (constraint_name)\n"+
			"JOIN ccu USING (constraint_name)\n"+
			"ORDER BY constraint_name, ordinal_position",
		dsIS, dsIS, dsIS,
	)

	it, err := c.Query(qText).Read(ctx)
	if err != nil {
		return err
	}

	type key struct{ C, F, P string }
	group := map[key][][2]string{}

	for {
		var row []bq.Value
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return err
		}
		constraint := toStr(row[0])
		fkTable := toStr(row[1])
		fkCol := toStr(row[2])
		pkTable := toStr(row[3])
		pkCol := toStr(row[4])

		k := key{
			C: constraint,
			F: fmt.Sprintf("%s.%s", ds, fkTable),
			P: fmt.Sprintf("%s.%s", ds, pkTable),
		}
		group[k] = append(group[k], [2]string{fkCol, pkCol})
	}

	for k, pairs := range group {
		e := &Edge{
			From:       k.F,
			To:         k.P,
			Kind:       EdgeFK,
			Cols:       pairs,
			Confidence: 1.0,
			Constraint: k.C,
			Note:       "declared FK",
		}
		g.Edges[e.From] = append(g.Edges[e.From], e)
	}
	return nil
}

func toStr(v bq.Value) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

type SimilarityScorer interface {
	ScoreColumns(ctx context.Context, a ColumnSample, b ColumnSample) (score float64, rationale string, err error)
}

type ColumnSample struct {
	TableID    string // "dataset.table"
	ColumnName string
	BQType     string
	Sample     []string
	ExtraNotes string
}

func AugmentGraphWithSamples(
	ctx context.Context,
	client *bq.Client,
	projectID string,
	g *BQGraph,
	scorer SimilarityScorer,
	sampleLimit int,
	minScore float64,
	maxPairsPerTable int,
) error {
	if sampleLimit <= 0 {
		sampleLimit = 10
	}
	if minScore <= 0 {
		minScore = 0.70
	}
	if maxPairsPerTable <= 0 {
		maxPairsPerTable = 50
	}

	// Cache samples: table -> col -> ColumnSample
	samples := map[string]map[string]ColumnSample{}

	getSample := func(ctx context.Context, tableID, col, typ string) (ColumnSample, error) {
		if samples[tableID] == nil {
			samples[tableID] = map[string]ColumnSample{}
		}
		if s, ok := samples[tableID][col]; ok {
			return s, nil
		}
		ds, tbl := splitTableID(tableID)
		if ds == "" || tbl == "" {
			return ColumnSample{}, fmt.Errorf("bad tableID: %s", tableID)
		}

		rows, err := selectSamples(ctx, client, projectID, ds, tbl, col, sampleLimit)
		if err != nil {
			return ColumnSample{}, err
		}

		seen := map[string]struct{}{}
		out := make([]string, 0, len(rows))
		for _, v := range rows {
			if _, ok := seen[v]; !ok {
				seen[v] = struct{}{}
				out = append(out, v)
			}
		}
		s := ColumnSample{
			TableID:    tableID,
			ColumnName: col,
			BQType:     typ,
			Sample:     out,
			ExtraNotes: deriveNotes(col, typ),
		}
		samples[tableID][col] = s
		return s, nil
	}

	for aID, a := range g.Nodes {
		targets := rankTargets(aID, g)

		pairsAdded := 0
		for _, bID := range targets {
			if aID == bID {
				continue
			}

			for acol, atyp := range a.Columns {
				if !maybeIdentifier(acol) && !maybeJoinableType(atyp) {
					continue
				}
				for bcol, btyp := range g.Nodes[bID].Columns {
					if !typeCompatible(atyp, btyp) {
						continue
					}
					nameSim := nameSimilarity(acol, bcol, baseName(bID))
					if nameSim < 0.35 && !(strings.HasSuffix(strings.ToLower(acol), "_id") && strings.ToLower(bcol) == "id") {
						continue
					}

					as, err := getSample(ctx, aID, acol, atyp)
					if err != nil {
						continue
					}
					bs, err := getSample(ctx, bID, bcol, btyp)
					if err != nil {
						continue
					}

					score, rationale, err := scorer.ScoreColumns(ctx, as, bs)
					if err != nil {
						continue
					}
					combined := (0.3 * nameSim) + (0.7 * score)
					if combined >= minScore {
						e := &Edge{
							From:       aID,
							To:         bID,
							Kind:       EdgeHeur,
							Cols:       [][2]string{{acol, bcol}},
							Confidence: combined,
							Note:       "name+value similarity; " + rationale,
						}
						g.Edges[aID] = append(g.Edges[aID], e)
						pairsAdded++
						if pairsAdded >= maxPairsPerTable {
							break
						}
					}
				}
				if pairsAdded >= maxPairsPerTable {
					break
				}
			}
			if pairsAdded >= maxPairsPerTable {
				break
			}
		}
	}
	return nil
}

// selectSamples: SELECT col FROM `proj.ds.tbl` WHERE col IS NOT NULL LIMIT N
func selectSamples(ctx context.Context, client *bq.Client, proj, ds, tbl, col string, limit int) ([]string, error) {
	q := client.Query(fmt.Sprintf(
		"SELECT %s FROM `%s.%s.%s` WHERE %s IS NOT NULL LIMIT %d",
		bqIdent(col), proj, ds, tbl, bqIdent(col), limit,
	))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, err
	}

	out := []string{}
	for {
		var row []bq.Value
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}
		if len(row) > 0 && row[0] != nil {
			out = append(out, fmt.Sprintf("%v", row[0]))
		}
	}
	return out, nil
}

// ---------- Utilities ----------

func dsIdent(project, dataset string) string {
	return fmt.Sprintf("`%s.%s`", project, dataset)
}

func splitTableID(t string) (ds, tbl string) {
	parts := strings.Split(t, ".")
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

func baseName(tableID string) string {
	_, tbl := splitTableID(tableID)
	return strings.ToLower(tbl)
}

func bqIdent(s string) string {
	return fmt.Sprintf("`%s`", s)
}

func maybeIdentifier(col string) bool {
	l := strings.ToLower(col)
	return l == "id" || strings.HasSuffix(l, "_id") || strings.Contains(l, "key")
}

func maybeJoinableType(typ string) bool {
	t := strings.ToUpper(typ)
	return strings.Contains(t, "STRING") || strings.Contains(t, "INT") || strings.Contains(t, "NUMERIC")
}

func typeCompatible(a, b string) bool {
	au := strings.ToUpper(a)
	bu := strings.ToUpper(b)
	switch {
	case strings.Contains(au, "STRING") && strings.Contains(bu, "STRING"):
		return true
	case (strings.Contains(au, "INT") || strings.Contains(au, "NUMERIC")) &&
		(strings.Contains(bu, "INT") || strings.Contains(bu, "NUMERIC")):
		return true
	default:
		return false
	}
}

func nameSimilarity(aCol, bCol, bTable string) float64 {
	a := strings.ToLower(aCol)
	b := strings.ToLower(bCol)
	if a == b {
		return 1.0
	}
	if strings.HasSuffix(a, "_id") && b == "id" && strings.TrimSuffix(a, "_id") == bTable {
		return 0.9
	}
	ta := tokenize(a)
	tb := tokenize(b)
	return jaccard(ta, tb)
}

func tokenize(s string) []string {
	s = strings.ReplaceAll(s, "_", " ")
	fields := strings.Fields(s)
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	return fields
}

func jaccard(a, b []string) float64 {
	if len(a) == 0 && len(b) == 0 {
		return 1.0
	}
	set := map[string]int{}
	for _, x := range a {
		set[x] |= 1
	}
	for _, y := range b {
		set[y] |= 2
	}
	i, u := 0, 0
	for _, v := range set {
		if v == 1 || v == 2 {
			u++
		}
		if v == 3 {
			i++
			u++
		}
	}
	if u == 0 {
		return 0
	}
	return float64(i) / float64(u)
}

func deriveNotes(col, typ string) string {
	n := []string{}
	ut := strings.ToUpper(typ)
	if maybeIdentifier(col) {
		n = append(n, "id-like")
	}
	if strings.Contains(ut, "STRING") {
		n = append(n, "string")
	}
	if strings.Contains(ut, "INT") || strings.Contains(ut, "NUMERIC") {
		n = append(n, "numeric-ish")
	}
	if len(n) == 0 {
		return ""
	}
	return strings.Join(n, ", ")
}

func rankTargets(from string, g *BQGraph) []string {
	ds, _ := splitTableID(from)
	all := make([]string, 0, len(g.Nodes))
	for id := range g.Nodes {
		if id != from {
			all = append(all, id)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		di, _ := splitTableID(all[i])
		dj, _ := splitTableID(all[j])
		if di == dj && di == ds {
			return all[i] < all[j]
		}
		if di == ds && dj != ds {
			return true
		}
		if dj == ds && di != ds {
			return false
		}
		return all[i] < all[j]
	})
	return all
}
