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

// BuildGraphForDatasets confines harvesting to the given dataset IDs.
func BuildGraphForDatasets(ctx context.Context, projectID string, datasetIDs []string) (*BQGraph, error) {
	client, err := bq.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer func(client *bq.Client) {
		if cerr := client.Close(); cerr != nil {
			slog.Error("bigquery client close", "error", cerr)
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
		slog.Info("Discovered column", "table", tableName, "column", colName, "type", dataType)

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

func AugmentGraphWithEdgesBetweenSimilarColumns(g *BQGraph, maxPairsPerTable int) error {

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
					nameSim := nameSimilarity(acol, bcol, baseName(bID))
					if nameSim < 0.75 && !(strings.HasSuffix(strings.ToLower(acol), "_id") && strings.ToLower(bcol) == "id") {
						continue
					}

					compatible := typeCompatible(atyp, btyp)
					if !compatible {
						if isStringType(atyp) && isNumericType(btyp) {
							compatible = true
						} else if isStringType(btyp) && isNumericType(atyp) {
							compatible = true
						}
					}
					if !compatible {
						continue
					}

					e := &Edge{
						From:       aID,
						To:         bID,
						Kind:       EdgeHeur,
						Cols:       [][2]string{{acol, bcol}},
						Confidence: nameSim,
						Note:       "name similarity",
					}
					g.Edges[aID] = append(g.Edges[aID], e)
					slog.Info("Adding heuristic edge", "from", fmt.Sprintf("%s.%s", bqIdent(aID), acol), "to", fmt.Sprintf("%s.%s", bqIdent(bID), bcol))
					pairsAdded++
					if pairsAdded >= maxPairsPerTable {
						break
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
	return strings.Contains(t, "STRING") || strings.Contains(t, "INT") || strings.Contains(t, "NUMERIC") || strings.Contains(t, "BIGNUMERIC")
}

func isStringType(t string) bool {
	u := strings.ToUpper(t)
	return strings.Contains(u, "STRING")
}

func isNumericType(t string) bool {
	u := strings.ToUpper(t)
	return strings.Contains(u, "INT") || strings.Contains(u, "NUMERIC") || strings.Contains(u, "BIGNUMERIC")
}

func typeCompatible(a, b string) bool {
	au := strings.ToUpper(a)
	bu := strings.ToUpper(b)
	switch {
	case strings.Contains(au, "STRING") && strings.Contains(bu, "STRING"):
		return true
	case isNumericType(au) && isNumericType(bu):
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

	// underscore-insensitive equality
	ar := strings.ReplaceAll(a, "_", "")
	br := strings.ReplaceAll(b, "_", "")
	if ar == br {
		return 0.95
	}

	// table-name + id forms, e.g. portfolio_id ↔ portfolioid, security_id ↔ securityid
	bt := strings.ReplaceAll(strings.ToLower(bTable), "_", "")
	if strings.HasSuffix(a, "_id") && strings.TrimSuffix(a, "_id") == bt && (b == "id" || b == bt+"id" || br == bt+"id") {
		return 0.9
	}
	if strings.HasSuffix(b, "_id") && strings.TrimSuffix(b, "_id") == bt && (a == "id" || a == bt+"id" || ar == bt+"id") {
		return 0.9
	}
	if (a == bt+"id" && (b == "id" || b == bt+"_id")) || (b == bt+"id" && (a == "id" || a == bt+"_id")) {
		return 0.9
	}

	// fallback jaccard on underscore-split tokens
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
	if isNumericType(ut) {
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
