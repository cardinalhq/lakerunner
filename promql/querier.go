package promql

import (
	"context"
	"fmt"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"
	"net/http"
	"sort"
	"time"
)

type QuerierService struct {
	mdb *lrdb.Store
}

func (q *QuerierService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	orgID := r.URL.Query().Get("orgId")
	if orgID == "" {
		http.Error(w, "missing orgId", http.StatusBadRequest)
		return
	}
	s := r.URL.Query().Get("s")
	e := r.URL.Query().Get("e")
	if s == "" || e == "" {
		http.Error(w, "missing s/e", http.StatusBadRequest)
		return
	}

	startTs, endTs, err := dateutils.ToStartEnd(s, e)
	if err != nil {
		http.Error(w, "invalid s/e: "+err.Error(), http.StatusBadRequest)
		return
	}
	if startTs >= endTs {
		http.Error(w, "start must be < end", http.StatusBadRequest)
		return
	}

	stepDuration := stepForQueryDuration(startTs, endTs)
	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		http.Error(w, "invalid orgId: "+err.Error(), http.StatusBadRequest)
		return
	}

	prom := r.URL.Query().Get("q")
	if prom == "" {
		http.Error(w, "missing query expression", http.StatusBadRequest)
		return
	}

	_, err = FromPromQL(prom)
	if err != nil {
		http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
		return
	}

	_, err = q.getSegmentInfos(r.Context(), startTs, endTs, stepDuration, orgUUID, orgID)
	if err != nil {
		http.Error(w, "error fetching segment infos: "+err.Error(), http.StatusInternalServerError)
		return
	}
}

func (q *QuerierService) getSegmentInfos(ctx context.Context, startTs int64, endTs int64, stepDuration time.Duration, orgUUID uuid.UUID, orgID string) ([]SegmentInfo, error) {
	var allSegments []SegmentInfo
	diHours := dateIntHoursRange(startTs, endTs, time.UTC)
	for _, dih := range diHours {
		rows, err := q.mdb.ListSegmentsForQuery(ctx, lrdb.ListSegmentsForQueryParams{
			Int8range:      startTs,
			Int8range_2:    endTs,
			Dateint:        int32(dih.DateInt),
			FrequencyMs:    int32(stepDuration.Milliseconds()),
			OrganizationID: orgUUID,
		})
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			endHour := zeroFilledHour(time.UnixMilli(row.EndTs).UTC().Hour())
			allSegments = append(allSegments, SegmentInfo{
				DateInt:     dih.DateInt,
				Hour:        endHour,
				SegmentID:   fmt.Sprintf("tbl_%d", row.SegmentID),
				StartTs:     row.StartTs,
				EndTs:       row.EndTs,
				ExprID:      "",
				Dataset:     "metrics",
				BucketName:  "bucket",
				CustomerID:  orgID,
				CollectorID: "collectorId",
				Frequency:   stepDuration.Milliseconds(),
			})
		}
	}
	return allSegments, nil
}

func stepForQueryDuration(startMs, endMs int64) time.Duration {
	oneHourish := int64(1 * 65 * 60 * 1000)
	twelveHours := int64(12 * 60 * 60 * 1000)
	oneDay := int64(24 * 60 * 60 * 1000)
	threeDays := int64(3 * 24 * 60 * 60 * 1000)

	span := endMs - startMs
	switch {
	case span <= oneHourish:
		return 10 * time.Second
	case span <= twelveHours:
		return time.Minute
	case span <= oneDay:
		return 5 * time.Minute
	case span <= threeDays:
		return 20 * time.Minute
	default:
		return time.Hour
	}
}

type DateIntHours struct {
	DateInt int      // e.g. 20250814
	Hours   []string // "00".."23"
}

// zeroFilledHour returns "00".."23".
func zeroFilledHour(h int) string {
	return fmt.Sprintf("%02d", h)
}

// toDateInt converts a time to YYYYMMDD (UTC unless you pass a different loc).
func toDateInt(t time.Time) int {
	y, m, d := t.Date()
	return y*10000 + int(m)*100 + d
}

// dateIntHoursRange: given a time range produces the date int hours, in reverse order of date ints, and hours are reverse as well.
func dateIntHoursRange(startMs, endMs int64, loc *time.Location) []DateIntHours {
	if loc == nil {
		loc = time.UTC
	}
	start := time.UnixMilli(startMs).In(loc).Truncate(time.Hour)
	end := time.UnixMilli(endMs).In(loc).Truncate(time.Hour)

	var out []DateIntHours

	var curDateInt int
	hoursSet := make(map[string]struct{})
	flush := func() {
		if curDateInt == 0 || len(hoursSet) == 0 {
			return
		}
		hh := make([]string, 0, len(hoursSet))
		for h := range hoursSet {
			hh = append(hh, h)
		}
		// reverse hour order "23".."00"
		sort.Sort(sort.Reverse(sort.StringSlice(hh)))
		out = append(out, DateIntHours{DateInt: curDateInt, Hours: hh})
		hoursSet = make(map[string]struct{})
	}

	for t := start; !t.After(end.Add(time.Hour)); t = t.Add(time.Hour) {
		di := toDateInt(t)
		if curDateInt != 0 && di != curDateInt {
			flush()
		}
		curDateInt = di
		hoursSet[zeroFilledHour(t.Hour())] = struct{}{}
	}
	flush()

	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}

	return out
}
