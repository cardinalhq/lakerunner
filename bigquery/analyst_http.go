package bigquery

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
)

type httpErr struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *AnalystServer) RegisterHTTPRoutes(mux *http.ServeMux) {
	// GET /datasets
	mux.HandleFunc("/datasets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, httpErr{"method not allowed"})
			return
		}
		out, err := s.GetBigQueryDataSets(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, httpErr{err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	// POST /graph  { "datasets": ["sales","ops"] }
	mux.HandleFunc("/graph", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, httpErr{"method not allowed"})
			return
		}
		var req struct {
			Datasets []string `json:"datasets"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, httpErr{err.Error()})
			return
		}
		out, err := s.GetTableGraph(r.Context(), req.Datasets)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, httpErr{err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	// POST /relevant-questions { "datasets": [...], "question": "...", "topK": 8 }
	mux.HandleFunc("/relevant-questions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, httpErr{"method not allowed"})
			return
		}
		var req struct {
			Datasets []string `json:"datasets"`
			Question string   `json:"question"`
			TopK     int      `json:"topK"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, httpErr{err.Error()})
			return
		}
		out, err := s.GetRelevantQuestions(r.Context(), req.Datasets, req.Question, req.TopK)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, httpErr{err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	// GET /distinct-values?dataset=...&table=...&column=...&limit=50
	mux.HandleFunc("/distinct-values", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			writeJSON(w, http.StatusMethodNotAllowed, httpErr{"method not allowed"})
			return
		}
		q := r.URL.Query()
		ds := q.Get("dataset")
		t := q.Get("table")
		col := q.Get("column")
		lim := 50
		if v := q.Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				lim = n
			}
		}
		out, err := s.GetUptoNDistinctStringValues(r.Context(), ds, t, col, lim)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, httpErr{err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	// POST /validate-sql { "question": "...", "sql": "..." }
	mux.HandleFunc("/validate-sql", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, httpErr{"method not allowed"})
			return
		}
		var req struct {
			Question string `json:"question"`
			SQL      string `json:"sql"`
			Dataset  string `json:"dataset"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, httpErr{err.Error()})
			return
		}
		out, err := s.ValidateQuestionSQL(r.Context(), req.Question, req.SQL, req.Dataset)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, httpErr{err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	// POST /execute-sql { "dataset": "sales", "sql": "..." }
	mux.HandleFunc("/execute-sql", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, httpErr{"method not allowed"})
			return
		}
		var req struct {
			Dataset string `json:"dataset"`
			SQL     string `json:"sql"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, httpErr{err.Error()})
			return
		}
		out, err := s.ExecuteSQL(r.Context(), req.Dataset, req.SQL)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, httpErr{err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, out)
	})
}

// Helper to start a server with routes registered.
func (s *AnalystServer) ServeHTTP(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	return srv.ListenAndServe()
}
