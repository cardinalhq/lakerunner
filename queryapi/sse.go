// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package queryapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
)

type PushDownHTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *PushDownHTTPError) Error() string {
	if strings.TrimSpace(e.Body) == "" {
		return fmt.Sprintf("worker returned %s", e.Status)
	}
	return fmt.Sprintf("worker returned %s: %s", e.Status, e.Body)
}

func PushDownStream[T any](
	ctx context.Context,
	worker Worker,
	request PushDownRequest,
	parse func(typ string, data json.RawMessage) (T, bool, error),
) (<-chan T, error) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryapi")
	ctx, pushdownSpan := tracer.Start(ctx, "query.api.pushdown_stream")
	pushdownSpan.SetAttributes(
		attribute.String("worker_ip", worker.IP),
		attribute.Int("worker_port", worker.Port),
		attribute.Int("segment_count", len(request.Segments)),
	)

	u := fmt.Sprintf("http://%s:%d/api/v1/pushDown", worker.IP, worker.Port)

	resp, err := mkRequest(ctx, request, u)
	if err != nil {
		pushdownSpan.RecordError(err)
		pushdownSpan.SetStatus(codes.Error, "worker request failed")
		pushdownSpan.End()
		return nil, err
	}

	out := make(chan T, 1024)

	go func() {
		defer close(out)
		resultsEmitted := 0
		defer func() {
			pushdownSpan.SetAttributes(attribute.Int("results_emitted", resultsEmitted))
			pushdownSpan.End()
		}()
		defer func() {
			if cerr := resp.Body.Close(); cerr != nil {
				slog.Error("failed to close response body", "err", cerr)
			}
		}()

		type envelope struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}

		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

		var dataBuf strings.Builder

		flush := func() bool {
			if dataBuf.Len() == 0 {
				return true
			}
			payload := dataBuf.String()
			dataBuf.Reset()

			var env envelope
			if err := json.Unmarshal([]byte(payload), &env); err != nil {
				slog.Error("SSE json unmarshal failed", "err", err)
				return false
			}

			switch env.Type {
			case "done":
				return false
			default:
				// Let the caller decide what to do with this event type.
				val, emit, perr := parse(env.Type, env.Data)
				if perr != nil {
					slog.Error("parse handler failed", "err", perr, "type", env.Type)
					return false
				}
				if emit {
					select {
					case <-ctx.Done():
						return false
					case out <- val:
						resultsEmitted++
					}
				}
				return true
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if !sc.Scan() {
				_ = flush()
				if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
					slog.Error("SSE scanner error", "err", err)
				}
				return
			}

			line := sc.Text()
			if line == "" {
				// Blank line = event boundary
				if !flush() {
					return
				}
				continue
			}
			if strings.HasPrefix(line, "data:") {
				chunk := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if dataBuf.Len() > 0 {
					dataBuf.WriteByte('\n')
				}
				dataBuf.WriteString(chunk)
			}
		}
	}()

	return out, nil
}

func mkRequest(ctx context.Context, request PushDownRequest, u string) (*http.Response, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal PushDownRequest: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	// Propagate trace context to the worker
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("worker request failed: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer func() {
			if cerr := resp.Body.Close(); cerr != nil {
				slog.Error("failed to close response body", "err", cerr)
			}
		}()
		// Read the error response body for debugging
		body, readErr := io.ReadAll(resp.Body)
		errBody := ""
		if readErr == nil && len(body) > 0 {
			errBody = string(body)
			slog.Error("worker returned error", "status", resp.Status, "body", errBody)
		}
		return nil, &PushDownHTTPError{
			StatusCode: resp.StatusCode,
			Status:     resp.Status,
			Body:       errBody,
		}
	}
	return resp, nil
}
