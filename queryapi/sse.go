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
)

func PushDownStream[T any](
	ctx context.Context,
	worker Worker,
	request PushDownRequest,
	parse func(typ string, data json.RawMessage) (T, bool, error),
) (<-chan T, error) {
	u := fmt.Sprintf("http://%s:%d/api/v1/pushDown", worker.IP, worker.Port)

	resp, err := mkRequest(ctx, request, u)
	if err != nil {
		return nil, err
	}

	out := make(chan T, 1024)

	go func() {
		defer close(out)
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
		return nil, fmt.Errorf("worker returned %s", resp.Status)
	}
	return resp, nil
}
