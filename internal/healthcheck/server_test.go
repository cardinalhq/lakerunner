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

package healthcheck

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestStatus_String(t *testing.T) {
	tests := []struct {
		status Status
		want   string
	}{
		{StatusStarting, "starting"},
		{StatusHealthy, "healthy"},
		{StatusUnhealthy, "unhealthy"},
		{Status(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("Status.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetConfigFromEnv(t *testing.T) {
	// Save original environment
	originalPort := os.Getenv("HEALTH_CHECK_PORT")
	os.Unsetenv("HEALTH_CHECK_PORT")
	defer func() {
		os.Unsetenv("HEALTH_CHECK_PORT")
		if originalPort != "" {
			os.Setenv("HEALTH_CHECK_PORT", originalPort)
		}
	}()

	// Test defaults
	config := GetConfigFromEnv()
	if config.Port != 8090 {
		t.Errorf("Expected Port to default to 8090, got %d", config.Port)
	}

	// Test custom values
	os.Setenv("HEALTH_CHECK_PORT", "9090")

	config = GetConfigFromEnv()
	if config.Port != 9090 {
		t.Errorf("Expected Port to be 9090, got %d", config.Port)
	}

	// Test invalid port
	os.Setenv("HEALTH_CHECK_PORT", "invalid")
	config = GetConfigFromEnv()
	if config.Port != 8090 {
		t.Errorf("Expected Port to fallback to 8090 for invalid value, got %d", config.Port)
	}
}

func TestNewServer(t *testing.T) {
	// Test with empty config
	server := NewServer(Config{})
	if server.port != 8090 {
		t.Errorf("Expected default port 8090, got %d", server.port)
	}

	// Test with custom config
	config := Config{
		Port: 9090,
	}
	server = NewServer(config)
	if server.port != 9090 {
		t.Errorf("Expected port 9090, got %d", server.port)
	}
}

func TestServer_SetGetStatus(t *testing.T) {
	server := NewServer(Config{})

	// Test initial status
	status := server.GetStatus()
	if status != StatusStarting {
		t.Errorf("Expected initial status to be StatusStarting, got %v", status)
	}

	// Test setting status
	server.SetStatus(StatusHealthy)
	status = server.GetStatus()
	if status != StatusHealthy {
		t.Errorf("Expected status to be StatusHealthy, got %v", status)
	}

	server.SetStatus(StatusUnhealthy)
	status = server.GetStatus()
	if status != StatusUnhealthy {
		t.Errorf("Expected status to be StatusUnhealthy, got %v", status)
	}
}

func TestHealthEndpoints(t *testing.T) {
	config := Config{
		Port: 8090,
	}
	server := NewServer(config)

	// Start server in background
	ctx := t.Context()

	go func() {
		_ = server.Start(ctx)
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// We can't easily test the actual HTTP endpoints without knowing the port
	// So we'll test the handler methods directly

	tests := []struct {
		name            string
		status          Status
		endpoint        string
		expectedStatus  int
		expectedHealthy bool
	}{
		{"healthz starting", StatusStarting, "/healthz", http.StatusServiceUnavailable, false},
		{"healthz healthy", StatusHealthy, "/healthz", http.StatusOK, true},
		{"healthz unhealthy", StatusUnhealthy, "/healthz", http.StatusServiceUnavailable, false},
		{"readyz starting", StatusStarting, "/readyz", http.StatusServiceUnavailable, false},
		{"readyz healthy", StatusHealthy, "/readyz", http.StatusOK, true},
		{"readyz unhealthy", StatusUnhealthy, "/readyz", http.StatusServiceUnavailable, false},
		{"livez starting", StatusStarting, "/livez", http.StatusOK, true},
		{"livez healthy", StatusHealthy, "/livez", http.StatusOK, true},
		{"livez unhealthy", StatusUnhealthy, "/livez", http.StatusServiceUnavailable, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server.SetStatus(tt.status)

			// Create a mock request
			req, err := http.NewRequest("GET", tt.endpoint, nil)
			if err != nil {
				t.Fatal(err)
			}

			// Create a response recorder
			rr := &mockResponseWriter{
				header: make(http.Header),
				body:   make([]byte, 0),
			}

			// Call the appropriate handler
			switch tt.endpoint {
			case "/healthz":
				server.healthzHandler(rr, req)
			case "/readyz":
				server.readyzHandler(rr, req)
			case "/livez":
				server.livezHandler(rr, req)
			}

			// Check status code
			if rr.statusCode != tt.expectedStatus {
				t.Errorf("Expected status %d, got %d", tt.expectedStatus, rr.statusCode)
			}

			// Check that response is valid JSON
			var response Response
			if err := json.Unmarshal(rr.body, &response); err != nil {
				t.Errorf("Invalid JSON response: %v", err)
			}

			// Check response fields
			if response.Healthy != tt.expectedHealthy {
				t.Errorf("Expected healthy %v, got %v", tt.expectedHealthy, response.Healthy)
			}
		})
	}
}

// mockResponseWriter implements http.ResponseWriter for testing
type mockResponseWriter struct {
	header     http.Header
	body       []byte
	statusCode int
}

func (m *mockResponseWriter) Header() http.Header {
	return m.header
}

func (m *mockResponseWriter) Write(data []byte) (int, error) {
	m.body = append(m.body, data...)
	return len(data), nil
}

func (m *mockResponseWriter) WriteHeader(statusCode int) {
	m.statusCode = statusCode
}
