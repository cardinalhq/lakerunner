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

// LegacyEvent represents a SSE event in the legacy API format.
type LegacyEvent struct {
	ID      string        `json:"id"`
	Type    string        `json:"type"`
	Message LegacyMessage `json:"message"`
}

// LegacyMessage represents the message payload in a legacy SSE event.
type LegacyMessage struct {
	Timestamp int64          `json:"timestamp"`
	Value     float64        `json:"value"`
	Tags      map[string]any `json:"tags"`
}

// LegacyDoneEvent represents the final "done" event in a legacy SSE stream.
type LegacyDoneEvent struct {
	ID      string            `json:"id"`
	Type    string            `json:"type"`
	Message map[string]string `json:"message"`
}

// ToLegacySSEEvent converts a log result to a legacy SSE event format.
func ToLegacySSEEvent(
	queryID string,
	segmentID int64,
	timestamp int64,
	value float64,
	tags map[string]any,
	denormalizer *LabelDenormalizer,
) LegacyEvent {
	dottedTags := denormalizer.DenormalizeMap(segmentID, tags)

	return LegacyEvent{
		ID:   queryID,
		Type: "event",
		Message: LegacyMessage{
			Timestamp: timestamp,
			Value:     value,
			Tags:      dottedTags,
		},
	}
}

// NewLegacyDoneEvent creates a "done" event for the legacy SSE stream.
func NewLegacyDoneEvent(queryID string, status string) LegacyDoneEvent {
	return LegacyDoneEvent{
		ID:   queryID,
		Type: "done",
		Message: map[string]string{
			"status": status,
		},
	}
}
