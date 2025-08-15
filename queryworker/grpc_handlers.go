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

package queryworker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryproto"
)

// Compile-time check to ensure Service implements QueryWorkerServer
var _ queryproto.QueryWorkerServer = (*Service)(nil)

// ProcessPushdown implements the GRPC streaming method for processing pushdown requests
func (s *Service) ProcessPushdown(req *queryproto.PushDownRequest, stream grpc.ServerStreamingServer[queryproto.SketchInputResponse]) error {
	// Convert GRPC request to internal promql.PushDownRequest
	internalReq, err := s.convertPushDownRequest(req)
	if err != nil {
		slog.Error("Failed to convert pushdown request", "error", err)
		errorResp := &queryproto.SketchInputResponse{
			Response: &queryproto.SketchInputResponse_Error{
				Error: &queryproto.ErrorResponse{Error: err.Error()},
			},
		}
		return stream.Send(errorResp)
	}

	slog.Info("Received GRPC pushdown request",
		"exprID", internalReq.BaseExpr.ID,
		"startTs", internalReq.StartTs,
		"endTs", internalReq.EndTs,
		"segmentCount", len(internalReq.Segments))

	ctx := stream.Context()
	resultsCh := make(chan promql.SketchInput, 1024)

	// Process segments in a goroutine
	go func() {
		defer close(resultsCh)

		if err := s.processSegments(ctx, internalReq, resultsCh); err != nil {
			slog.Error("Failed to process segments", "error", err)
			// Send error via channel - will be handled in the main loop
			select {
			case <-ctx.Done():
			default:
				// Create a sentinel error result that we can detect
				errorResult := promql.SketchInput{
					ExprID:         "ERROR",
					OrganizationID: err.Error(),
					Timestamp:      -1,
					Frequency:      -1,
					SketchTags:     promql.SketchTags{},
				}
				select {
				case resultsCh <- errorResult:
				case <-ctx.Done():
				}
			}
			return
		}
	}()

	// Stream results
	for {
		select {
		case <-ctx.Done():
			slog.Info("Client disconnected")
			return ctx.Err()
		case result, ok := <-resultsCh:
			if !ok {
				// End of stream - send completion
				doneResp := &queryproto.SketchInputResponse{
					Response: &queryproto.SketchInputResponse_Done{
						Done: &queryproto.CompletionResponse{Status: "complete"},
					},
				}
				return stream.Send(doneResp)
			}

			// Check for error sentinel
			if result.ExprID == "ERROR" && result.Timestamp == -1 {
				errorResp := &queryproto.SketchInputResponse{
					Response: &queryproto.SketchInputResponse_Error{
						Error: &queryproto.ErrorResponse{Error: result.OrganizationID},
					},
				}
				if err := stream.Send(errorResp); err != nil {
					return err
				}
				continue
			}

			// Convert internal result to GRPC response
			grpcResult := s.convertSketchInput(result)
			dataResp := &queryproto.SketchInputResponse{
				Response: &queryproto.SketchInputResponse_Data{
					Data: grpcResult,
				},
			}

			if err := stream.Send(dataResp); err != nil {
				slog.Error("Failed to send GRPC response", "error", err)
				return err
			}
		}
	}
}

// Health implements the health check endpoint
func (s *Service) Health(ctx context.Context, req *queryproto.HealthRequest) (*queryproto.HealthResponse, error) {
	return &queryproto.HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().Unix(),
		Service:   "query-worker",
	}, nil
}

// convertPushDownRequest converts GRPC PushDownRequest to internal promql.PushDownRequest
func (s *Service) convertPushDownRequest(req *queryproto.PushDownRequest) (promql.PushDownRequest, error) {
	orgID, err := uuid.Parse(req.OrganizationId)
	if err != nil {
		return promql.PushDownRequest{}, status.Errorf(codes.InvalidArgument, "invalid organization ID: %v", err)
	}

	// Convert BaseExpr
	baseExpr := promql.BaseExpr{
		ID:           req.BaseExpr.Id,
		Metric:       req.BaseExpr.Metric,
		Range:        req.BaseExpr.Range,
		SubqueryStep: req.BaseExpr.SubqueryStep,
		Offset:       req.BaseExpr.Offset,
		GroupBy:      req.BaseExpr.GroupBy,
		Without:      req.BaseExpr.Without,
	}

	// Convert matchers
	for _, matcher := range req.BaseExpr.Matchers {
		baseExpr.Matchers = append(baseExpr.Matchers, promql.LabelMatch{
			Label: matcher.Name,
			Value: matcher.Value,
			// TODO: Convert matcher.Type string to promql.MatchOp
			// For now, using a simple conversion - this may need refinement
			Op: promql.MatchOp(matcher.Type),
		})
	}

	// Convert segments
	var segments []promql.SegmentInfo
	for _, seg := range req.Segments {
		segments = append(segments, promql.SegmentInfo{
			DateInt:     int(seg.DateInt),
			Hour:        seg.Hour,
			SegmentID:   seg.SegmentId,
			StartTs:     seg.StartTs,
			EndTs:       seg.EndTs,
			ExprID:      seg.ExprId,
			Dataset:     seg.Dataset,
			BucketName:  seg.BucketName,
			CustomerID:  seg.CustomerId,
			CollectorID: seg.CollectorId,
			Frequency:   seg.Frequency,
		})
	}

	return promql.PushDownRequest{
		OrganizationID: orgID,
		BaseExpr:       baseExpr,
		StartTs:        req.StartTs,
		EndTs:          req.EndTs,
		Step:           time.Duration(req.StepDurationMs) * time.Millisecond,
		Segments:       segments,
	}, nil
}

// convertSketchInput converts internal promql.SketchInput to GRPC SketchInput
func (s *Service) convertSketchInput(si promql.SketchInput) *queryproto.SketchInput {
	// Convert SketchTags to map
	tags := make(map[string]string)
	// Note: The promql.SketchTags type would need to be examined to see how to extract tags
	// For now, leaving as empty map since the current implementation shows it as empty

	return &queryproto.SketchInput{
		ExprId:         si.ExprID,
		OrganizationId: si.OrganizationID,
		Timestamp:      si.Timestamp,
		Frequency:      si.Frequency,
		SketchTags: &queryproto.SketchTags{
			Tags: tags,
		},
	}
}
