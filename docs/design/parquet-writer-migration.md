# Parquet Writer Migration Guide

This document provides a detailed plan for migrating from the existing buffet writer to the new unified parquet writer system.

## Current State Analysis

### Existing Writer Locations

1. **Buffet Writer** (`internal/buffet/buffet.go`)
   - Used by logs compaction
   - Used by some traces processing
   - General-purpose spill-to-disk then write approach

2. **TID Merger** (`internal/tidprocessing/tidmerge.go`) 
   - Used by metrics compaction
   - Specialized for TID-ordered merging
   - Critical for metrics processing integrity

3. **Log Fingerprinting** (`internal/logcrunch/fingerprint_file.go`)
   - Used by logs compaction
   - Handles timestamp-based sorting and splitting
   - Includes fingerprint tracking for deduplication

### Dependencies and Risk Assessment

#### High Risk (Metrics)
- **File**: `cmd/compact_metrics.go` → `internal/metriccompaction/compaction.go`
- **Current Approach**: Uses TIDMerger for TID-ordered output
- **Risk**: Incorrect TID handling breaks query correctness
- **Migration Priority**: Last (Phase 4)

#### Medium Risk (Logs)  
- **File**: `cmd/compact_logs.go` → `internal/logcrunch/compaction.go`
- **Current Approach**: Uses buffet writer with external sorting
- **Risk**: Performance impact, memory usage changes
- **Migration Priority**: First (Phase 2)

#### Low Risk (Traces)
- **File**: `cmd/compact_traces.go` → `internal/tracecompaction/*`
- **Current Approach**: Uses similar pattern to logs
- **Risk**: Minimal, less complex requirements
- **Migration Priority**: Middle (Phase 3)

## Phase-by-Phase Migration Plan

### Phase 1: Foundation (Weeks 1-2)
**Goal**: Establish new writer system without breaking existing functionality

#### Tasks
1. **Complete Implementation**
   - ✅ Core writer interfaces and components 
   - ✅ Signal-specific factory functions
   - ✅ Comprehensive test suite
   - Add performance benchmarks
   - Add integration tests

2. **Validation Framework**
   - Create writer compatibility tests
   - Add output verification utilities
   - Implement A/B testing framework

3. **Documentation**
   - ✅ Requirements documentation
   - API documentation
   - Migration runbooks

#### Deliverables
- New parquet writer package fully implemented
- Test suite with >90% coverage
- Performance benchmarks established
- Documentation complete

#### Success Criteria
- All tests pass
- Benchmarks show comparable performance to existing writers
- Code review approved

### Phase 2: Logs Migration (Weeks 3-4)
**Goal**: Replace buffet writer in logs compaction

#### Prerequisites
- Phase 1 complete
- Logs compaction thoroughly understood
- Test data prepared for validation

#### Implementation Steps

1. **Create Logs Writer Adapter**
   ```go
   // cmd/logs_writer_adapter.go
   func NewLogsWriterFromBuffet(baseName, tmpdir string, nodes map[string]parquet.Node, rpf int64) (*parquetwriter.UnifiedWriter, error) {
       return parquetwriter.NewLogsWriter(baseName, tmpdir, nodes, targetFileSize)
   }
   ```

2. **Modify Logs Compaction**
   - Update `internal/logcrunch/fingerprint_file.go`
   - Replace buffet.NewWriter with new logs writer
   - Maintain existing API for smooth transition

3. **Add Feature Flags**
   ```go
   // Environment variable to control writer selection
   useNewParquetWriter := os.Getenv("LAKERUNNER_USE_NEW_PARQUET_WRITER") == "true"
   ```

4. **Gradual Rollout**
   - Start with 10% of workloads
   - Monitor metrics and error rates  
   - Gradually increase to 100%

#### Validation Steps
- Compare output files byte-for-byte (where possible)
- Verify timestamp ordering in output
- Check file size distributions
- Monitor memory usage and performance

#### Rollback Plan
- Feature flag to revert to buffet writer
- No schema changes required
- Monitoring alerts for performance regression

#### Success Criteria
- No correctness regressions
- Performance within 10% of baseline
- Memory usage within acceptable bounds
- Zero critical errors in production

### Phase 3: Traces Migration (Weeks 5-6)
**Goal**: Migrate traces compaction to new writer

#### Prerequisites  
- Phase 2 deployed and stable
- Traces compaction patterns analyzed
- Test cases prepared

#### Implementation Steps

1. **Analyze Current Traces Processing**
   - Document slot-based organization
   - Understand file splitting requirements
   - Identify any special processing needs

2. **Create Traces Writer Integration**
   - Implement traces-specific factory function
   - Add slot ID tracking and statistics
   - Ensure proper ordering by start time

3. **Update Traces Compaction**
   - Modify `cmd/compact_traces.go`
   - Update related processing in `internal/tracecompaction/`
   - Add monitoring and metrics

4. **Testing and Validation**
   - Verify slot grouping behavior
   - Check trace/span locality
   - Validate query performance

#### Success Criteria
- Traces queries perform as expected
- Slot organization maintained
- File sizes within target ranges
- No data corruption or loss

### Phase 4: Metrics Migration (Weeks 7-8)
**Goal**: Replace TIDMerger with new writer (HIGHEST RISK)

#### Prerequisites
- Phases 2 and 3 deployed and stable
- Extensive testing completed
- Rollback procedures documented

#### Risk Mitigation
- Extended testing period with synthetic data
- Shadow deployment for validation
- Real-time monitoring with automatic rollback
- Expert review of all changes

#### Implementation Steps

1. **Deep Analysis of TIDMerger**
   - Document all TID handling logic
   - Understand merge algorithms
   - Identify critical invariants

2. **Create Metrics Writer Wrapper**
   ```go
   // internal/tidprocessing/unified_merger.go
   func NewTIDMergerFromUnified(config TIDMergerConfig) (*TIDMerger, error) {
       // Wraps unified writer with TIDMerger-compatible API
   }
   ```

3. **Extensive Testing**
   - Unit tests for TID grouping logic
   - Integration tests with real metric workloads
   - Performance testing under load
   - Correctness validation with known datasets

4. **Gradual Migration**
   - Start with non-critical metric types
   - Monitor query correctness closely
   - Full rollout only after extensive validation

#### Validation Requirements
- **TID Ordering**: Verify all output maintains TID order
- **No TID Splitting**: Confirm no TID groups are split across files
- **Query Correctness**: Validate metric queries return correct results
- **Performance**: Ensure comparable processing speed

#### Success Criteria
- Zero query correctness regressions
- TID integrity maintained 100%
- Performance within 5% of baseline
- Memory usage acceptable

### Phase 5: Cleanup and Optimization (Week 9)
**Goal**: Remove old code and optimize new system

#### Tasks
1. **Remove Deprecated Code**
   - Delete buffet writer (after confirming no other users)
   - Remove old TIDMerger implementation
   - Clean up intermediate adapter code

2. **Performance Optimization**
   - Tune buffer sizes based on production data
   - Optimize memory usage patterns
   - Add advanced features (parallel writing, etc.)

3. **Documentation Updates**
   - Update all code documentation
   - Create operational runbooks
   - Document new monitoring and alerting

#### Success Criteria
- Code complexity reduced
- Performance improved from baseline
- Documentation complete and accurate

## Monitoring and Alerting

### Key Metrics to Track

#### Correctness Metrics
- Query result validation (automated tests)
- File ordering verification
- TID integrity checks (for metrics)
- Record count accuracy

#### Performance Metrics
- Processing latency per signal type
- Throughput (records/second)
- Memory usage patterns
- File size distributions

#### Error Metrics
- Writer error rates
- Schema violation counts
- Context cancellation handling
- Resource exhaustion events

### Alerting Thresholds

#### Critical Alerts (Page immediately)
- Query correctness failures
- TID splitting detected (metrics)
- Processing error rate > 1%
- Memory usage > 90% of limit

#### Warning Alerts (Alert during business hours)
- Performance degradation > 20%
- File size distribution anomalies  
- Increased error rates < 1%
- Memory usage 70-90% of limit

## Risk Mitigation Strategies

### Technical Risks

1. **Data Corruption**
   - **Mitigation**: Extensive testing, checksums, validation queries
   - **Detection**: Automated correctness tests, query result comparison
   - **Response**: Immediate rollback, data recovery procedures

2. **Performance Regression**  
   - **Mitigation**: Benchmarking, load testing, gradual rollout
   - **Detection**: Real-time performance monitoring
   - **Response**: Tuning, rollback if severe

3. **Memory Exhaustion**
   - **Mitigation**: Buffer size limits, memory monitoring
   - **Detection**: Memory usage alerts, OOM detection  
   - **Response**: Dynamic buffer sizing, emergency memory cleanup

### Operational Risks

1. **Deployment Issues**
   - **Mitigation**: Feature flags, blue-green deployment
   - **Detection**: Health checks, error rate monitoring
   - **Response**: Automated rollback, manual intervention procedures

2. **Monitoring Gaps**
   - **Mitigation**: Comprehensive monitoring during migration
   - **Detection**: Regular review of metrics coverage
   - **Response**: Add missing monitors, improve alerting

## Testing Strategy

### Unit Testing
- All new components individually tested
- Error conditions and edge cases covered
- Memory usage and performance validated

### Integration Testing
- End-to-end pipeline testing
- Real data pattern validation
- Cross-component interaction testing

### Load Testing
- Production-scale data volumes
- Peak load scenarios
- Memory and CPU usage under load

### Correctness Testing
- Output file validation
- Query result comparison  
- Data integrity verification

### Chaos Testing
- Simulated failures during processing
- Resource exhaustion scenarios
- Context cancellation handling

## Success Metrics

### Migration Success
- All signal types migrated successfully
- Zero critical production issues
- Performance maintained or improved
- Code complexity reduced

### Long-term Success
- Easier to add new signal types
- Better resource utilization
- Improved monitoring and observability
- Reduced maintenance burden

## Timeline Summary

| Phase | Duration | Key Activities | Risk Level |
|-------|----------|---------------|------------|
| 1 | Weeks 1-2 | Foundation, testing, documentation | Low |
| 2 | Weeks 3-4 | Logs migration | Medium |
| 3 | Weeks 5-6 | Traces migration | Low-Medium |
| 4 | Weeks 7-8 | Metrics migration | High |
| 5 | Week 9 | Cleanup and optimization | Low |

**Total Duration**: 9 weeks
**High-Risk Activities**: Weeks 7-8 (metrics migration)
**Key Milestones**: End of weeks 2, 4, 6, 8, 9