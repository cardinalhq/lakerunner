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

package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/accumulation"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MockStore implements accumulation.Store interface
type MockStore struct {
	mock.Mock
	mu                sync.Mutex
	segments          map[string]lrdb.MetricSeg // key: segmentID
	compactedSegments map[int64]bool
	newSegments       []lrdb.CompactMetricSegsNew
	segmentToFile     map[string]string // objectID -> test file path
}

func NewMockStore() *MockStore {
	return &MockStore{
		segments:          make(map[string]lrdb.MetricSeg),
		compactedSegments: make(map[int64]bool),
		newSegments:       []lrdb.CompactMetricSegsNew{},
		segmentToFile:     make(map[string]string),
	}
}

func (m *MockStore) GetMetricEstimate(ctx context.Context, organizationID uuid.UUID, frequencyMs int32) int64 {
	args := m.Called(ctx, organizationID, frequencyMs)
	return args.Get(0).(int64)
}

func (m *MockStore) KafkaJournalUpsert(ctx context.Context, params lrdb.KafkaJournalUpsertParams) error {
	args := m.Called(ctx, params)
	return args.Error(0)
}

func (m *MockStore) KafkaJournalGetLastProcessed(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedParams) (int64, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockStore) GetMetricSegByPrimaryKey(ctx context.Context, params lrdb.GetMetricSegByPrimaryKeyParams) (lrdb.MetricSeg, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%d", params.SegmentID)
	if seg, ok := m.segments[key]; ok {
		return seg, nil
	}
	return lrdb.MetricSeg{}, sql.ErrNoRows
}

func (m *MockStore) SetSingleMetricSegCompacted(ctx context.Context, params lrdb.SetSingleMetricSegCompactedParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.compactedSegments[params.SegmentID] = true
	if seg, ok := m.segments[fmt.Sprintf("%d", params.SegmentID)]; ok {
		seg.Compacted = true
		m.segments[fmt.Sprintf("%d", params.SegmentID)] = seg
	}
	return nil
}

func (m *MockStore) CompactMetricSegs(ctx context.Context, params lrdb.CompactMetricSegsParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Mark old segments as compacted
	for _, old := range params.OldRecords {
		m.compactedSegments[old.SegmentID] = true
		if seg, ok := m.segments[fmt.Sprintf("%d", old.SegmentID)]; ok {
			seg.Compacted = true
			m.segments[fmt.Sprintf("%d", old.SegmentID)] = seg
		}
	}

	// Store new segments
	m.newSegments = append(m.newSegments, params.NewRecords...)

	return nil
}

func (m *MockStore) RollupMetricSegs(ctx context.Context, sourceParams lrdb.RollupSourceParams, targetParams lrdb.RollupTargetParams, sourceSegmentIDs []int64, newRecords []lrdb.RollupNewRecord) error {
	// Not used in compaction
	return nil
}

// MockBlobClient implements cloudstorage.Client interface
type MockBlobClient struct {
	mock.Mock
	mu            sync.Mutex
	uploadedFiles map[string]string // key -> local file path
	testDataDir   string
	objectMapping map[string]string // object ID -> test file path
}

// Verify interface implementation
var _ cloudstorage.Client = (*MockBlobClient)(nil)

func NewMockBlobClient(testDataDir string) *MockBlobClient {
	return &MockBlobClient{
		uploadedFiles: make(map[string]string),
		testDataDir:   testDataDir,
		objectMapping: make(map[string]string),
	}
}

func (m *MockBlobClient) DownloadObject(ctx context.Context, tmpdir, bucket, key string) (filename string, size int64, notFound bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Look up the test file path for this object ID
	testFilePath, exists := m.objectMapping[key]
	if !exists {
		return "", 0, true, fmt.Errorf("object not found: %s", key)
	}

	// Copy the file to tmpdir
	sourceFile, err := os.Open(testFilePath)
	if err != nil {
		return "", 0, false, err
	}
	defer sourceFile.Close()

	stat, err := sourceFile.Stat()
	if err != nil {
		return "", 0, false, err
	}

	destPath := filepath.Join(tmpdir, filepath.Base(testFilePath))
	destFile, err := os.Create(destPath)
	if err != nil {
		return "", 0, false, err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return "", 0, false, err
	}

	return destPath, stat.Size(), false, nil
}

func (m *MockBlobClient) UploadObject(ctx context.Context, bucket, key, sourceFilename string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.uploadedFiles[key] = sourceFilename
	return nil
}

func (m *MockBlobClient) DeleteObject(ctx context.Context, bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.uploadedFiles, key)
	return nil
}

// MockStorageProfileProvider implements storageprofile.StorageProfileProvider
type MockStorageProfileProvider struct {
	profile storageprofile.StorageProfile
}

func (m *MockStorageProfileProvider) GetStorageProfileForBucket(ctx context.Context, organizationID uuid.UUID, bucketName string) (storageprofile.StorageProfile, error) {
	return m.profile, nil
}

func (m *MockStorageProfileProvider) GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]storageprofile.StorageProfile, error) {
	return []storageprofile.StorageProfile{m.profile}, nil
}

func (m *MockStorageProfileProvider) GetStorageProfileForOrganization(ctx context.Context, organizationID uuid.UUID) (storageprofile.StorageProfile, error) {
	return m.profile, nil
}

func (m *MockStorageProfileProvider) GetStorageProfileForOrganizationAndInstance(ctx context.Context, organizationID uuid.UUID, instanceNum int16) (storageprofile.StorageProfile, error) {
	return m.profile, nil
}

func (m *MockStorageProfileProvider) GetStorageProfileForOrganizationAndCollector(ctx context.Context, organizationID uuid.UUID, collectorName string) (storageprofile.StorageProfile, error) {
	return m.profile, nil
}

func (m *MockStorageProfileProvider) GetLowestInstanceStorageProfile(ctx context.Context, organizationID uuid.UUID, bucketName string) (storageprofile.StorageProfile, error) {
	return m.profile, nil
}

func (m *MockStorageProfileProvider) ResolveOrganization(ctx context.Context, bucketName, objectPath string) (uuid.UUID, error) {
	return m.profile.OrganizationID, nil
}

func TestCompactionFullCycle(t *testing.T) {
	ctx := context.Background()

	// Test configuration
	testOrgID := uuid.New()
	testBucket := "test-bucket"
	testCollector := "test-collector"
	testDataDir := filepath.Join("..", "..", "..", "testdata", "metrics", "compact-test-0001")

	// Verify test data exists
	_, err := os.Stat(testDataDir)
	require.NoError(t, err, "Test data directory should exist")

	// Get list of test files
	testFiles, err := os.ReadDir(testDataDir)
	require.NoError(t, err)

	var parquetFiles []os.DirEntry
	for _, f := range testFiles {
		if filepath.Ext(f.Name()) == ".parquet" {
			parquetFiles = append(parquetFiles, f)
		}
	}
	require.NotEmpty(t, parquetFiles, "Should have parquet test files")

	// Setup mocks
	mockStore := NewMockStore()
	mockBlobClient := NewMockBlobClient(testDataDir)

	// Set up expectations
	mockStore.On("GetMetricEstimate", mock.Anything, testOrgID, int32(60000)).Return(int64(100000))

	// Create storage profile
	storageProfile := storageprofile.StorageProfile{
		OrganizationID: testOrgID,
		Bucket:         testBucket,
		CollectorName:  testCollector,
	}
	mockProfileProvider := &MockStorageProfileProvider{profile: storageProfile}

	// Create compaction strategy
	compactionConfig := Config{
		TargetFileSizeBytes: config.TargetFileSize, // Default target file size
		MaxAccumulationTime: "30s",
	}
	strategy := NewCompactionStrategy(compactionConfig)

	// Create manager
	manager, err := accumulation.NewManager(
		strategy,
		mockStore,
		30*time.Second,
		compactionConfig.TargetFileSizeBytes,
	)
	require.NoError(t, err)
	defer manager.Close()

	// Map actual test files to segment IDs and track real record counts
	// These are the actual record counts from the test files
	fileRecordCounts := map[string]int64{
		"tbl_299476429685392687.parquet": 227,
		"tbl_299476441865651503.parquet": 15,
		"tbl_299476446630380847.parquet": 227,
		"tbl_299476458558980900.parquet": 231,
		"tbl_299476464716219172.parquet": 15,
		"tbl_299476475503969060.parquet": 227,
		"tbl_299476481342440751.parquet": 227,
		"tbl_299476495972173103.parquet": 231,
		"tbl_299476496878142244.parquet": 15,
		"tbl_299476509242950436.parquet": 227,
		"tbl_299476513621803812.parquet": 227,
		"tbl_299476526607368996.parquet": 227,
	}

	// Prepare test segments in the mock store
	segmentIDs := make([]int64, 0, len(parquetFiles))

	for i, file := range parquetFiles {
		segmentID := idgen.DefaultFlakeGenerator.NextID()
		segmentIDs = append(segmentIDs, segmentID)

		fileInfo, err := file.Info()
		require.NoError(t, err)

		// Use actual record count from the file
		recordCount := fileRecordCounts[file.Name()]
		if recordCount == 0 {
			recordCount = 100 // fallback if not found
		}

		// Create timestamps for this segment
		startTs := int64(1736400000000 + i*60000)
		endTs := int64(1736400060000 + i*60000)

		// Create the object ID that will be used by CreateReaderStack
		dateint, hour := helpers.MSToDateintHour(startTs)
		objectID := helpers.MakeDBObjectID(testOrgID, testCollector, dateint, hour, segmentID, "metrics")

		// Map the object ID to the actual test file path
		testFilePath := filepath.Join(testDataDir, file.Name())
		mockBlobClient.objectMapping[objectID] = testFilePath

		// Create a mock segment in the store
		segment := lrdb.MetricSeg{
			OrganizationID: testOrgID,
			Dateint:        20250109,
			FrequencyMs:    60000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			RecordCount:    recordCount,
			FileSize:       fileInfo.Size(),
			Compacted:      false,
			SlotID:         0,
			SlotCount:      1,
			TsRange: pgtype.Range[pgtype.Int8]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Int8{Int64: startTs, Valid: true},
				Upper:     pgtype.Int8{Int64: endTs, Valid: true},
				Valid:     true,
			},
			Fingerprints: []int64{int64(1000 + i), int64(2000 + i)}, // Mock fingerprints
		}
		mockStore.segments[fmt.Sprintf("%d", segmentID)] = segment
	}

	// Create synthetic notifications and add work
	inputRecordCount := int64(0)
	inputFileSize := int64(0)

	for i, segmentID := range segmentIDs {
		segment := mockStore.segments[fmt.Sprintf("%d", segmentID)]

		notification := &messages.MetricSegmentNotificationMessage{
			OrganizationID: testOrgID,
			DateInt:        20250109,
			FrequencyMs:    60000,
			SegmentID:      segmentID,
			InstanceNum:    1,
			SlotID:         0,
			SlotCount:      1,
			RecordCount:    segment.RecordCount,
			FileSize:       segment.FileSize,
		}

		inputRecordCount += segment.RecordCount
		inputFileSize += segment.FileSize

		// Add work to manager
		err = manager.AddWorkFromKafka(ctx, notification, mockProfileProvider, nil)
		require.NoError(t, err, "Should add work for segment %d", i)
	}

	// Verify work was accumulated
	assert.True(t, manager.HasWork(), "Should have accumulated work")

	// Get accumulators and flush them
	accumulators := manager.GetAccumulators()
	require.NotEmpty(t, accumulators, "Should have at least one accumulator")

	for key, acc := range accumulators {
		err = accumulation.FlushAccumulator(ctx, acc, mockStore, mockBlobClient, manager.GetTmpDir(), strategy, nil)
		require.NoError(t, err, "Flush should succeed for accumulator %v", key)
	}

	// Verify results

	// 1. Check that files were uploaded to blob storage
	mockBlobClient.mu.Lock()
	uploadedCount := len(mockBlobClient.uploadedFiles)
	mockBlobClient.mu.Unlock()
	assert.Greater(t, uploadedCount, 0, "Should have uploaded at least one file")
	assert.LessOrEqual(t, uploadedCount, len(segmentIDs), "Should not upload more files than input")

	// 2. Check that old segments were marked as compacted
	mockStore.mu.Lock()
	for _, segmentID := range segmentIDs {
		assert.True(t, mockStore.compactedSegments[segmentID], "Segment %d should be marked as compacted", segmentID)
	}

	// 3. Check new segments were created
	assert.Greater(t, len(mockStore.newSegments), 0, "Should have created new segments")

	// 4. Verify record counts (allowing for small loss due to aggregation/dedup)
	outputRecordCount := int64(0)
	outputFileSize := int64(0)
	for _, newSeg := range mockStore.newSegments {
		outputRecordCount += newSeg.RecordCount
		outputFileSize += newSeg.FileSize

		// Verify timestamps are set
		assert.Greater(t, newSeg.StartTs, int64(0), "StartTs should be set")
		assert.Greater(t, newSeg.EndTs, newSeg.StartTs, "EndTs should be after StartTs")

		// Verify fingerprints are set
		assert.NotEmpty(t, newSeg.Fingerprints, "Fingerprints should be set")
	}

	// Check record loss is within reasonable bounds
	// The test data contains many gauge metrics without sketches that will be dropped
	// This is expected behavior for the current implementation
	recordLoss := inputRecordCount - outputRecordCount
	assert.GreaterOrEqual(t, recordLoss, int64(0), "Should not gain records")

	// Calculate loss percentage
	lossPercentage := float64(recordLoss) / float64(inputRecordCount) * 100
	assert.LessOrEqual(t, lossPercentage, 90.0, "Should not lose more than 90%% of records")

	// Ensure we still have meaningful output
	assert.Greater(t, outputRecordCount, int64(10), "Should have at least 10 output records")

	// Verify we have fewer records due to aggregation/deduplication
	assert.Less(t, outputRecordCount, inputRecordCount, "Should have fewer output records due to deduplication/aggregation")

	// 5. Verify segment reduction (we should have fewer output segments than input)
	assert.Less(t, len(mockStore.newSegments), len(segmentIDs),
		"Compaction should reduce segment count (had %d, now %d)", len(segmentIDs), len(mockStore.newSegments))

	mockStore.mu.Unlock()

	// 6. Verify uploaded files exist and have reasonable sizes
	mockBlobClient.mu.Lock()
	for key, localPath := range mockBlobClient.uploadedFiles {
		stat, err := os.Stat(localPath)
		assert.NoError(t, err, "Uploaded file should exist: %s", key)
		assert.Greater(t, stat.Size(), int64(1000), "File should have reasonable size")
	}
	mockBlobClient.mu.Unlock()

	// Log summary
	t.Logf("Compaction Summary:")
	t.Logf("  Input: %d segments, %d records, %d bytes", len(segmentIDs), inputRecordCount, inputFileSize)
	t.Logf("  Output: %d segments, %d records, %d bytes", len(mockStore.newSegments), outputRecordCount, outputFileSize)
	t.Logf("  Reduction: %.1f%% segments, %.1f%% records",
		100.0*(1-float64(len(mockStore.newSegments))/float64(len(segmentIDs))),
		100.0*(1-float64(outputRecordCount)/float64(inputRecordCount)))
}

// TestCompactionWithLargeFiles tests compaction behavior with already-large files
func TestCompactionWithLargeFiles(t *testing.T) {
	ctx := context.Background()

	testOrgID := uuid.New()
	testBucket := "test-bucket"
	testCollector := "test-collector"
	testDataDir := filepath.Join("..", "..", "..", "testdata", "metrics", "compact-test-0002")

	// Verify test data exists
	_, err := os.Stat(testDataDir)
	require.NoError(t, err, "Test data directory should exist")

	// Setup mocks
	mockStore := NewMockStore()
	_ = NewMockBlobClient(testDataDir) // Not used for this test since file is already optimized

	// Set up expectations - return a smaller estimate to make files seem "large enough"
	mockStore.On("GetMetricEstimate", mock.Anything, testOrgID, int32(60000)).Return(int64(1000))

	// Create storage profile
	storageProfile := storageprofile.StorageProfile{
		OrganizationID: testOrgID,
		Bucket:         testBucket,
		CollectorName:  testCollector,
	}
	mockProfileProvider := &MockStorageProfileProvider{profile: storageProfile}

	// Create compaction strategy with smaller target
	compactionConfig := Config{
		TargetFileSizeBytes: 50 * 1024, // 50KB target (smaller than test files)
		MaxAccumulationTime: "30s",
	}
	strategy := NewCompactionStrategy(compactionConfig)

	// Create manager
	manager, err := accumulation.NewManager(
		strategy,
		mockStore,
		30*time.Second,
		compactionConfig.TargetFileSizeBytes,
	)
	require.NoError(t, err)
	defer manager.Close()

	// Create a large segment that's already optimized
	largeSegmentID := idgen.DefaultFlakeGenerator.NextID()
	largeSegment := lrdb.MetricSeg{
		OrganizationID: testOrgID,
		Dateint:        20250109,
		FrequencyMs:    60000,
		SegmentID:      largeSegmentID,
		InstanceNum:    1,
		RecordCount:    5000,  // Large record count
		FileSize:       80000, // 80KB - larger than target
		Compacted:      false,
		SlotID:         0,
		SlotCount:      1,
		TsRange: pgtype.Range[pgtype.Int8]{
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Lower:     pgtype.Int8{Int64: 1736400000000, Valid: true},
			Upper:     pgtype.Int8{Int64: 1736400300000, Valid: true},
			Valid:     true,
		},
		Fingerprints: []int64{1001, 2001},
	}
	mockStore.segments[fmt.Sprintf("%d", largeSegmentID)] = largeSegment

	// Send notification for large segment
	notification := &messages.MetricSegmentNotificationMessage{
		OrganizationID: testOrgID,
		DateInt:        20250109,
		FrequencyMs:    60000,
		SegmentID:      largeSegmentID,
		InstanceNum:    1,
		SlotID:         0,
		SlotCount:      1,
		RecordCount:    largeSegment.RecordCount,
		FileSize:       largeSegment.FileSize,
	}

	err = manager.AddWorkFromKafka(ctx, notification, mockProfileProvider, nil)
	require.NoError(t, err)

	// The large file should be marked as compacted immediately without processing
	mockStore.mu.Lock()
	assert.True(t, mockStore.compactedSegments[largeSegmentID],
		"Large segment should be marked as compacted without processing")
	mockStore.mu.Unlock()

	// Verify no work was accumulated (since it was already optimized)
	assert.False(t, manager.HasWork(), "Should not accumulate work for already-optimized segment")
}
