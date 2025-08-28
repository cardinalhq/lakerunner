# GOB vs CBOR Spiller Performance Comparison

## Summary

CBOR significantly outperforms GOB for spilling `map[string]any` data structures in the parquetwriter package:

- **32% smaller file sizes** (292.3 vs 432.1 bytes/row)
- **18% faster roundtrip performance** (18.8ms vs 22.9ms for 5K records)  
- **29% fewer memory allocations** (166K vs 205K allocs)
- **Better type preservation** for metrics data
- **More compact encoding** especially for mixed data types

## Detailed Results

### File Size Comparison (10K Mixed Records)
```
GOB:  4,320,678 bytes (432.1 bytes/row)
CBOR: 2,923,150 bytes (292.3 bytes/row)
→ CBOR is 32% more space efficient
```

### Write Performance (10K Mixed Records)
```
GOB:  44.8ms  (4.2MB memory, 260K allocs)
CBOR: 31.7ms  (2.1MB memory, 100K allocs)
→ CBOR is 29% faster with 50% less memory usage
```

### Roundtrip Performance (5K Records)
```
GOB:  22.9ms  (6.0MB memory, 205K allocs)
CBOR: 18.8ms  (6.0MB memory, 166K allocs) 
→ CBOR is 18% faster with 19% fewer allocations
```

### Performance by Data Type

| Data Type | GOB (ms/1K) | CBOR (ms/1K) | CBOR Advantage |
|-----------|-------------|--------------|----------------|
| Metrics   | 2.95        | 2.44         | 17% faster     |
| Logs      | 3.38        | 2.60         | 23% faster     |
| Mixed     | 4.67        | 3.28         | 30% faster     |

## Key Findings

### CBOR Advantages
1. **Space Efficiency**: Much more compact binary representation
2. **Speed**: Faster encoding/decoding, especially for complex data
3. **Memory**: Significantly fewer allocations and lower memory usage
4. **Type Preservation**: Better handling of Go types like `[]float64`, direct `map[string]any` decoding
5. **Standards**: CBOR is an IETF standard (RFC 8949) vs GOB being Go-specific
6. **Simplified Decoding**: Uses `DefaultMapType` to decode directly to `map[string]any` without conversion

### When to Use CBOR
- **Recommended for new spillers**: Default choice for parquetwriter
- **Mixed data types**: Excels with complex `map[string]any` structures
- **Large datasets**: Space savings become more significant
- **Performance critical**: When spilling is a bottleneck

### When to Keep GOB
- **Existing code**: No need to migrate working GOB spillers immediately
- **Go-specific types**: If using complex Go-only types not supported by CBOR
- **Compatibility**: If interoperating with existing GOB-based systems

## Benchmark Environment
- **CPU**: Apple M2 Pro
- **Go**: 1.25
- **Platform**: darwin/arm64  
- **Test Data**: Realistic metrics, logs, and mixed-type data patterns
- **Methodology**: Multiple runs with memory profiling enabled

## Usage Recommendation

For new parquetwriter spillers, use the CBOR implementation:

```go
// Recommended
spiller, err := spillers.NewCborSpiller()

// Legacy (still supported)
spiller := spillers.NewGobSpiller()
```

The performance benefits make CBOR the clear choice for new development, while existing GOB spillers can be migrated when convenient.