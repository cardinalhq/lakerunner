# Arrow Allocator Comparison

This document explains why we use the default Go allocator for the Arrow backend.

## TL;DR

**We use `memory.DefaultAllocator` (Go's heap allocator)** because alternative allocators
tested significantly slower with no meaningful benefits.

## Testing History

We tested alternative allocators to see if moving memory allocation outside Go's heap
could improve performance by reducing GC pressure.

## Benchmark Results (900 rows, 52 columns)

### Default Go Allocator (Production) ✅ WINNER
- **Throughput**: 22,271 logs/sec/core
- **Memory**: 200,250,344 B/op (200 MB)
- **Allocations**: 87,739 allocs/op
- **Output size**: 54,857 bytes

### CgoArrowAllocator (Arrow C++ Memory Pool)
- **Throughput**: 19,395 logs/sec/core (**12.9% slower** ❌)
- **Go Memory**: 196,074,631 B/op (196 MB) - 4 MB less
- **Allocations**: 87,759 allocs/op - 20 more allocations
- **C++ Pool tracked**: 20,263,232 bytes (20.3 MB)
- **Output size**: 54,826 bytes

### Key Findings

1. **CgoArrowAllocator is significantly slower**: 12.9% performance loss
2. **Allocation split**: ~20 MB in C++ pool, ~196 MB still on Go heap (10% in C++)
3. **Minimal Go memory savings**: Only 4 MB reduction (2%)
4. **CGO overhead dominates**: The cost of CGO calls outweighs any GC benefits

## Analysis

### Why is CgoArrowAllocator slower?

1. **CGO overhead dominates**: Each allocation/free requires crossing the Go/C boundary
2. **Many small allocations**: Arrow's column builders make ~87,000 allocations per write
3. **Minimal GC benefit**: Only 10% of memory moved to C++ pool, so GC pressure barely reduced
4. **Go heap still active**: 90% of allocations remain on Go heap, requiring GC anyway

### Memory Allocation Breakdown

**Default Go Allocator:**
- 100% on Go heap (200 MB)
- Simple, fast allocations
- GC handles everything

**CgoArrowAllocator:**
- 10% in C++ pool (20 MB) - requires CGO calls
- 90% still on Go heap (196 MB) - requires GC
- Worst of both worlds: CGO overhead + GC pressure

## Conclusion

**Strong Recommendation**: Use the default Go allocator (production default).

The CgoArrowAllocator provides **no benefit**:
- ❌ 12.9% slower performance
- ❌ Minimal memory savings (4 MB / 2%)
- ❌ CGO overhead exceeds any GC benefits
- ❌ Still requires GC for 90% of memory

**Only consider alternative allocators if:**
- You're exporting data via C API (cdata interface) and need C-visible memory
- Profiling shows GC is a major bottleneck (it's not in our case)
- You're willing to accept 13%+ performance loss for reduced GC pressure

## How to Test Alternative Allocators (Future)

If you want to test CgoArrowAllocator again:

1. **Install Arrow C++**: `brew install apache-arrow`
2. **Create allocator wrapper** with build tags:
   ```go
   //go:build cgo && ccalloc
   func newArrowAllocator() memory.Allocator {
       return memory.NewCgoArrowAllocator()
   }
   ```
3. **Update NewArrowBackend**: Use `newArrowAllocator()` instead of `memory.DefaultAllocator`
4. **Benchmark**: `go test -tags="cgo,ccalloc" -bench=BenchmarkBackendComparison ./internal/perftest/`

The CgoArrowAllocator has an `AllocatedBytes()` method to track C++ pool usage separately
from Go heap allocations.
