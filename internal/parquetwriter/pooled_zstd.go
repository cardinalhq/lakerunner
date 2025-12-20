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

package parquetwriter

import (
	"io"
	"sync"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/klauspost/compress/zstd"
)

// pooledZstdCodec implements compress.Codec with encoder pooling.
// The standard arrow-go zstd codec creates a new encoder for every
// EncodeLevel and NewWriterLevel call, allocating ~1GB in history buffers
// each time. This codec pools encoders by compression level.
type pooledZstdCodec struct {
	// Pool of encoders keyed by compression level
	// We use separate pools per level since encoders are configured per-level
	encoderPools sync.Map // map[zstd.EncoderLevel]*sync.Pool

	// Single decoder for all operations (decoders are stateless for our use)
	decoder     *zstd.Decoder
	decoderOnce sync.Once

	// Single default encoder for Encode() calls (level-agnostic)
	defaultEncoder     *zstd.Encoder
	defaultEncoderOnce sync.Once
}

var _ compress.Codec = (*pooledZstdCodec)(nil)

// Global instance registered via init()
var pooledCodec = &pooledZstdCodec{}

func init() {
	compress.RegisterCodec(compress.Codecs.Zstd, pooledCodec)
}

func (p *pooledZstdCodec) getDecoder() *zstd.Decoder {
	p.decoderOnce.Do(func() {
		p.decoder, _ = zstd.NewReader(nil)
	})
	return p.decoder
}

func (p *pooledZstdCodec) getDefaultEncoder() *zstd.Encoder {
	p.defaultEncoderOnce.Do(func() {
		p.defaultEncoder, _ = zstd.NewWriter(nil, zstd.WithZeroFrames(true))
	})
	return p.defaultEncoder
}

func (p *pooledZstdCodec) getEncoderPool(level zstd.EncoderLevel) *sync.Pool {
	if pool, ok := p.encoderPools.Load(level); ok {
		return pool.(*sync.Pool)
	}

	// Create new pool for this level
	newPool := &sync.Pool{
		New: func() any {
			enc, _ := zstd.NewWriter(nil,
				zstd.WithZeroFrames(true),
				zstd.WithEncoderLevel(level),
			)
			return enc
		},
	}

	// Store and return (LoadOrStore handles race)
	actual, _ := p.encoderPools.LoadOrStore(level, newPool)
	return actual.(*sync.Pool)
}

func (p *pooledZstdCodec) getEncoder(level zstd.EncoderLevel) *zstd.Encoder {
	pool := p.getEncoderPool(level)
	return pool.Get().(*zstd.Encoder)
}

func (p *pooledZstdCodec) putEncoder(level zstd.EncoderLevel, enc *zstd.Encoder) {
	pool := p.getEncoderPool(level)
	pool.Put(enc)
}

// Decode implements compress.Codec
func (p *pooledZstdCodec) Decode(dst, src []byte) []byte {
	dst, err := p.getDecoder().DecodeAll(src, dst[:0])
	if err != nil {
		panic(err)
	}
	return dst
}

// Encode implements compress.Codec using the default encoder
func (p *pooledZstdCodec) Encode(dst, src []byte) []byte {
	return p.getDefaultEncoder().EncodeAll(src, dst[:0])
}

// EncodeLevel implements compress.Codec with pooled encoders
func (p *pooledZstdCodec) EncodeLevel(dst, src []byte, level int) []byte {
	compressLevel := zstd.EncoderLevelFromZstd(level)
	if level == compress.DefaultCompressionLevel {
		compressLevel = zstd.SpeedDefault
	}

	enc := p.getEncoder(compressLevel)
	result := enc.EncodeAll(src, dst[:0])
	p.putEncoder(compressLevel, enc)

	return result
}

// CompressBound implements compress.Codec
// From zstd.h, ZSTD_COMPRESSBOUND
func (p *pooledZstdCodec) CompressBound(len int64) int64 {
	extra := ((128 << 10) - len) >> 11
	if len >= (128 << 10) {
		extra = 0
	}
	return len + (len >> 8) + extra
}

// pooledZstdWriter wraps a zstd.Encoder to return it to pool on Close
type pooledZstdWriter struct {
	enc   *zstd.Encoder
	level zstd.EncoderLevel
	codec *pooledZstdCodec
}

func (w *pooledZstdWriter) Write(p []byte) (n int, err error) {
	return w.enc.Write(p)
}

func (w *pooledZstdWriter) Close() error {
	err := w.enc.Close()
	// Reset encoder for reuse and return to pool
	w.enc.Reset(nil)
	w.codec.putEncoder(w.level, w.enc)
	return err
}

// NewReader implements compress.StreamingCodec
func (p *pooledZstdCodec) NewReader(r io.Reader) io.ReadCloser {
	ret, _ := zstd.NewReader(r)
	return &zstdReaderCloser{ret}
}

type zstdReaderCloser struct {
	*zstd.Decoder
}

func (z *zstdReaderCloser) Close() error {
	z.Decoder.Close()
	return nil
}

// NewWriter implements compress.StreamingCodec with default level
func (p *pooledZstdCodec) NewWriter(w io.Writer) io.WriteCloser {
	return p.mustNewWriterLevel(w, compress.DefaultCompressionLevel)
}

// NewWriterLevel implements compress.StreamingCodec with pooled encoders
func (p *pooledZstdCodec) NewWriterLevel(w io.Writer, level int) (io.WriteCloser, error) {
	return p.mustNewWriterLevel(w, level), nil
}

func (p *pooledZstdCodec) mustNewWriterLevel(w io.Writer, level int) io.WriteCloser {
	compressLevel := zstd.EncoderLevelFromZstd(level)
	if level == compress.DefaultCompressionLevel {
		compressLevel = zstd.SpeedDefault
	}

	enc := p.getEncoder(compressLevel)
	enc.Reset(w)

	return &pooledZstdWriter{
		enc:   enc,
		level: compressLevel,
		codec: p,
	}
}
