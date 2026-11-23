package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

const (
	recordSize         = 43 // itch_tob Record: itch_ns, seq, bid_px, bid_sz, ask_px, ask_sz, trade_px, trade_sz (u32), sym u16, event u8
	eventRecordSize    = 44 // itch_tob --events EventRecord: itch_ns, seq, ref, new_ref u64, px, shares u32, sym u16, type u8, side u8
	depthHeaderSize    = 27 // itch_tob --depth DepthHeader: itch_ns, seq u64, trade_px, trade_sz u32, sym u16, event u8; then 4*depth u32 levels
	flushRows          = 50000
	maxBufferedBytes   = 512 << 20
	decodeChunkRecords = 4096
)

// sink writes one symbol's records to its parquet file.
type sink interface {
	add(record []byte) // buffers one record
	rows() int         // buffered rows
	flush() error      // writes the buffered rows as one row group
	close() error
}

// typedSink writes one symbol's records to its parquet file through a typed writer; decode turns a record into a row.
type typedSink[T any] struct {
	file   *os.File
	writer *parquet.GenericWriter[T]
	buffer []T
	decode func(record []byte) T
}

func (state *typedSink[T]) add(record []byte) {
	state.buffer = append(state.buffer, state.decode(record))
}

func (state *typedSink[T]) rows() int { return len(state.buffer) }

func (state *typedSink[T]) flush() error {
	if len(state.buffer) == 0 {
		return nil
	}
	if _, err := state.writer.Write(state.buffer); err != nil {
		return err
	}
	state.buffer = state.buffer[:0]
	return state.writer.Flush() // one row group per flush
}

func (state *typedSink[T]) close() error {
	if err := state.writer.Close(); err != nil {
		return err
	}
	return state.file.Close()
}

func newTypedSink[T any](file *os.File, options []parquet.WriterOption, decode func([]byte) T) sink {
	return &typedSink[T]{file: file, writer: parquet.NewGenericWriter[T](file, options...), decode: decode}
}

func tickDecoder(midnight int64) func([]byte) Tick {
	return func(record []byte) Tick {
		return Tick{
			Timestamp:  int64(binary.LittleEndian.Uint64(record[0:8])) + midnight,
			Event:      eventName(record[42]),
			Bid:        price(binary.LittleEndian.Uint32(record[16:20])),
			BidSize:    int32(binary.LittleEndian.Uint32(record[20:24])),
			Ask:        price(binary.LittleEndian.Uint32(record[24:28])),
			AskSize:    int32(binary.LittleEndian.Uint32(record[28:32])),
			TradePrice: price(binary.LittleEndian.Uint32(record[32:36])),
			TradeSize:  int32(binary.LittleEndian.Uint32(record[36:40])),
			Seq:        binary.LittleEndian.Uint64(record[8:16]),
		}
	}
}

func eventDecoder(midnight int64) func([]byte) OrderEvent {
	return func(record []byte) OrderEvent {
		side := ""
		if record[43] != 0 {
			side = string(record[43:44])
		}
		return OrderEvent{
			Timestamp: int64(binary.LittleEndian.Uint64(record[0:8])) + midnight,
			Event:     string(record[42:43]),
			Seq:       binary.LittleEndian.Uint64(record[8:16]),
			Ref:       binary.LittleEndian.Uint64(record[16:24]),
			NewRef:    binary.LittleEndian.Uint64(record[24:32]),
			Side:      side,
			Price:     price(binary.LittleEndian.Uint32(record[32:36])),
			Shares:    int32(binary.LittleEndian.Uint32(record[36:40])),
		}
	}
}

// bookSink writes the orderbook schema of schemaFor. It buffers the raw records (a fraction of the size of decoded rows) and decodes at flush.
type bookSink struct {
	file     *os.File
	writer   *parquet.Writer
	depth    int
	size     int // bytes per record
	buffer   []byte
	midnight int64
}

func (state *bookSink) add(record []byte) { state.buffer = append(state.buffer, record...) }

func (state *bookSink) rows() int { return len(state.buffer) / state.size }

func (state *bookSink) flush() error {
	if len(state.buffer) == 0 {
		return nil
	}
	chunk := make([]parquet.Row, 0, decodeChunkRecords)
	columns := 2 + 4*state.depth + 3
	for offset := 0; offset < len(state.buffer); offset += state.size {
		record := state.buffer[offset : offset+state.size]
		row := make(parquet.Row, 0, columns)
		put := func(value parquet.Value) { row = append(row, value.Level(0, 0, len(row))) }
		put(parquet.Int64Value(int64(binary.LittleEndian.Uint64(record[0:8])) + state.midnight))
		put(parquet.ByteArrayValue([]byte(eventName(record[26]))))
		levels := record[depthHeaderSize:]
		level := func(index int) (float64, int32) { // index counts (price, size) pairs: bids first, then asks
			return price(binary.LittleEndian.Uint32(levels[8*index:])), int32(binary.LittleEndian.Uint32(levels[8*index+4:]))
		}
		for k := 0; k < state.depth; k++ {
			bid, bidSize := level(k)
			ask, askSize := level(state.depth + k)
			put(parquet.DoubleValue(bid))
			put(parquet.Int32Value(bidSize))
			put(parquet.DoubleValue(ask))
			put(parquet.Int32Value(askSize))
		}
		put(parquet.DoubleValue(price(binary.LittleEndian.Uint32(record[16:20]))))
		put(parquet.Int32Value(int32(binary.LittleEndian.Uint32(record[20:24]))))
		put(parquet.Int64Value(int64(binary.LittleEndian.Uint64(record[8:16]))))
		chunk = append(chunk, row)
		if len(chunk) == decodeChunkRecords {
			if _, err := state.writer.WriteRows(chunk); err != nil {
				return err
			}
			chunk = chunk[:0]
		}
	}
	if len(chunk) > 0 {
		if _, err := state.writer.WriteRows(chunk); err != nil {
			return err
		}
	}
	state.buffer = state.buffer[:0]
	return state.writer.Flush()
}

func (state *bookSink) close() error {
	if err := state.writer.Close(); err != nil {
		return err
	}
	return state.file.Close()
}

func raiseOpenFileLimit() {
	var limit syscall.Rlimit
	if syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit) == nil && limit.Cur < limit.Max {
		limit.Cur = limit.Max
		_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limit)
	}
}

func readSymbols(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var symbols []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && len(line) <= 8 {
			symbols = append(symbols, line)
		}
	}
	return symbols, nil
}

// Ingest reads itch_tob records from input and writes outDir/<SYMBOL>/<day>.parquet for every symbol that has rows.
// kind tick reads top-of-book records, orderbook reads itch_tob --depth N records, stream reads itch_tob --events records.
func Ingest(day, symbolsPath, outDir, kind string, depth int, input io.Reader) (rows int64, symbolsWithData int, err error) {
	raiseOpenFileLimit()
	if !validKind(kind) {
		return 0, 0, fmt.Errorf("unknown kind %q", kind)
	}
	if kind == kindOrderbook && (depth < 1 || depth > maxDepth) {
		return 0, 0, fmt.Errorf("depth must be between 1 and %d", maxDepth)
	}
	symbols, err := readSymbols(symbolsPath)
	if err != nil {
		return 0, 0, err
	}
	midnight, err := midnightUnixNanos(day)
	if err != nil {
		return 0, 0, err
	}
	size, rowBytes := recordSize, 64 // rowBytes: memory per buffered row
	switch kind {
	case kindOrderbook:
		size = depthHeaderSize + 16*depth
		rowBytes = size
	case kindStream:
		size = eventRecordSize
	}
	maxBufferedRows := maxBufferedBytes / rowBytes
	writers := make(map[uint16]sink)
	buffered := 0

	open := func(index uint16) (sink, error) {
		directory := filepath.Join(outDir, symbols[index])
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, err
		}
		file, err := os.Create(filepath.Join(directory, day+".parquet"))
		if err != nil {
			return nil, err
		}
		compression := parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault})
		switch kind {
		case kindOrderbook:
			writer := parquet.NewWriter(file, schemaFor(kindOrderbook, depth), compression, parquet.PageBufferSize(16<<10))
			return &bookSink{file: file, writer: writer, depth: depth, size: size, midnight: midnight}, nil
		case kindStream:
			return newTypedSink(file, []parquet.WriterOption{compression, parquet.PageBufferSize(64 << 10)}, eventDecoder(midnight)), nil
		}
		return newTypedSink(file, []parquet.WriterOption{compression, parquet.PageBufferSize(64 << 10)}, tickDecoder(midnight)), nil
	}
	flush := func(state sink) error {
		count := state.rows()
		if err := state.flush(); err != nil {
			return err
		}
		buffered -= count
		return nil
	}

	symbolOffset := 40 // the sym field of the record
	if kind == kindOrderbook {
		symbolOffset = 24
	}
	reader := bufio.NewReaderSize(input, 4<<20)
	record := make([]byte, size)
	for {
		if _, err := io.ReadFull(reader, record); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return rows, len(writers), err
		}
		rows++
		index := binary.LittleEndian.Uint16(record[symbolOffset : symbolOffset+2])
		if int(index) >= len(symbols) {
			continue
		}
		state := writers[index]
		if state == nil {
			if state, err = open(index); err != nil {
				return rows, len(writers), err
			}
			writers[index] = state
		}
		state.add(record)
		buffered++
		if state.rows() >= flushRows {
			if err := flush(state); err != nil {
				return rows, len(writers), err
			}
		}
		if buffered > maxBufferedRows {
			for _, other := range writers {
				if err := flush(other); err != nil {
					return rows, len(writers), err
				}
			}
		}
	}
	for _, state := range writers {
		if err := flush(state); err != nil {
			return rows, len(writers), err
		}
		if err := state.close(); err != nil {
			return rows, len(writers), err
		}
	}
	fmt.Fprintf(os.Stderr, "ingest %s: %d rows, %d symbols with data\n", day, rows, len(writers))
	return rows, len(writers), nil
}
