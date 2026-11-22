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
	recordSize      = 43 // itch_tob Record: itch_ns, seq, bid_px, bid_sz, ask_px, ask_sz, trade_px, trade_sz (u32), sym u16, event u8
	flushRows       = 50000
	maxBufferedRows = 8000000
)

type symbolWriter struct {
	file   *os.File
	writer *parquet.GenericWriter[Tick]
	buffer []Tick
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
func Ingest(day, symbolsPath, outDir string, input io.Reader) (rows int64, symbolsWithData int, err error) {
	raiseOpenFileLimit()
	symbols, err := readSymbols(symbolsPath)
	if err != nil {
		return 0, 0, err
	}
	midnight, err := midnightUnixNanos(day)
	if err != nil {
		return 0, 0, err
	}
	writers := make(map[uint16]*symbolWriter)
	buffered := 0

	flush := func(index uint16, state *symbolWriter) error {
		if len(state.buffer) == 0 {
			return nil
		}
		if _, err := state.writer.Write(state.buffer); err != nil {
			return err
		}
		if err := state.writer.Flush(); err != nil { // one row group per flush
			return err
		}
		buffered -= len(state.buffer)
		state.buffer = state.buffer[:0]
		return nil
	}
	open := func(index uint16) (*symbolWriter, error) {
		directory := filepath.Join(outDir, symbols[index])
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return nil, err
		}
		file, err := os.Create(filepath.Join(directory, day+".parquet"))
		if err != nil {
			return nil, err
		}
		writer := parquet.NewGenericWriter[Tick](file, parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}), parquet.PageBufferSize(64<<10))
		return &symbolWriter{file: file, writer: writer}, nil
	}

	reader := bufio.NewReaderSize(input, 4<<20)
	record := make([]byte, recordSize)
	for {
		if _, err := io.ReadFull(reader, record); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return rows, len(writers), err
		}
		rows++
		index := binary.LittleEndian.Uint16(record[40:42])
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
		state.buffer = append(state.buffer, Tick{
			Timestamp:  int64(binary.LittleEndian.Uint64(record[0:8])) + midnight,
			Event:      eventName(record[42]),
			Bid:        price(binary.LittleEndian.Uint32(record[16:20])),
			BidSize:    int32(binary.LittleEndian.Uint32(record[20:24])),
			Ask:        price(binary.LittleEndian.Uint32(record[24:28])),
			AskSize:    int32(binary.LittleEndian.Uint32(record[28:32])),
			TradePrice: price(binary.LittleEndian.Uint32(record[32:36])),
			TradeSize:  int32(binary.LittleEndian.Uint32(record[36:40])),
			Seq:        binary.LittleEndian.Uint64(record[8:16]),
		})
		buffered++
		if len(state.buffer) >= flushRows {
			if err := flush(index, state); err != nil {
				return rows, len(writers), err
			}
		}
		if buffered > maxBufferedRows {
			for flushIndex, flushState := range writers {
				if err := flush(flushIndex, flushState); err != nil {
					return rows, len(writers), err
				}
			}
		}
	}
	for index, state := range writers {
		if err := flush(index, state); err != nil {
			return rows, len(writers), err
		}
		if err := state.writer.Close(); err != nil {
			return rows, len(writers), err
		}
		if err := state.file.Close(); err != nil {
			return rows, len(writers), err
		}
	}
	fmt.Fprintf(os.Stderr, "ingest %s: %d rows, %d symbols with data\n", day, rows, len(writers))
	return rows, len(writers), nil
}
