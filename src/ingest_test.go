package main

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func record(itchNanos, seq uint64, bid, bidSize, ask, askSize, trade, tradeSize uint32, symbol uint16, event byte) []byte {
	buffer := make([]byte, recordSize)
	binary.LittleEndian.PutUint64(buffer[0:], itchNanos)
	binary.LittleEndian.PutUint64(buffer[8:], seq)
	for offset, value := range []uint32{bid, bidSize, ask, askSize, trade, tradeSize} {
		binary.LittleEndian.PutUint32(buffer[16+4*offset:], value)
	}
	binary.LittleEndian.PutUint16(buffer[40:], symbol)
	buffer[42] = event
	return buffer
}

func TestIngestAndCSV(t *testing.T) {
	directory := t.TempDir()
	symbolsFile := filepath.Join(directory, "symbols.txt")
	os.WriteFile(symbolsFile, []byte("AAA\nBBB\nCCC\n"), 0o644)
	var input bytes.Buffer
	input.Write(record(1_000, 1, 1000000, 100, 0, 0, 0, 0, 0, 'Q'))
	input.Write(record(2_000, 2, 1000000, 100, 1005000, 200, 1002000, 25, 0, 'T'))
	input.Write(record(3_000, 3, 2000000, 10, 0, 0, 0, 0, 1, 'Q'))
	rows, symbols, err := Ingest("2026-11-20", symbolsFile, directory, kindTick, 0, &input)
	if err != nil || rows != 3 || symbols != 2 {
		t.Fatalf("rows=%d symbols=%d err=%v", rows, symbols, err)
	}
	if _, err := os.Stat(filepath.Join(directory, "CCC")); err == nil {
		t.Fatal("symbol without rows must not get a file")
	}
	ticks, err := parquet.ReadFile[Tick](filepath.Join(directory, "AAA", "2026-11-20.parquet"))
	if err != nil || len(ticks) != 2 {
		t.Fatalf("ticks=%v err=%v", ticks, err)
	}
	midnight, _ := midnightUnixNanos("2026-11-20")
	if ticks[0].Timestamp != midnight+1000 || ticks[0].Bid != 100.0 || !math.IsNaN(ticks[0].Ask) || ticks[1].Event != "T" || ticks[1].TradePrice != 100.2 || ticks[1].TradeSize != 25 {
		t.Fatalf("unexpected ticks %+v", ticks)
	}
	csvPath := filepath.Join(directory, "AAA.csv")
	if err := WriteCSV(filepath.Join(directory, "AAA", "2026-11-20.parquet"), csvPath); err != nil {
		t.Fatal(err)
	}
	content, _ := os.ReadFile(csvPath)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if len(lines) != 3 || !strings.HasSuffix(lines[2], ",T,100,100,100.5,200,100.2,25,2") {
		t.Fatalf("unexpected csv:\n%s", content)
	}
}
