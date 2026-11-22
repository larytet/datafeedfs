package main

import (
	"bytes"
	"encoding/binary"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// A tiny pcap of Nasdaq ITCH 5.0 messages in MoldUDP64 packets (one message per packet) with a known order-book story.

const (
	syntheticPort = 30001
	openNanos     = int64(9*3600+30*60) * 1_000_000_000 // 09:30:00 exchange time
)

func itchHeader(kind byte, locate uint16, nanos int64) []byte {
	header := []byte{kind, byte(locate >> 8), byte(locate), 0, 0}
	stamp := make([]byte, 8)
	binary.BigEndian.PutUint64(stamp, uint64(nanos))
	return append(header, stamp[2:]...)
}

func symbolField(symbol string) []byte { return []byte(symbol + strings.Repeat(" ", 8-len(symbol))) }

func be32(value uint32) []byte { return binary.BigEndian.AppendUint32(nil, value) }
func be64(value uint64) []byte { return binary.BigEndian.AppendUint64(nil, value) }

func join(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

func syntheticStory() [][]byte {
	at := func(step int64) int64 { return openNanos + step*1_000_000 } // one millisecond per step
	directory := func(locate uint16, symbol string) []byte {
		return join(itchHeader('R', locate, 0), symbolField(symbol), make([]byte, 20))
	}
	add := func(locate uint16, symbol string, nanos int64, reference uint64, side byte, shares, price uint32) []byte {
		return join(itchHeader('A', locate, nanos), be64(reference), []byte{side}, be32(shares), symbolField(symbol), be32(price))
	}
	return [][]byte{
		directory(1, "AAA"), directory(2, "BBB"), directory(3, "CCC"),
		add(1, "AAA", at(1), 1, 'B', 100, 100000),
		add(1, "AAA", at(2), 2, 'S', 200, 100500),
		add(1, "AAA", at(3), 3, 'B', 50, 100000),
		join(itchHeader('E', 1, at(4)), be64(1), be32(40), be64(1)),                // execute 40 of order 1
		join(itchHeader('D', 1, at(5)), be64(3)),                                   // delete order 3
		join(itchHeader('U', 1, at(6)), be64(2), be64(4), be32(150), be32(100400)), // replace order 2
		add(2, "BBB", at(7), 5, 'B', 10, 200000),
		join(itchHeader('P', 1, at(8)), be64(0), []byte{'B'}, be32(25), symbolField("AAA"), be32(100200), be64(7)), // non-displayed trade
	}
}

func syntheticPcap(duplicateSequence int) []byte {
	pcap := binary.LittleEndian.AppendUint32(nil, 0xa1b2c3d4)
	pcap = append(pcap, 2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	pcap = binary.LittleEndian.AppendUint32(pcap, 65535)
	pcap = binary.LittleEndian.AppendUint32(pcap, 1)
	packet := func(sequence int, message []byte, second uint32) []byte {
		mold := join([]byte("TESTSESS01"), be64(uint64(sequence)), []byte{0, 1, byte(len(message) >> 8), byte(len(message))}, message)
		udp := join([]byte{0x9c, 0x40, byte(syntheticPort / 256), byte(syntheticPort % 256)}, []byte{byte((8 + len(mold)) >> 8), byte(8 + len(mold)), 0, 0}, mold)
		total := 20 + len(udp)
		ip := join([]byte{0x45, 0, byte(total >> 8), byte(total), 0, 0, 0x40, 0, 64, 17, 0, 0, 10, 0, 0, 1, 233, 54, 12, 111}, udp)
		frame := join([]byte{0x01, 0x00, 0x5e, 0x36, 0x0c, 0x6f, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x08, 0x00}, ip)
		record := binary.LittleEndian.AppendUint32(nil, second)
		record = binary.LittleEndian.AppendUint32(record, 0)
		record = binary.LittleEndian.AppendUint32(record, uint32(len(frame)))
		record = binary.LittleEndian.AppendUint32(record, uint32(len(frame)))
		return append(record, frame...)
	}
	for index, message := range syntheticStory() {
		sequence := index + 1
		pcap = append(pcap, packet(sequence, message, uint32(1+index))...)
		if sequence == duplicateSequence { // a retransmitted packet: must be dropped by sequence number
			pcap = append(pcap, packet(sequence, message, uint32(1+index))...)
		}
	}
	return pcap
}

type expectedRow struct {
	event      string
	bid        float64
	bidSize    int32
	ask        float64
	askSize    int32
	tradePrice float64
	tradeSize  int32
}

var none = math.NaN()

var expectedAAA = []expectedRow{
	{"Q", 10.0, 100, none, 0, none, 0},
	{"Q", 10.0, 100, 10.05, 200, none, 0},
	{"Q", 10.0, 150, 10.05, 200, none, 0},
	{"T", 10.0, 150, 10.05, 200, 10.0, 40}, // the trade is reported against the book as it was
	{"Q", 10.0, 110, 10.05, 200, none, 0},
	{"Q", 10.0, 60, 10.05, 200, none, 0},
	{"Q", 10.0, 60, 10.04, 150, none, 0},
	{"T", 10.0, 60, 10.04, 150, 10.02, 25},
}

func sameFloat(left, right float64) bool {
	return (math.IsNaN(left) && math.IsNaN(right)) || math.Abs(left-right) < 1e-9
}

func runParser(t *testing.T, binary, mode, outDir, symbolsFile string) string {
	t.Helper()
	command := exec.Command("bash", filepath.Join("parsers", "parse_nasdaq_itch.sh"))
	command.Stdin = bytes.NewReader(syntheticPcap(7))
	command.Env = append(os.Environ(), "DFS_MODE="+mode, "DFS_DATE=2026-11-20", "DFS_PCAP_NAME=synthetic.pcap", "DFS_SYMBOLS_FILE="+symbolsFile,
		"DFS_OUT_DIR="+outDir, "DFS_PORT=30001", "DFS_INGEST="+binary+" ingest")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("parser (%s): %v", mode, err)
	}
	return string(output)
}

func TestParserEndToEnd(t *testing.T) {
	for _, tool := range []string{"gcc", "bash"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is required", tool)
		}
	}
	directory := t.TempDir()
	binary := filepath.Join(directory, "datafeedfs-go")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, output)
	}
	symbolsFile := filepath.Join(directory, "symbols.txt")
	os.WriteFile(symbolsFile, []byte("AAA\nBBB\nCCC\n"), 0o644)

	if listed := strings.Fields(runParser(t, binary, "list", "", symbolsFile)); strings.Join(listed, ",") != "AAA,BBB,CCC" {
		t.Fatalf("listed symbols %v", listed)
	}
	runParser(t, binary, "extract", filepath.Join(directory, "out"), symbolsFile)
	ticks, err := parquet.ReadFile[Tick](filepath.Join(directory, "out", "AAA", "2026-11-20.parquet"))
	if err != nil || len(ticks) != len(expectedAAA) {
		t.Fatalf("AAA: %d ticks, err %v, want %d", len(ticks), err, len(expectedAAA))
	}
	for index, want := range expectedAAA {
		got := ticks[index]
		if got.Event != want.event || !sameFloat(got.Bid, want.bid) || got.BidSize != want.bidSize || !sameFloat(got.Ask, want.ask) ||
			got.AskSize != want.askSize || !sameFloat(got.TradePrice, want.tradePrice) || got.TradeSize != want.tradeSize {
			t.Errorf("AAA row %d: got %+v want %+v", index, got, want)
		}
	}
	bbb, err := parquet.ReadFile[Tick](filepath.Join(directory, "out", "BBB", "2026-11-20.parquet"))
	if err != nil || len(bbb) != 1 || bbb[0].Bid != 20.0 || bbb[0].BidSize != 10 || !math.IsNaN(bbb[0].Ask) {
		t.Fatalf("BBB: %+v err %v", bbb, err)
	}
	if _, err := os.Stat(filepath.Join(directory, "out", "CCC")); err == nil {
		t.Fatal("CCC has no activity and must not get a file")
	}
}
