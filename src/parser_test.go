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
		directory(1, "AAA"), directory(2, "BBB"), directory(3, "CCC"), directory(4, "DDD"),
		add(1, "AAA", at(1), 1, 'B', 100, 100000),
		add(1, "AAA", at(2), 2, 'S', 200, 100500),
		add(1, "AAA", at(3), 3, 'B', 50, 100000),
		join(itchHeader('E', 1, at(4)), be64(1), be32(40), be64(1)),                // execute 40 of order 1
		join(itchHeader('D', 1, at(5)), be64(3)),                                   // delete order 3
		join(itchHeader('U', 1, at(6)), be64(2), be64(4), be32(150), be32(100400)), // replace order 2
		add(2, "BBB", at(7), 5, 'B', 10, 200000),
		join(itchHeader('P', 1, at(8)), be64(0), []byte{'B'}, be32(25), symbolField("AAA"), be32(100200), be64(7)), // non-displayed trade
		// DDD has two levels per side: changes below the touch are in the orderbook files but not in the tick files
		add(4, "DDD", at(9), 10, 'B', 10, 200000),
		add(4, "DDD", at(10), 11, 'B', 20, 199900),
		add(4, "DDD", at(11), 12, 'S', 5, 200100),
		add(4, "DDD", at(12), 13, 'S', 7, 200200),
		join(itchHeader('X', 4, at(13)), be64(11), be32(5)), // cancel 5 of the 20 at the second bid level
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

func runParser(t *testing.T, binary, mode, kind, outDir, symbolsFile string) string {
	t.Helper()
	command := exec.Command("bash", filepath.Join("..", "parsers", "parse_nasdaq_itch.sh"))
	command.Stdin = bytes.NewReader(syntheticPcap(7))
	command.Env = append(os.Environ(), "DFS_MODE="+mode, "DFS_DATE=2026-11-20", "DFS_PCAP_NAME=synthetic.pcap", "DFS_SYMBOLS_FILE="+symbolsFile,
		"DFS_OUT_DIR="+outDir, "DFS_PORT=30001", "DFS_KIND="+kind, "DFS_DEPTH=5", "DFS_INGEST="+binary+" ingest")
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
	os.WriteFile(symbolsFile, []byte("AAA\nBBB\nCCC\nDDD\n"), 0o644)

	if listed := strings.Fields(runParser(t, binary, "list", kindTick, "", symbolsFile)); strings.Join(listed, ",") != "AAA,BBB,CCC,DDD" {
		t.Fatalf("listed symbols %v", listed)
	}
	runParser(t, binary, "extract", kindTick, filepath.Join(directory, "out"), symbolsFile)
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

// readTable reads every row of a parquet file of any kind: the column names and, per row, the values by column name.
func readTable(t *testing.T, path string) ([]string, []map[string]parquet.Value) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, _ := file.Stat()
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, field := range parquetFile.Schema().Fields() {
		names = append(names, field.Name())
	}
	reader := parquet.NewReader(parquetFile)
	defer reader.Close()
	var table []map[string]parquet.Value
	batch := make([]parquet.Row, 64)
	for {
		count, err := reader.ReadRows(batch)
		for _, row := range batch[:count] {
			values := map[string]parquet.Value{}
			for index, value := range row {
				values[names[index]] = value.Clone()
			}
			table = append(table, values)
		}
		if err != nil {
			break
		}
	}
	return names, table
}

func TestParserOrderbookAndStream(t *testing.T) {
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
	os.WriteFile(symbolsFile, []byte("AAA\nBBB\nCCC\nDDD\n"), 0o644)
	day := "2026-11-20.parquet"

	// tick: DDD has two rows, the changes below the touch are not in it
	runParser(t, binary, "extract", kindTick, filepath.Join(directory, "tick"), symbolsFile)
	if ticks, err := parquet.ReadFile[Tick](filepath.Join(directory, "tick", "DDD", day)); err != nil || len(ticks) != 2 {
		t.Fatalf("DDD tick rows: %d, err %v, want 2", len(ticks), err)
	}

	// orderbook: a row for every change of the top 5 levels
	runParser(t, binary, "extract", kindOrderbook, filepath.Join(directory, "book"), symbolsFile)
	names, rows := readTable(t, filepath.Join(directory, "book", "DDD", day))
	if want := 2 + 4*5 + 3; len(names) != want || names[0] != "timestamp" || names[2] != "bid_1" || names[3] != "bid_size_1" || names[4] != "ask_1" || names[5] != "ask_size_1" ||
		names[6] != "bid_2" || names[len(names)-1] != "seq" {
		t.Fatalf("orderbook columns (%d): %v", len(names), names)
	}
	if len(rows) != 5 {
		t.Fatalf("DDD orderbook rows: %d, want 5", len(rows))
	}
	type levels struct{ bid1, bidSize1, bid2, bidSize2, ask1, askSize1, ask2, askSize2 float64 }
	want := []levels{
		{20, 10, none, 0, none, 0, none, 0},
		{20, 10, 19.99, 20, none, 0, none, 0},
		{20, 10, 19.99, 20, 20.01, 5, none, 0},
		{20, 10, 19.99, 20, 20.01, 5, 20.02, 7},
		{20, 10, 19.99, 15, 20.01, 5, 20.02, 7}, // below the touch: not a tick row
	}
	for index, row := range rows {
		got := levels{row["bid_1"].Double(), float64(row["bid_size_1"].Int32()), row["bid_2"].Double(), float64(row["bid_size_2"].Int32()),
			row["ask_1"].Double(), float64(row["ask_size_1"].Int32()), row["ask_2"].Double(), float64(row["ask_size_2"].Int32())}
		if !sameFloat(got.bid1, want[index].bid1) || got.bidSize1 != want[index].bidSize1 || !sameFloat(got.bid2, want[index].bid2) || got.bidSize2 != want[index].bidSize2 ||
			!sameFloat(got.ask1, want[index].ask1) || got.askSize1 != want[index].askSize1 || !sameFloat(got.ask2, want[index].ask2) || got.askSize2 != want[index].askSize2 {
			t.Errorf("DDD orderbook row %d: got %+v want %+v", index, got, want[index])
		}
		if row["event"].String() != "Q" || !math.IsNaN(row["bid_3"].Double()) || row["bid_size_3"].Int32() != 0 || !math.IsNaN(row["ask_5"].Double()) {
			t.Errorf("DDD orderbook row %d: event %s, deeper levels must be NaN and 0", index, row["event"].String())
		}
	}
	// AAA: the trades are in the orderbook file too, against the book as it was
	_, aaa := readTable(t, filepath.Join(directory, "book", "AAA", day))
	if len(aaa) != len(expectedAAA) || aaa[3]["event"].String() != "T" || aaa[3]["trade_size"].Int32() != 40 || aaa[3]["bid_size_1"].Int32() != 150 {
		t.Fatalf("AAA orderbook rows: %d, row 3 %v", len(aaa), aaa[3])
	}
	csvPath := filepath.Join(directory, "DDD.csv")
	if err := WriteCSV(filepath.Join(directory, "book", "DDD", day), csvPath); err != nil {
		t.Fatal(err)
	}
	content, _ := os.ReadFile(csvPath)
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	if !strings.HasPrefix(lines[0], "timestamp,event,bid_1,bid_size_1,ask_1,ask_size_1,bid_2,") || len(lines) != 6 ||
		!strings.HasSuffix(lines[5], ",Q,20,10,20.01,5,19.99,15,20.02,7,nan,0,nan,0,nan,0,nan,0,nan,0,nan,0,nan,0,17") {
		t.Fatalf("unexpected orderbook csv:\n%s", content)
	}

	// stream: every order message of AAA, as it came
	runParser(t, binary, "extract", kindStream, filepath.Join(directory, "stream"), symbolsFile)
	events, err := parquet.ReadFile[OrderEvent](filepath.Join(directory, "stream", "AAA", day))
	if err != nil {
		t.Fatal(err)
	}
	wantEvents := []OrderEvent{
		{Event: "A", Ref: 1, Side: "B", Price: 10.0, Shares: 100},
		{Event: "A", Ref: 2, Side: "S", Price: 10.05, Shares: 200},
		{Event: "A", Ref: 3, Side: "B", Price: 10.0, Shares: 50},
		{Event: "E", Ref: 1, Side: "B", Price: 10.0, Shares: 40},
		{Event: "D", Ref: 3, Side: "B", Price: 10.0, Shares: 50},
		{Event: "U", Ref: 2, NewRef: 4, Side: "S", Price: 10.04, Shares: 150},
		{Event: "P", Side: "B", Price: 10.02, Shares: 25},
	}
	if len(events) != len(wantEvents) {
		t.Fatalf("AAA stream: %d events, want %d: %+v", len(events), len(wantEvents), events)
	}
	for index, want := range wantEvents {
		got := events[index]
		if got.Event != want.Event || got.Ref != want.Ref || got.NewRef != want.NewRef || got.Side != want.Side || !sameFloat(got.Price, want.Price) || got.Shares != want.Shares {
			t.Errorf("AAA stream event %d: got %+v want %+v", index, got, want)
		}
	}
	ddd, err := parquet.ReadFile[OrderEvent](filepath.Join(directory, "stream", "DDD", day))
	if err != nil || len(ddd) != 5 || ddd[4].Event != "X" || ddd[4].Ref != 11 || ddd[4].Shares != 5 || ddd[4].Seq != 17 {
		t.Fatalf("DDD stream: %+v err %v", ddd, err)
	}
}
