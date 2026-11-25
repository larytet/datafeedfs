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
)

// A synthetic Cboe PITCH capture, laid out as the PITCH spec describes it (little-endian, Sequenced Unit Header, length-led messages), run through
// parsers/itch_tob.c --protocol pitch. It checks the decoder against this layout; it cannot confirm the layout against a real Cboe capture.

func le(width int, value uint64) []byte {
	out := make([]byte, 8)
	binary.LittleEndian.PutUint64(out, value)
	return out[:width]
}

func sym(name string, width int) []byte {
	return append([]byte(name), bytes.Repeat([]byte(" "), width-len(name))...)
}

func pmsg(kind byte, parts ...[]byte) []byte {
	body := append([]byte{kind}, bytes.Join(parts, nil)...)
	return append([]byte{byte(len(body) + 1)}, body...)
}

func addLong(offset uint32, id uint64, side byte, shares uint32, name string, price uint64) []byte {
	return pmsg(0x21, le(4, uint64(offset)), le(8, id), []byte{side}, le(4, uint64(shares)), sym(name, 6), le(8, price), []byte{0})
}

func addShort(offset uint32, id uint64, side byte, shares uint16, name string, cents uint16) []byte {
	return pmsg(0x22, le(4, uint64(offset)), le(8, id), []byte{side}, le(2, uint64(shares)), sym(name, 6), le(2, uint64(cents)), []byte{0})
}

func pitchPacket(port uint16, unit byte, sequence uint32, messages ...[]byte) []byte {
	payload := bytes.Join(messages, nil)
	unitHeader := append(append(le(2, uint64(8+len(payload))), byte(len(messages)), unit), le(4, uint64(sequence))...)
	udp := append(binary.BigEndian.AppendUint16(binary.BigEndian.AppendUint16(nil, 1234), port), 0, 0, 0, 0)
	udp = append(udp, unitHeader...)
	udp = append(udp, payload...)
	ip := append([]byte{0x45, 0, 0, 0, 0, 0, 0, 0, 64, 17, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8}, udp...)
	frame := append(append(bytes.Repeat([]byte{0}, 12), 0x08, 0x00), ip...)
	record := binary.LittleEndian.AppendUint32(nil, 1)
	record = binary.LittleEndian.AppendUint32(record, 0)
	record = binary.LittleEndian.AppendUint32(record, uint32(len(frame)))
	record = binary.LittleEndian.AppendUint32(record, uint32(len(frame)))
	return append(record, frame...)
}

func TestPitchDecoder(t *testing.T) {
	if _, err := exec.LookPath("gcc"); err != nil {
		t.Skip("no gcc")
	}
	dir := t.TempDir()
	binary_ := filepath.Join(dir, "itch_tob")
	if out, err := exec.Command("gcc", "-O2", "-o", binary_, filepath.Join("..", "parsers", "itch_tob.c")).CombinedOutput(); err != nil {
		t.Fatalf("gcc: %v\n%s", err, out)
	}
	const port = 30101
	timeMsg := pmsg(0x20, le(4, 34200))
	pcap := []byte{0xd4, 0xc3, 0xb2, 0xa1, 2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0, 1, 0, 0, 0}
	first := pitchPacket(port, 1, 100, timeMsg,
		addLong(10, 1, 'B', 100, "AAA", 100000),
		addShort(20, 2, 'S', 50, "AAA", 1005),
		addLong(30, 9, 'B', 7, "ZZZ", 50000)) // not requested
	pcap = append(pcap, first...)
	pcap = append(pcap, first...) // the same packet again, as the B feed would deliver it
	pcap = append(pcap, pitchPacket(port, 1, 104,
		pmsg(0x27, le(4, 40), le(8, 1), le(4, 200), le(8, 100100), []byte{0}),                                 // modify: 200 @ 10.0100
		pmsg(0x23, le(4, 50), le(8, 2), le(4, 30), le(8, 777)),                                                // executed 30 of the ask
		pmsg(0x24, le(4, 60), le(8, 1), le(4, 40), le(4, 160), le(8, 778), le(8, 100050)),                     // 40 at 10.0050, 160 left
		pmsg(0x26, le(4, 70), le(8, 2), le(2, 5)),                                                             // short reduce: ask 20 -> 15
		pmsg(0x29, le(4, 80), le(8, 1)),                                                                       // delete the bid
		pmsg(0x2A, le(4, 90), le(8, 0), []byte{'B'}, le(4, 25), sym("AAA", 6), le(8, 100200), le(8, 779)))...) // non-displayed trade
	pcapPath := filepath.Join(dir, "day.pcap")
	os.WriteFile(pcapPath, pcap, 0o644)
	symbols := filepath.Join(dir, "symbols.txt")
	os.WriteFile(symbols, []byte("AAA\n"), 0o644)

	run := func(args ...string) []byte {
		command := exec.Command(binary_, append(args, "--protocol", "pitch", "--port", "30101")...)
		input, _ := os.Open(pcapPath)
		defer input.Close()
		command.Stdin = input
		out, err := command.Output()
		if err != nil {
			t.Fatalf("itch_tob %v: %v", args, err)
		}
		return out
	}
	if listed := strings.Fields(string(run("--list-symbols"))); strings.Join(listed, ",") != "AAA,ZZZ" {
		t.Fatalf("listed %v", listed)
	}
	// the same through the parser script: the contract of datafeedfs (list mode)
	script := exec.Command(filepath.Join("..", "parsers", "parse_cboe_pitch.sh"))
	script.Env = append(os.Environ(), "DFS_MODE=list", "DFS_PCAP_NAME=day.pcap", "DFS_PORT=30101")
	script.Stdin, _ = os.Open(pcapPath)
	if listed, err := script.Output(); err != nil || strings.Join(strings.Fields(string(listed)), ",") != "AAA,ZZZ" {
		t.Fatalf("parse_cboe_pitch.sh list: %q %v", listed, err)
	}
	out := run("--symbols-file", symbols, "--out", "-")
	if len(out)%recordSize != 0 {
		t.Fatalf("output is %d bytes", len(out))
	}
	type row struct {
		event                      byte
		bid, bidSize, ask, askSize float64
		trade                      float64
		tradeSize                  uint32
	}
	var got []row
	for chunk := out; len(chunk) > 0; chunk = chunk[recordSize:] {
		field := func(i int) float64 { return float64(binary.LittleEndian.Uint32(chunk[16+4*i:])) / 10000 }
		got = append(got, row{chunk[42], field(0), field(1) * 10000, field(2), field(3) * 10000, field(4), uint32(field(5) * 10000)})
	}
	want := []row{
		{'Q', 10, 100, 0, 0, 0, 0},
		{'Q', 10, 100, 10.05, 50, 0, 0},
		{'Q', 10.01, 200, 10.05, 50, 0, 0},      // modify
		{'T', 10.01, 200, 10.05, 50, 10.05, 30}, // execution at the order's price, book as before
		{'Q', 10.01, 200, 10.05, 20, 0, 0},
		{'T', 10.01, 200, 10.05, 20, 10.005, 40}, // execution at another price
		{'Q', 10.01, 160, 10.05, 20, 0, 0},
		{'Q', 10.01, 160, 10.05, 15, 0, 0}, // reduce
		{'Q', 0, 0, 10.05, 15, 0, 0},       // delete
		{'T', 0, 0, 10.05, 15, 10.02, 25},  // non-displayed trade
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.event != w.event || math.Abs(g.bid-w.bid) > 1e-9 || g.bidSize != w.bidSize || math.Abs(g.ask-w.ask) > 1e-9 || g.askSize != w.askSize || math.Abs(g.trade-w.trade) > 1e-9 || g.tradeSize != w.tradeSize {
			t.Errorf("row %d: got %+v want %+v", i, g, w)
		}
	}
	if seq := binary.LittleEndian.Uint64(out[8:]); seq != 1<<32|101 {
		t.Errorf("first row seq %#x, want unit 1 sequence 101", seq)
	}
}
