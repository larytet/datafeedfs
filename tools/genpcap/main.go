// genpcap writes a deterministic synthetic Nasdaq TotalView-ITCH 5.0 capture (classic pcap, MoldUDP64 over UDP) for the end-to-end test:
// several symbols whose books are built from adds, partial cancels, deletes, replaces, executions, non-displayed trades and a cross.
// The book never crosses. The same seed always gives the same bytes.
//
//	go run ./tools/genpcap -out day.pcap && xz -9 -T0 day.pcap
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
)

const (
	udpPort  = 26477
	sessionN = "TESTSESS01"
	tickSize = 100 // 1/10000 dollar: one cent
)

type order struct {
	ref    uint64
	side   byte
	price  uint32
	shares uint32
}

type symbol struct {
	name   string
	locate uint16
	mid    uint32
	orders []*order
}

type generator struct {
	random   *rand.Rand
	out      *bufio.Writer
	symbols  []*symbol
	nextRef  uint64
	match    uint64
	nanos    int64
	sequence uint64
	batch    [][]byte
	packets  int
}

func be16(value uint16) []byte { return binary.BigEndian.AppendUint16(nil, value) }
func be32(value uint32) []byte { return binary.BigEndian.AppendUint32(nil, value) }
func be64(value uint64) []byte { return binary.BigEndian.AppendUint64(nil, value) }
func join(parts ...[]byte) []byte {
	var joined []byte
	for _, part := range parts {
		joined = append(joined, part...)
	}
	return joined
}

func stock(name string) []byte { return []byte(name + strings.Repeat(" ", 8-len(name))) }

func (g *generator) header(kind byte, locate uint16) []byte {
	return join([]byte{kind}, be16(locate), be16(0), be64(uint64(g.nanos))[2:])
}

func (g *generator) writePacket(messages [][]byte) {
	mold := join([]byte(sessionN), be64(g.sequence+1), be16(uint16(len(messages))))
	for _, message := range messages {
		mold = append(mold, be16(uint16(len(message)))...)
		mold = append(mold, message...)
	}
	g.sequence += uint64(len(messages))
	udpLength := 8 + len(mold)
	udp := join([]byte{0x9c, 0x40}, be16(udpPort), be16(uint16(udpLength)), be16(0), mold)
	total := 20 + len(udp)
	ip := join([]byte{0x45, 0}, be16(uint16(total)), []byte{0, 0, 0x40, 0, 64, 17, 0, 0, 10, 0, 0, 1, 233, 54, 12, 111}, udp)
	frame := join([]byte{0x01, 0x00, 0x5e, 0x36, 0x0c, 0x6f, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x08, 0x00}, ip)
	seconds, micros := uint32(g.nanos/1_000_000_000), uint32(g.nanos%1_000_000_000/1000)
	record := binary.LittleEndian.AppendUint32(nil, seconds)
	record = binary.LittleEndian.AppendUint32(record, micros)
	record = binary.LittleEndian.AppendUint32(record, uint32(len(frame)))
	record = binary.LittleEndian.AppendUint32(record, uint32(len(frame)))
	g.out.Write(record)
	g.out.Write(frame)
	g.packets++
}

// emit queues a message; a packet carries one to eight of them.
func (g *generator) emit(message []byte) {
	g.batch = append(g.batch, message)
	if len(g.batch) >= 1+g.random.Intn(8) {
		g.flush()
	}
}

func (g *generator) flush() {
	if len(g.batch) > 0 {
		g.writePacket(g.batch)
		g.batch = nil
	}
}

func (s *symbol) best(side byte) (price uint32, found bool) {
	for _, candidate := range s.orders {
		if candidate.side != side {
			continue
		}
		if !found || (side == 'B' && candidate.price > price) || (side == 'S' && candidate.price < price) {
			price, found = candidate.price, true
		}
	}
	return
}

func (s *symbol) remove(index int) {
	s.orders[index] = s.orders[len(s.orders)-1]
	s.orders = s.orders[:len(s.orders)-1]
}

// addPrice picks a price near the mid that does not cross the opposite side.
func (g *generator) addPrice(s *symbol, side byte) uint32 {
	distance := uint32(1+g.random.ExpFloat64()*3) * tickSize
	if side == 'B' {
		price := s.mid - distance
		if ask, ok := s.best('S'); ok && price >= ask {
			price = ask - tickSize
		}
		return price
	}
	price := s.mid + distance
	if bid, ok := s.best('B'); ok && price <= bid {
		price = bid + tickSize
	}
	return price
}

func (g *generator) add(s *symbol) {
	side := byte('B')
	if g.random.Intn(2) == 0 {
		side = 'S'
	}
	g.nextRef++
	item := &order{ref: g.nextRef, side: side, price: g.addPrice(s, side), shares: uint32(1+g.random.Intn(20)) * 100}
	s.orders = append(s.orders, item)
	kind := byte('A')
	if g.random.Intn(10) == 0 {
		kind = 'F'
	}
	message := join(g.header(kind, s.locate), be64(item.ref), []byte{side}, be32(item.shares), stock(s.name), be32(item.price))
	if kind == 'F' {
		message = append(message, 'M', 'P', 'I', 'D')
	}
	g.emit(message)
}

func (g *generator) step() {
	s := g.symbols[g.random.Intn(len(g.symbols))]
	g.nanos += int64(20_000 + g.random.Intn(180_000))
	if g.random.Intn(400) == 0 { // the mid drifts by a cent
		s.mid = uint32(int64(s.mid) + int64(g.random.Intn(3)-1)*tickSize)
	}
	roll := g.random.Intn(100)
	if len(s.orders) < 30 || roll < 40 {
		g.add(s)
		return
	}
	index := g.random.Intn(len(s.orders))
	item := s.orders[index]
	switch {
	case roll < 55: // partial cancel
		cancel := uint32(1+g.random.Intn(int(item.shares/100))) * 100
		if cancel >= item.shares {
			cancel = item.shares - 100
		}
		if cancel == 0 {
			g.add(s)
			return
		}
		item.shares -= cancel
		g.emit(join(g.header('X', s.locate), be64(item.ref), be32(cancel)))
	case roll < 72: // delete
		g.emit(join(g.header('D', s.locate), be64(item.ref)))
		s.remove(index)
	case roll < 84: // replace: new reference, new size and a nearby price that does not cross
		g.nextRef++
		price := item.price
		if g.random.Intn(2) == 0 {
			price = g.addPrice(s, item.side)
		}
		shares := uint32(1+g.random.Intn(20)) * 100
		g.emit(join(g.header('U', s.locate), be64(item.ref), be64(g.nextRef), be32(shares), be32(price)))
		item.ref, item.price, item.shares = g.nextRef, price, shares
	case roll < 94: // execution of an order at the best price of its side
		if best, _ := s.best(item.side); best != item.price {
			g.add(s)
			return
		}
		executed := uint32(1+g.random.Intn(int(item.shares/100))) * 100
		g.match++
		if executed >= item.shares {
			executed = item.shares
		}
		kind, price := byte('E'), item.price
		message := join(g.header('E', s.locate), be64(item.ref), be32(executed), be64(g.match))
		if g.random.Intn(5) == 0 { // executed with price: printable, at the order's price
			kind = 'C'
			message = join(g.header('C', s.locate), be64(item.ref), be32(executed), be64(g.match), []byte{'Y'}, be32(price))
		}
		_ = kind
		g.emit(message)
		item.shares -= executed
		if item.shares == 0 {
			s.remove(index)
		}
	case roll < 99: // non-displayed trade
		g.match++
		side := byte('B')
		if g.random.Intn(2) == 0 {
			side = 'S'
		}
		g.emit(join(g.header('P', s.locate), be64(0), []byte{side}, be32(uint32(1+g.random.Intn(10))*100), stock(s.name), be32(s.mid), be64(g.match)))
	default: // cross
		g.match++
		g.emit(join(g.header('Q', s.locate), be64(uint64(1000+g.random.Intn(100000))), stock(s.name), be32(s.mid), be64(g.match), []byte{'O'}))
	}
}

func main() {
	output := flag.String("out", "", "output pcap path (required)")
	seed := flag.Int64("seed", 1, "random seed")
	messages := flag.Int("messages", 600000, "number of order messages after the stock directory")
	names := flag.String("symbols", "AAPL,MSFT,NVDA,SPY,QQQ,TSLA,AMD,META", "comma separated symbols")
	flag.Parse()
	if *output == "" {
		fmt.Fprintln(os.Stderr, "usage: genpcap -out day.pcap [-seed N] [-messages N] [-symbols A,B,C]")
		os.Exit(2)
	}
	file, err := os.Create(*output)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer file.Close()
	g := &generator{random: rand.New(rand.NewSource(*seed)), out: bufio.NewWriterSize(file, 1<<20), nanos: int64(9*3600+30*60) * 1_000_000_000}
	g.out.Write(binary.LittleEndian.AppendUint32(nil, 0xa1b2c3d4))
	g.out.Write([]byte{2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	g.out.Write(binary.LittleEndian.AppendUint32(nil, 65535))
	g.out.Write(binary.LittleEndian.AppendUint32(nil, 1))
	for index, name := range strings.Split(*names, ",") {
		g.symbols = append(g.symbols, &symbol{name: name, locate: uint16(index + 1), mid: uint32(20000+g.random.Intn(60000)) / tickSize * tickSize})
	}
	for _, s := range g.symbols {
		g.emit(join(g.header('R', s.locate), stock(s.name), make([]byte, 20)))
	}
	g.flush()
	for i := 0; i < *messages; i++ {
		g.step()
	}
	g.flush()
	if err := g.out.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "genpcap: %d symbols, %d messages in %d packets\n", len(g.symbols), g.sequence, g.packets)
}
