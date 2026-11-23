package main

import (
	"fmt"
	"reflect"

	"github.com/parquet-go/parquet-go"
)

// Kinds of file the filesystem serves: /tick/<SYMBOL>/<day>.parquet, /orderbook/<SYMBOL>/<day>.parquet and /stream/<SYMBOL>/<day>.parquet.
const (
	kindTick      = "tick"
	kindOrderbook = "orderbook"
	kindStream    = "stream"

	defaultDepth = 5
	maxDepth     = 64 // the limit of itch_tob --depth
)

var kinds = []string{kindTick, kindOrderbook, kindStream}

func validKind(kind string) bool {
	return kind == kindTick || kind == kindOrderbook || kind == kindStream
}

// OrderEvent is one row of a stream file: an order message of the feed as it came, no book.
// event: A add, E execute at the order's price, C execute at another price, X cancel, D delete, U replace, P non-displayed trade, Q cross.
// ref is the order reference number (the old one for U, 0 for P and Q), new_ref the replacement's (U only). side is B or S (empty for Q).
// price: the order's price for A E X D U (the new price for U), the execution price for C, P and Q (NaN when absent).
// shares: added (A), executed (E, C), cancelled (X), remaining before the delete (D), new size (U), traded (P, Q).
type OrderEvent struct {
	Timestamp int64   `parquet:"timestamp,timestamp(nanosecond)"`
	Event     string  `parquet:"event,dict"`
	Seq       uint64  `parquet:"seq"`
	Ref       uint64  `parquet:"ref"`
	NewRef    uint64  `parquet:"new_ref"`
	Side      string  `parquet:"side,dict"`
	Price     float64 `parquet:"price"`
	Shares    int32   `parquet:"shares"`
}

// orderbookType builds the row type of an orderbook file with the given number of levels per side. Columns, in order:
//
//	timestamp, event, bid_1, bid_size_1, ask_1, ask_size_1, ..., bid_N, bid_size_N, ask_N, ask_size_N, trade_price, trade_size, seq
//
// Level 1 is the best price. A missing level has a NaN price and size 0. It is a struct type built at run time so that the column
// order is the order above (a parquet.Group would sort the columns by name).
func orderbookType(depth int) reflect.Type {
	field := func(name, column string, kind reflect.Type, options string) reflect.StructField {
		return reflect.StructField{Name: name, Type: kind, Tag: reflect.StructTag(fmt.Sprintf(`parquet:"%s%s"`, column, options))}
	}
	float, integer := reflect.TypeOf(float64(0)), reflect.TypeOf(int32(0))
	fields := []reflect.StructField{
		field("Timestamp", "timestamp", reflect.TypeOf(int64(0)), ",timestamp(nanosecond)"),
		field("Event", "event", reflect.TypeOf(""), ",dict"),
	}
	for level := 1; level <= depth; level++ {
		fields = append(fields,
			field(fmt.Sprintf("Bid%d", level), fmt.Sprintf("bid_%d", level), float, ""),
			field(fmt.Sprintf("BidSize%d", level), fmt.Sprintf("bid_size_%d", level), integer, ""),
			field(fmt.Sprintf("Ask%d", level), fmt.Sprintf("ask_%d", level), float, ""),
			field(fmt.Sprintf("AskSize%d", level), fmt.Sprintf("ask_size_%d", level), integer, ""))
	}
	fields = append(fields,
		field("TradePrice", "trade_price", float, ""),
		field("TradeSize", "trade_size", integer, ""),
		field("Seq", "seq", reflect.TypeOf(uint64(0)), ""))
	return reflect.StructOf(fields)
}

// schemaFor returns the parquet schema of a kind of file.
func schemaFor(kind string, depth int) *parquet.Schema {
	switch kind {
	case kindOrderbook:
		return parquet.SchemaOf(reflect.New(orderbookType(depth)).Elem().Interface())
	case kindStream:
		return parquet.SchemaOf(OrderEvent{})
	}
	return parquet.SchemaOf(Tick{})
}
