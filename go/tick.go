package main

import (
	"math"
	"time"
	_ "time/tzdata" // the container has no tzdata; the day boundary is New York midnight
)

// Tick is one row of a per-symbol parquet file.
// Prices are NaN when absent (no bid, no ask, no trade).
type Tick struct {
	Timestamp  int64   `parquet:"timestamp,timestamp(nanosecond)"`
	Event      string  `parquet:"event,dict"`
	Bid        float64 `parquet:"bid"`
	BidSize    int32   `parquet:"bid_size"`
	Ask        float64 `parquet:"ask"`
	AskSize    int32   `parquet:"ask_size"`
	TradePrice float64 `parquet:"trade_price"`
	TradeSize  int32   `parquet:"trade_size"`
	Seq        uint64  `parquet:"seq"`
}

const priceScale = 10000

func price(raw uint32) float64 {
	if raw == 0 {
		return math.NaN()
	}
	return float64(raw) / priceScale
}

func eventName(code byte) string {
	switch code {
	case 'T':
		return "T"
	case 'X':
		return "X"
	}
	return "Q"
}

// midnightUnixNanos is midnight of the trading day in America/New_York as UTC unix nanoseconds.
func midnightUnixNanos(day string) (int64, error) {
	location, err := time.LoadLocation("America/New_York")
	if err != nil {
		return 0, err
	}
	parsed, err := time.ParseInLocation("2006-01-02", day, location)
	if err != nil {
		return 0, err
	}
	return parsed.UnixNano(), nil
}
