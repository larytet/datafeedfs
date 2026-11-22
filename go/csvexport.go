package main

import (
	"bufio"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/parquet-go/parquet-go"
)

func formatFloat(value float64) string {
	if math.IsNaN(value) {
		return "nan"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

// WriteCSV renders a per-symbol parquet file as CSV (same columns, timestamps as UTC with nanoseconds).
func WriteCSV(parquetPath, csvPath string) error {
	file, err := os.Open(parquetPath)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	parquetFile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return err
	}
	reader := parquet.NewGenericReader[Tick](parquetFile)
	defer reader.Close()

	temporary := csvPath + ".tmp"
	output, err := os.Create(temporary)
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	writer := bufio.NewWriterSize(output, 1<<20)
	writer.WriteString("timestamp,event,bid,bid_size,ask,ask_size,trade_price,trade_size,seq\n")
	line := make([]byte, 0, 160)
	batch := make([]Tick, 8192)
	for {
		count, readErr := reader.Read(batch)
		for _, tick := range batch[:count] {
			line = time.Unix(0, tick.Timestamp).UTC().AppendFormat(line[:0], "2006-01-02 15:04:05.000000000Z")
			line = append(line, ',')
			line = append(line, tick.Event...)
			line = append(line, ',')
			line = append(line, formatFloat(tick.Bid)...)
			line = append(line, ',')
			line = strconv.AppendInt(line, int64(tick.BidSize), 10)
			line = append(line, ',')
			line = append(line, formatFloat(tick.Ask)...)
			line = append(line, ',')
			line = strconv.AppendInt(line, int64(tick.AskSize), 10)
			line = append(line, ',')
			line = append(line, formatFloat(tick.TradePrice)...)
			line = append(line, ',')
			line = strconv.AppendInt(line, int64(tick.TradeSize), 10)
			line = append(line, ',')
			line = strconv.AppendUint(line, tick.Seq, 10)
			line = append(line, '\n')
			writer.Write(line)
		}
		if readErr != nil {
			break
		}
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if err := output.Close(); err != nil {
		return err
	}
	return os.Rename(temporary, csvPath)
}
