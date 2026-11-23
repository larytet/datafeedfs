package main

import (
	"bufio"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

func formatFloat(value float64) string {
	if math.IsNaN(value) {
		return "nan"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

// WriteCSV renders a parquet file of any kind as CSV: the same columns, timestamps as UTC with nanoseconds, NaN as "nan".
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
	fields := parquetFile.Schema().Fields()
	unsigned := make([]bool, len(fields))
	names := make([]string, len(fields))
	for index, field := range fields {
		names[index] = field.Name()
		if logical := field.Type().LogicalType(); logical != nil {
			if integer, ok := logical.Value.(*format.IntType); ok {
				unsigned[index] = !integer.IsSigned
			}
		}
	}
	reader := parquet.NewReader(parquetFile)
	defer reader.Close()

	temporary := csvPath + ".tmp"
	output, err := os.Create(temporary)
	if err != nil {
		return err
	}
	defer os.Remove(temporary)
	writer := bufio.NewWriterSize(output, 1<<20)
	writer.WriteString(strings.Join(names, ",") + "\n")
	line := make([]byte, 0, 512)
	batch := make([]parquet.Row, 4096)
	for {
		count, readErr := reader.ReadRows(batch)
		for _, row := range batch[:count] {
			line = line[:0]
			for index, value := range row {
				if index > 0 {
					line = append(line, ',')
				}
				switch {
				case names[index] == "timestamp":
					line = time.Unix(0, value.Int64()).UTC().AppendFormat(line, "2006-01-02 15:04:05.000000000Z")
				case value.Kind() == parquet.Double:
					line = append(line, formatFloat(value.Double())...)
				case value.Kind() == parquet.Float:
					line = append(line, formatFloat(float64(value.Float()))...)
				case value.Kind() == parquet.ByteArray:
					line = append(line, value.ByteArray()...)
				case value.Kind() == parquet.Int32 && !unsigned[index]:
					line = strconv.AppendInt(line, int64(value.Int32()), 10)
				case unsigned[index]:
					line = strconv.AppendUint(line, value.Uint64(), 10)
				default:
					line = strconv.AppendInt(line, value.Int64(), 10)
				}
			}
			line = append(line, '\n')
			writer.Write(line)
		}
		if readErr != nil {
			if readErr != io.EOF {
				return readErr
			}
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
