package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
)

/*
type StockData struct {
	Ticker       string   `json:"ticker"`
	QueryCount   int      `json:"queryCount"`
	ResultsCount int      `json:"resultsCount"`
	Adjusted     bool     `json:"adjusted"`
	Results      []Result `json:"results"`
	Status       string   `json:"status"`
	RequestID    string   `json:"request_id"`
	Count        int      `json:"count"`
}

type Result struct {
	V  float64 `json:"v"`
	VW float64 `json:"vw"`
	O  float64 `json:"o"`
	C  float64 `json:"c"`
	H  float64 `json:"h"`
	L  float64 `json:"l"`
	T  int64   `json:"t"`
	N  int     `json:"n"`
}
*/

func main() {
	// Load .env File
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal(err)
	}

	// Define required Variable
	Api_Key := os.Getenv("Poly_API_Key")

	// Read Stock ticker list CSV
	stock_tickers, err := os.Open("stocks_list.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer stock_tickers.Close()

	reader := csv.NewReader(bufio.NewReader(stock_tickers))

	record, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// Polygon Client
	c := polygon.New(Api_Key)

	for _, rec := range record {
		// Create CSV file
		filename := fmt.Sprintf("%s_1hr_agg.csv", rec[0])
		agg_csv, err := os.Create(filename)
		if err != nil {
			log.Fatal(err)
		}
		defer agg_csv.Close()

		// Create a new writer.
		wr := csv.NewWriter(bufio.NewWriter(agg_csv))

		// Write the Column names in a CSV file
		column_data := []string{"Ticker", "Open Time", "Open", "High", "Low", "Close", "Volume"}
		wr.Write(column_data)

		// Set Parameters
		params := models.ListAggsParams{
			Ticker:     rec[0],
			Multiplier: 1,
			Timespan:   "hour",
			From:       models.Millis(time.Date(2019, 6, 3, 0, 0, 0, 0, time.UTC)),
			To:         models.Millis(time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)),
		}.WithOrder(models.Asc).WithLimit(50000).WithAdjusted(true)

		// Make Request
		iter := c.ListAggs(context.Background(), params)

		for iter.Next() {
			open := strconv.FormatFloat(iter.Item().Open, 'f', 2, 64)
			high := strconv.FormatFloat(iter.Item().High, 'f', 2, 64)
			low := strconv.FormatFloat(iter.Item().Low, 'f', 2, 64)
			close := strconv.FormatFloat(iter.Item().Close, 'f', 2, 64)
			volume := strconv.FormatFloat(iter.Item().Volume, 'f', 0, 64)
			timestamp := time.Time(iter.Item().Timestamp).UnixMilli() / 1000
			date := time.Unix(timestamp, 0).Format("2006-01-02 15:04:05")

			row := []string{rec[0], date, open, high, low, close, volume}

			wr.Write(row)
		}
		fmt.Printf("%s CSV created\n", rec[0])
	}

	/*
		// Get Api Response
		response, err := http.Get(url)
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()

		resp, err := io.ReadAll(response.Body)
		if err != nil {
			log.Fatal(err)
		}

		// Attempt to unmarshal the JSON response into the stock struct.
		var stock StockData
		err = json.Unmarshal([]byte(resp), &stock)
		if err != nil {
			fmt.Println("error:", err)
		}

		// Create CSV file
		file, err := os.Create("test.csv")
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		// Create a new writer.
		wr := csv.NewWriter(bufio.NewWriter(file))

		column_data := []string{"Ticker", "Open Time", "Open", "High", "Low", "Close", "Volume"}
		wr.Write(column_data)

		for i, arr := range stock.Results {
			open := strconv.FormatFloat(arr.O, 'f', 2, 64)
			high := strconv.FormatFloat(arr.H, 'f', 2, 64)
			low := strconv.FormatFloat(arr.L, 'f', 2, 64)
			close := strconv.FormatFloat(arr.C, 'f', 2, 64)
			volume := strconv.FormatFloat(arr.V, 'f', 0, 64)
			timestamp := arr.T / 1000
			date := time.Unix(timestamp, 0).Format("2006-01-02 15:04:05")

			row := []string{ticker, date, open, high, low, close, volume}
			wr.Write(row)
			fmt.Println(i)
		}

		wr.Flush()

		// fmt.Printf("%+v\n", stock)
	*/

	/*
		csvfile, err := os.Open("AAPL_Daily_Agg.csv")
		if err != nil {
			log.Fatal(err)
		}

		reader := csv.NewReader(bufio.NewReader(csvfile))
		rows, err := reader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}
		for i, row := range rows {
			for j := range row {
				fmt.Printf("%s", rows[i][j])
			}
		}
	*/

}
