package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	client "github.com/influxdata/influxdb/client/v2"
)

var (
	broker        string
	consumerGroup string
	topics        []string
)

type signalMessage struct {
	Value string `json:"signal"`
	At    string `json:"at"`
}

type macdMessage struct {
	Macd       string `json:"macd"`
	MacdSignal string `json:"macd_signal"`
	At         string `json:"at"`
}

type quoteMessage struct {
	Quote string `json:"quote"`
	At    string `json:"at"`
}

type portfolioMessage struct {
	BuyingPower string `json:"buying_power"`
	Equity      string `json:"equity"`
	At          string `json:"at"`
}

type positionsMessage struct {
	Positions []struct {
		AverageBuyPrice string `json:"average_buy_price"`
		CurrentQuote    string `json:"current_quote"`
		Name            string `json:"name"`
		Quantity        string `json:"quantity"`
		Symbol          string `json:"symbol"`
	} `json:"positions"`
	At string `json:"at"`
}

func main() {
	broker = os.Getenv("KAFKA_ENDPOINT")
	topics = strings.Split(os.Getenv("KAFKA_CONSUMER_TOPICS"), ",")
	consumerGroup = os.Getenv("KAFKA_CONSUMER_GROUP")

	brokers := []string{broker}
	config := cluster.NewConfig()

	for {
		consumer, err := cluster.NewConsumer(brokers, consumerGroup, topics, config)
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer consumer.Close()

		// Create a new HTTPClient
		influx, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     os.Getenv("INFLUXDB_ENDPOINT"),
			Username: os.Getenv("INFLUXDB_USERNAME"),
			Password: os.Getenv("INFLUXDB_PASSWORD"),
		})
		if err != nil {
			fmt.Println(err)
			continue
		}
		defer influx.Close()

		for {
			select {
			case message, ok := <-consumer.Messages():
				if ok {
					symbol := string(message.Key)
					topic := message.Topic
					fmt.Println(topic, "->", symbol, "->", string(message.Value))

					// Create a new point batch
					batch, err := client.NewBatchPoints(client.BatchPointsConfig{
						Database:  os.Getenv("INFLUXDB_DATABASE"),
						Precision: "s",
					})
					if err != nil {
						log.Fatal(err)
					}

					switch topic {
					case "equity-quotes":
						equityQuote := quoteMessage{}

						err = json.Unmarshal(message.Value, &equityQuote)
						if err != nil {
							fmt.Println(err)
							continue
						}

						timestamp, err := time.Parse("2006-01-02 15:04:05 -0700", equityQuote.At)
						if err != nil {
							fmt.Println(err)
							continue
						}

						quote, err := strconv.ParseFloat(equityQuote.Quote, 32)
						if err != nil {
							fmt.Println(err)
							continue
						}

						// Create a point and add to batch
						tags := map[string]string{"symbol": symbol}
						fields := map[string]interface{}{
							"quote": quote,
						}

						point, err := client.NewPoint("quotes", tags, fields, timestamp)
						if err != nil {
							fmt.Println(err)
							continue
						}
						batch.AddPoint(point)

						// Write the batch
						err = influx.Write(batch)
						if err != nil {
							fmt.Println(err)
							continue
						}
					case "equity-signals":
						equitySignal := signalMessage{}

						err = json.Unmarshal(message.Value, &equitySignal)
						if err != nil {
							fmt.Println(err)
							continue
						}

						timestamp, err := time.Parse("2006-01-02 15:04:05 -0700", equitySignal.At)
						if err != nil {
							fmt.Println(err)
							continue
						}

						signal := equitySignal.Value

						// Create a point and add to batch
						tags := map[string]string{"symbol": symbol}
						fields := map[string]interface{}{
							"value": signal,
						}

						point, err := client.NewPoint("signals", tags, fields, timestamp)
						if err != nil {
							fmt.Println(err)
							continue
						}
						batch.AddPoint(point)

						// Write the batch
						err = influx.Write(batch)
						if err != nil {
							fmt.Println(err)
							continue
						}
					case "macd-stats":
						stat := macdMessage{}

						err = json.Unmarshal(message.Value, &stat)
						if err != nil {
							fmt.Println(err)
							continue
						}

						timestamp, err := time.Parse("2006-01-02 15:04:05 -0700", stat.At)
						if err != nil {
							fmt.Println(err)
							continue
						}

						macd, err := strconv.ParseFloat(stat.Macd, 32)
						if err != nil {
							fmt.Println(err)
							continue
						}

						signal, err := strconv.ParseFloat(stat.MacdSignal, 32)
						if err != nil {
							fmt.Println(err)
							continue
						}

						// Create a point and add to batch
						tags := map[string]string{"symbol": symbol}
						fields := map[string]interface{}{
							"value":  macd,
							"signal": signal,
						}

						point, err := client.NewPoint("macd", tags, fields, timestamp)
						if err != nil {
							fmt.Println(err)
							continue
						}
						batch.AddPoint(point)

						// Write the batch
						err = influx.Write(batch)
						if err != nil {
							fmt.Println(err)
							continue
						}

					case "portfolio-positions":
						positionsMessage := positionsMessage{}

						err = json.Unmarshal(message.Value, &positionsMessage)
						if err != nil {
							fmt.Println(err)
							continue
						}

						timestamp, err := time.Parse("2006-01-02 15:04:05 -0700", positionsMessage.At)
						if err != nil {
							fmt.Println(err)
							continue
						}

						for _, position := range positionsMessage.Positions {
							buyPrice, err := strconv.ParseFloat(position.AverageBuyPrice, 32)
							if err != nil {
								fmt.Println(err)
								continue
							}

							quote, err := strconv.ParseFloat(position.CurrentQuote, 32)
							if err != nil {
								fmt.Println(err)
								continue
							}

							quantity, err := strconv.ParseFloat(position.Quantity, 32)
							if err != nil {
								fmt.Println(err)
								continue
							}

							// Create a point and add to batch
							tags := map[string]string{"symbol": position.Symbol}
							fields := map[string]interface{}{
								"buy_price": buyPrice,
								"quote":     quote,
								"quantity":  quantity,
							}

							point, err := client.NewPoint("positions", tags, fields, timestamp)
							if err != nil {
								fmt.Println(err)
								continue
							}
							batch.AddPoint(point)

							// Write the batch
							err = influx.Write(batch)
							if err != nil {
								fmt.Println(err)
								continue
							}
						}

					case "portfolio-stats":
						stat := portfolioMessage{}

						err = json.Unmarshal(message.Value, &stat)
						if err != nil {
							fmt.Println(err)
							continue
						}

						timestamp, err := time.Parse("2006-01-02 15:04:05 -0700", stat.At)
						if err != nil {
							fmt.Println(err)
							continue
						}

						buyingPower, err := strconv.ParseFloat(stat.BuyingPower, 32)
						if err != nil {
							fmt.Println(err)
							continue
						}

						equity, err := strconv.ParseFloat(stat.Equity, 32)
						if err != nil {
							fmt.Println(err)
							continue
						}

						// Create a point and add to batch
						tags := map[string]string{}
						fields := map[string]interface{}{
							"buying_power": buyingPower,
							"equity":       equity,
						}

						point, err := client.NewPoint("portfolio", tags, fields, timestamp)
						if err != nil {
							fmt.Println(err)
							continue
						}
						batch.AddPoint(point)

						// Write the batch
						err = influx.Write(batch)
						if err != nil {
							fmt.Println(err)
							continue
						}
					}

					consumer.MarkOffset(message, "") // mark message as processed
				}
			}
		}
	}
}
