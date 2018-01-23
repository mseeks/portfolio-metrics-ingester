package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	client "github.com/influxdata/influxdb/client/v2"
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

type portfolioMessage struct {
	Value float32 `json:"portfolio_value"`
	At    string  `json:"at"`
}

func main() {
	broker := os.Getenv("KAFKA_ENDPOINT")
	topics := strings.Split(os.Getenv("KAFKA_CONSUMER_TOPICS"), ",")

	for {
		consumer, err := sarama.NewConsumer([]string{broker}, nil)
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

		var waitgroup sync.WaitGroup

		for index := range topics {
			waitgroup.Add(1)

			go func(topic string) {
				defer waitgroup.Done()

				partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
				if err != nil {
					fmt.Println(err)
					return
				}
				defer partitionConsumer.Close()

				for {
					select {
					case message := <-partitionConsumer.Messages():
						symbol := string(message.Key)
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
							fmt.Println(topic)
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

							// Create a point and add to batch
							tags := map[string]string{}
							fields := map[string]interface{}{
								"value": stat.Value,
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
					}
				}
			}(topics[index])
		}

		waitgroup.Wait()
	}
}
