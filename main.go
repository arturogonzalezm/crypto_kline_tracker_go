package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
)

type KlineData struct {
	Symbol        string
	Interval      string
	IntervalStart time.Time
	Open          float64
	High          float64
	Low           float64
	Close         float64
	Volume        float64
}

func (k *KlineData) PriceChange() float64 {
	return k.Close - k.Open
}

func (k *KlineData) PriceChangePercent() float64 {
	return (k.PriceChange() / k.Open) * 100.0
}

func newKlineData(symbol, interval string, kline map[string]interface{}) (*KlineData, error) {
	timestamp, ok := kline["t"].(float64)
	if !ok {
		return nil, errors.New("invalid timestamp")
	}

	return &KlineData{
		Symbol:        symbol,
		Interval:      interval,
		IntervalStart: time.Unix(0, int64(timestamp)*int64(time.Millisecond)),
		Open:          mustParseFloat(kline["o"]),
		High:          mustParseFloat(kline["h"]),
		Low:           mustParseFloat(kline["l"]),
		Close:         mustParseFloat(kline["c"]),
		Volume:        mustParseFloat(kline["v"]),
	}, nil
}

func mustParseFloat(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case string:
		f, _ := strconv.ParseFloat(v, 64)
		return f
	default:
		return 0
	}
}

func runWebsocket(ctx context.Context, symbol, interval string, klineChan chan<- *KlineData) {
	wsURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@kline_%s", symbol, interval)
	u, err := url.Parse(wsURL)
	if err != nil {
		log.Printf("Failed to parse WebSocket URL for %s %s: %v\n", symbol, interval, err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := connectAndStream(ctx, u, symbol, interval, klineChan); err != nil {
				log.Printf("WebSocket error for %s %s: %v\n", symbol, interval, err)
				time.Sleep(5 * time.Second) // Wait before reconnecting
			}
		}
	}
}

func connectAndStream(ctx context.Context, u *url.URL, symbol, interval string, klineChan chan<- *KlineData) error {
	log.Printf("Connecting to Binance WebSocket for %s %s...\n", symbol, interval)
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return errors.Wrap(err, "failed to connect to WebSocket")
	}
	defer c.Close()
	log.Printf("Connected to WebSocket for %s %s.\n", symbol, interval)

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, message, err := c.ReadMessage()
			if err != nil {
				return errors.Wrap(err, "WebSocket read error")
			}

			var data map[string]interface{}
			if err := json.Unmarshal(message, &data); err != nil {
				log.Printf("Failed to unmarshal message for %s %s: %v\n", symbol, interval, err)
				continue
			}

			if kline, ok := data["k"].(map[string]interface{}); ok {
				klineData, err := newKlineData(symbol, interval, kline)
				if err != nil {
					log.Printf("Failed to create KlineData for %s %s: %v\n", symbol, interval, err)
					continue
				}
				klineChan <- klineData
			}
		}
	}
}

func processKlineData(klineData *KlineData) {
	log.Printf(
		"Symbol: %s | Interval: %s | Local time: %s | Interval start: %s | "+
			"Open: %.2f | High: %.2f | Low: %.2f | Close: %.2f | "+
			"Volume: %.2f | Change: %.2f (%.2f%%)\n",
		klineData.Symbol,
		klineData.Interval,
		time.Now().Format("2006-01-02 15:04:05"),
		klineData.IntervalStart.Format("2006-01-02 15:04"),
		klineData.Open,
		klineData.High,
		klineData.Low,
		klineData.Close,
		klineData.Volume,
		klineData.PriceChange(),
		klineData.PriceChangePercent(),
	)
}

func processKlineStream(ctx context.Context, klineChan <-chan *KlineData) {
	klineCache := make(map[string]*KlineData)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case klineData := <-klineChan:
			key := fmt.Sprintf("%s_%s", klineData.Symbol, klineData.Interval)
			klineCache[key] = klineData
			processKlineData(klineData)
		case <-ticker.C:
			for _, data := range klineCache {
				processKlineData(data)
			}
		}
	}
}

func main() {
	symbols := []string{"btcusdt", "ethusdt", "bnbusdt", "adausdt", "dogeusdt"}
	intervals := []string{"1m", "5m", "15m"}

	log.Println("Starting Binance WebSocket client")
	log.Printf("Symbols: %v, Intervals: %v\n", symbols, intervals)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	klineChan := make(chan *KlineData, 100)
	var wg sync.WaitGroup

	for _, symbol := range symbols {
		for _, interval := range intervals {
			wg.Add(1)
			go func(s, i string) {
				defer wg.Done()
				runWebsocket(ctx, s, i, klineChan)
			}(symbol, interval)
		}
	}

	go processKlineStream(ctx, klineChan)

	// Use all available CPU cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

	log.Println("Shutting down...")
	cancel()
	wg.Wait()
	log.Println("Binance WebSocket client shut down")
}
