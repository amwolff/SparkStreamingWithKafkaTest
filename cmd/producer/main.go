package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

var loc *time.Location

func init() {
	l, err := time.LoadLocation("Europe/Warsaw")
	if err != nil {
		logrus.WithError(err).Panic("LoadLocation")
	}
	loc = l
}

type reading struct {
	Date  *string  `json:"date"`
	Value *float64 `json:"value"`
}

type data struct {
	Key         string    `json:"key"`
	Values      []reading `json:"values"`
	stationName string
}

var stationNameToURL = map[string]string{
	"WIOŚ Gołdap ul. Jaćwieska":   "http://api.gios.gov.pl/pjp-api/rest/data/getData/5718",
	"KMŚ Puszcza Borecka":         "http://api.gios.gov.pl/pjp-api/rest/data/getData/16424",
	"WIOŚ Ełk":                    "http://api.gios.gov.pl/pjp-api/rest/data/getData/16216",
	"WIOŚ Ostróda Piłsudskiego":   "http://api.gios.gov.pl/pjp-api/rest/data/getData/16181",
	"WIOŚ Biskupiec-Mobilna":      "http://api.gios.gov.pl/pjp-api/rest/data/getData/20435",
	"WIOŚ Olsztyn ul. Puszkina":   "http://api.gios.gov.pl/pjp-api/rest/data/getData/5761",
	"WIOŚ Elbląg ul. Bażyńskiego": "http://api.gios.gov.pl/pjp-api/rest/data/getData/5678",
}

var brokersAddrs = []string{"master:9092", "slave01:9092", "slave02:9092",
	"slave03:9092", "slave04:9092", "slave05:9092"}

func initProducer() (sarama.SyncProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = true
	cfg.Producer.Return.Successes = true
	cfg.Version = sarama.V0_11_0_0

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return sarama.NewSyncProducer(brokersAddrs, cfg)
}

const topicName = "gios"

func transmit(producer sarama.SyncProducer, key, val, date string) (
	int32,
	int64,
	error) {

	t, err := time.ParseInLocation("2006-01-02 15:04:05", date, loc)
	if err != nil {
		return 0, 0, fmt.Errorf("ParseInLocation: %w", err)
	}

	p, o, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topicName,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(val),
		Timestamp: t,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("SendMessage: %w", err)
	}

	return p, o, nil
}

func ftoa(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// TODO: clean up `main`.
func main() {
	producer, err := initProducer()
	if err != nil {
		logrus.WithError(err).Panic("initProducer")
	}
	defer producer.Close()

	prevDates := make(map[string]string)
	for {
		for name, url := range stationNameToURL {
			errFields := logrus.Fields{
				"station": name,
				"url":     url,
			}

			resp, err := http.Get(url)
			if err != nil {
				logrus.WithError(err).WithFields(errFields).Error("Get")
				continue
			}

			var d data
			if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
				logrus.WithError(err).WithFields(errFields).Error("Decode")
				if err := resp.Body.Close(); err != nil {
					logrus.WithError(err).WithFields(errFields).Error("Close")
					continue
				}
				continue
			}

			if err := resp.Body.Close(); err != nil {
				logrus.WithError(err).WithFields(errFields).Error("Close")
				continue
			}

			// Unfortunately this API isn't perfect.
			if d.Values == nil ||
				d.Values[0].Date == nil ||
				d.Values[0].Value == nil {
				logrus.Warningf("New data isn't fully loaded for %s", name)
				continue
			}

			date := *d.Values[0].Date
			val := *d.Values[0].Value

			log := logrus.WithFields(logrus.Fields{
				"station": name,
				"date":    date,
				"reading": val,
			})

			if prevDates[name] != date {
				prevDates[name] = date

				log.Info("Got new data")

				p, o, err := transmit(producer, name, ftoa(val), date)
				if err != nil {
					log.WithError(err).Error("transmit")
					continue
				}
				log.WithField("partition", p).
					WithField("offset", o).
					Info("Transmitted successfully")
			} else {
				log.Info("Already have recent data")
			}
		}
		logrus.Info("Will now wait")
		time.Sleep(10 * time.Minute)
	}
}
