package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

type reading struct {
	Date  *string  `json:"date"`
	Value *float64 `json:"value"`
}

type data struct {
	Key    string    `json:"key"`
	Values []reading `json:"values"`
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

func main() {
	prevDates := make(map[string]string)
	for {
		var toTransmit []data
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

			fields := logrus.Fields{
				"station": name,
				"date":    date,
				"reading": *d.Values[0].Value,
			}

			if prevDates[name] != date {
				prevDates[name] = date
				toTransmit = append(toTransmit, d)
				logrus.WithFields(fields).Info("Got new data")
			} else {
				logrus.WithFields(fields).Info("Already have recent data")
			}
		}

		// TODO: transmit new data to Kafka broker
		logrus.Infof("Have %d tuples to transmit", len(toTransmit))

		time.Sleep(10 * time.Minute)
	}
}
