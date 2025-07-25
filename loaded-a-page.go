/*
This program was created by me to help answer a simple product question
"How many users can we expect to see this feature"

It takes a CSV of `url`s as input as well as the number of days you want to measure page views of.
It prints out the final result of all URL page views and writes each page view to a CSV file.

This program was my introduction into go routines and concurrency with Go.
*/

package main

import (
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type Query struct {
	EventType string   `json:"event_type"`
	Filters   []Filter `json:"filters"`
	GroupBy   []string `json:"group_by"`
}

type Filter struct {
	Type  string   `json:"subprop_type"`
	Key   string   `json:"subprop_key"`
	Op    string   `json:"subprop_op"`
	Value []string `json:"subprop_value"`
}

type AmplitudeResponse struct {
	Data AmplitudeData `json:"data"`
}

type AmplitudeData struct {
	Series          [][]float64        `json:"series"`
	SeriesCollapsed [][]CollapsedValue `json:"seriesCollapsed"`
}

type CollapsedValue struct {
	Value float64 `json:"value"`
}

const amp = "https://amplitude.com/api/2/events/segmentation"

var f = flag.String("f", "", "location of the CSV file")
var k = flag.String("k", "", "amplitude auth key")
var s = flag.String("s", "", "amplitude secret")
var d = flag.Int("d", 1, "days since time of exectuion")

func main() {
	var t float64
	ch := make(chan float64)
	flag.Parse()

	file, err := os.Open(*f)
	if err != nil {
		fmt.Printf("Error opening file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	r := csv.NewReader(file)
	urls, err := r.ReadAll()
	if err != nil {
		fmt.Printf("Error parsing CSV: %v", err)
		os.Exit(1)
	}

	for _, url := range urls {
		go fetch(url[0], ch)
	}

	for range len(urls) {
		t += <-ch
	}
	fmt.Printf("Total: %v \n", t)
}

func fetch(url string, ch chan float64) {
	c := &http.Client{}
	r, _ := http.NewRequest("GET", amp, nil)
	e := base64.StdEncoding.EncodeToString([]byte(*k + ":" + *s))
	end := time.Now()
	start := end.AddDate(0, 0, -*d)

	const layout = "20060102"
	startStr := start.Format(layout)
	endStr := end.Format(layout)

	q := Query{
		EventType: "Loaded a Page",
		Filters: []Filter{{
			Type:  "event",
			Key:   "url",
			Op:    "is",
			Value: []string{url},
		}},
		GroupBy: []string{},
	}
	qJson, err := json.Marshal(q)
	if err != nil {
		fmt.Printf("Error marshalling json %v", err)
	}

	r.Header.Add("Authorization", "Basic "+e)
	r.Header.Add("Content-Type", "application/json")

	params := r.URL.Query()

	params.Add("e", string(qJson))
	params.Add("start", startStr)
	params.Add("end", endStr)
	params.Add("i", "1")

	r.URL.RawQuery = params.Encode()

	var res *http.Response
	res, _ = c.Do(r)

	for res.StatusCode != 200 {
		t := time.NewTimer(5 * time.Second)
		<-t.C
		res, _ = c.Do(r)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("Error reading response body: %v", err)
		os.Exit(1)
	}

	var bJson AmplitudeResponse

	err = json.Unmarshal(body, &bJson)
	if err != nil {
		fmt.Printf("Error parsing response: %v", err)
		os.Exit(1)
	}
	ch <- float64(bJson.Data.SeriesCollapsed[0][0].Value)
}
