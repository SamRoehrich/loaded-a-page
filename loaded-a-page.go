/*
This program was created by me to help answer a simple product question
"Does the feature have enough page views to justify its existance" or "How many users can we expect to see this feature"

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
	"net/http"
	"os"
	"time"
)

const amp = "https://amplitude.com/api/2/events/segmentation"

var f = flag.String("f", "", "location of the CSV file")
var k = flag.String("k", "", "amplitude auth key")
var s = flag.String("s", "", "amplitude secret")
var d = flag.Int("d", 1, "days since time of exectuion")

func main() {
	var t int
	ch := make(chan int)
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
	fmt.Printf("Total: %d \n", t)
}

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

func fetch(url string, ch chan int) {
	c := &http.Client{}
	r, _ := http.NewRequest("GET", amp, nil)
	e := base64.StdEncoding.EncodeToString([]byte(*k + ":" + *s))
	end := time.Now()
	start := end.AddDate(0, 0, -*d)

	const layout = "20060102"
	startStr := start.Format(layout)
	endStr := end.Format(layout)

	q := Query{
		EventType: "_all",
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

	res, _ := c.Do(r)

	fmt.Printf("Status: %d\n", res.StatusCode)
	ch <- len(url)
}
