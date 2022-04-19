package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

func newClientWithConfig() *elasticsearch.Client {
	// cfg := elasticsearch.Config{
	// 	Addresses: []string{
	// 	  "http://localhost:9200",
	// 	  "http://localhost:9201",
	// 	},
	// 	Transport: &http.Transport{
	// 	  MaxIdleConnsPerHost:   10,
	// 	  ResponseHeaderTimeout: time.Second,
	// 	  DialContext:           (&net.Dialer{Timeout: time.Second}).DialContext,
	// 	  TLSClientConfig: &tls.Config{
	// 		MaxVersion:         tls.VersionTLS11,
	// 		InsecureSkipVerify: true,
	// 	  },
	// 	},
	//   }
	//   es, err := elasticsearch.NewClient(cfg)

	return newClient()
}

func newClient() *elasticsearch.Client {
	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}
	return es
}

// cluster info

func getClusterInfo(es *elasticsearch.Client) *esapi.Response {
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	return res
}

func decodeClusterInfo(res *esapi.Response) map[string]interface{} {
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	return r
}

func requestIndex(es *elasticsearch.Client, i int, title string) {
	// Set up the request object directly.
	req := esapi.IndexRequest{
		Index:      "test",
		DocumentID: strconv.Itoa(i + 1),
		Body:       strings.NewReader(`{"title" : "` + title + `"}`),
		Refresh:    "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error indexing document ID=%d", res.Status(), i+1)
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}
}

func decodeResponse(res *esapi.Response, status string) map[string]interface{} {
	var r map[string]interface{}
	if res.IsError() {
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Fatalf("error parsing the response body: %s", err)
		}
		// Print the response status and error information.
		log.Fatalf("[%s] %s: %s",
			status,
			r["error"].(map[string]interface{})["type"],
			r["error"].(map[string]interface{})["reason"],
		)
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	return r
}

func printResult(r map[string]interface{}, status string) {
	// Print the response status, number of results, and request duration.

	// log.Printf(
	// 	"[%s] %d hits; took: %dms",
	// 	status,
	// 	int(r["hits"].(map[string]interface{})["total"].(float64)),
	// 	int(r["took"].(float64)),
	// )

	// Print the ID and document source for each hit.
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	}
}

func main() {
	log.SetFlags(0)

	var (
		r   map[string]interface{}
		wg  sync.WaitGroup
		err error
	)

	// Initialize a client with the default settings.
	//
	// An `ELASTICSEARCH_URL` environment variable will be used when exported.
	//
	es := newClient()

	// 1. Get cluster info -------------------------------------------------------
	//
	res := getClusterInfo(es)

	// Deserialize the response into a map.
	r = decodeClusterInfo(res)

	// Print version number.
	log.Printf("~~~~~~~> Elasticsearch %s", r["version"].(map[string]interface{})["number"])

	// 2. Index documents concurrently -------------------------------------------------------
	//
	for i, title := range []string{"Test One", "Test Two"} {
		wg.Add(1)

		go func(i int, title string) {
			defer wg.Done()
			requestIndex(es, i, title)
		}(i, title)
	}
	wg.Wait()

	log.Println(strings.Repeat("-", 37))

	// 3. Search for the indexed documents -------------------------------------------------------
	//
	// Use the helper methods of the client.
	res, err = es.Search(
		es.Search.WithContext(context.Background()),
		es.Search.WithIndex("test"),
		es.Search.WithBody(strings.NewReader(`{"query" : { "match" : { "title" : "test" } }}`)),
		es.Search.WithTrackTotalHits(true),
		es.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	defer res.Body.Close()

	status := res.Status()
	r = decodeResponse(res, status)
	printResult(r, status)

	log.Println(strings.Repeat("=", 37))
}

// ~~~~~~~> Elasticsearch 7.8.0-SNAPSHOT
// [200 OK] updated; version=1
// [200 OK] updated; version=1
// -------------------------------------
// [200 OK] 2 hits; took: 7ms
//  * ID=1, map[title:Test One]
//  * ID=2, map[title:Test Two]
// =====================================
