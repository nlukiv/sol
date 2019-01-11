package util

import (
	"encoding/json"
	"fmt"
	"github.com/jmoiron/jsonq"
	"github.com/segmentio/ksuid"
	"github.com/vanng822/go-solr/solr"
	"hash/fnv"
	"math/rand"
	"net/http"
	"strings"
)

func FormatRequest(r *http.Request) string {

	// Create return string
	var request []string
	// Add the request string
	url := fmt.Sprintf("%v %v %v", r.Method, r.URL, r.Proto)
	request = append(request, url)
	// Add the host
	request = append(request, fmt.Sprintf("Host: %v", r.Host))
	// Loop through headers
	for name, headers := range r.Header {
		name = strings.ToLower(name)
		for _, h := range headers {
			request = append(request, fmt.Sprintf("%v: %v", name, h))
		}
	}

	// If this is a POST, add post data
	if r.Method == "POST" {
		r.ParseForm()
		request = append(request, "\n")
		request = append(request, r.Form.Encode())
	}
	// Return the request as a string
	return strings.Join(request, "\n")
}

func JsonQuery(js string) *jsonq.JsonQuery {
	data := map[string]interface{}{}
	dec := json.NewDecoder(strings.NewReader(js))
	dec.Decode(&data)
	return jsonq.NewQuery(data)
}

func ResponseJson(response *solr.SolrResponse) string {
	jso, err := json.Marshal(*response)
	check(err)
	return string(jso)
}

func check(e error) {
	if e != nil {
		fmt.Println(e)
	}
}

func GetID() uint64 {
	bytes := ksuid.New().Bytes()
	h := fnv.New64a()
	h.Write(bytes)
	return h.Sum64()
}

func GetIDstr() string {
	bytes := ksuid.New()
	return bytes.String()
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
