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
	"time"
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
	json, err := json.Marshal(*response)
	check(err)
	return string(json)
}

func MapToJson(response map[string]interface{}) string {
	json, err := json.Marshal(response)
	check(err)
	return string(json)
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

//
//func GetID() (uint64, error) {
//	bytes := ksuid.New().Bytes()
//	h := fnv.New64a()
//	_, err := h.Write(bytes)
//	if err != nil {
//		return 0, err
//	}
//	return h.Sum64(), nil
//}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

type Entity struct {
	DocumentID    uint64
	Entity        string
	EnglishEntity string
	Positions     []int
}

func MakeEntity(documentID uint64, entity, englishEntity string, positions []int) Entity {
	return Entity{DocumentID: documentID, Entity: entity, EnglishEntity: englishEntity, Positions: positions}
}

func StringToTime(s string) (time.Time, error) {
	layout := "2006-01-02 15:04:05.000"
	t, err := time.Parse(layout, s)
	if err != nil {
		t := time.Time{}
		return t, err
	}

	return t, nil
}

func ToIntSlice(s []interface{}) []int {
	ints := make([]int, len(s))
	for i := range s {
		ints[i] = int(s[i].(float64))
	}
	return ints
}
