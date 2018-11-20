package main

import (
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	//rtt "github.com/rtt/Go-Solr"
	//vang "github.com/vanng822/go-solr/solr"
	vang "github.com/vanng822/go-solr/solr"
)

const clientId = "284"
const solrUrl = "http://192.168.0.12:8983/solr"
const path = "D:\\Soft\\solr-7.5.0\\server\\solr\\"

func main() {
	//s, err := rtt.Init("localhost", 8983, clientId)
	//check(err)
	//
	//si, _ := vang.NewSolrInterface(solrUrl, "collection1")
	//query := vang.NewQuery()
	//query.Q("*:*")
	//ss := si.Search(query)
	//r, _ := ss.Result(nil)
	//fmt.Println(r.Results.Docs)
	//rand.Seed(time.Now().UnixNano())

	//Create(solrUrl, randomString(4))
	//Delete(solrUrl, "cor", true)
	s := StatusAll(solrUrl)

	data, _ := json.Marshal(s)
	fmt.Println(string(data))
	status := gjson.Get(string(data), "Status")
	//m := map[string]interface{}{"one": 1, "two": 2}
	//enc := json.NewEncoder(os.Stdout)
	//err := enc.Encode(s)
	//if err != nil {
	//	fmt.Println(err.Error())
	//}
	fmt.Println(s)
	fmt.Println(status)
}

func formatRequest(r *http.Request) string {
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

// needs more work
//http://localhost:8983/solr/admin/cores?action=CREATE&name=cot&instanceDir=cot&configSet=_default
//https://stackoverflow.com/questions/21619947/create-new-cores-in-solr-via-http
func Create(solrUrl string, coreName string) {
	fmt.Println("creating sore: " + coreName)
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	v.Add("name", coreName)
	////exec.Command("D:\\Soft\\solr-7.5.0\\bin\\solr", "create_core", "-c", coreName).Run()
	//v.Add("instanceDir", path+coreName)
	//v.Add("dataDir", path+coreName + "\\data\\")
	//v.Add("config", "solrconfig.xml")
	//v.Add("schema", "")

	//dniwe
	//	exec.Command("D:\\Soft\\solr-7.5.0\\bin\\solr", "create_core", "-c", coreName).Run()

	ca.Action("CREATE", &v)
}

// Status of all Solr cores.
// Return type - string of json
func StatusAll(solrUrl string) string {
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	response, err := ca.Action("STATUS", &v)
	check(err)
	json, err := json.Marshal(*response)
	check(err)
	return string(json)
}

// Status of specific Solr core.
// Return type - string of json
func StatusCore(solrUrl string, coreName string) string {
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	v.Add("core", coreName)
	res, err := ca.Action("STATUS", &v)
	check(err)
	json, err := json.Marshal(*res)
	check(err)
	return string(json)
}

// Delete(unload) specific Solr core.
// Return type - string of json
func Delete(solrUrl string, coreName string, deleteIndex bool) string {
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	v.Add("core", coreName)
	v.Add("deleteIndex", strconv.FormatBool(deleteIndex))

	res, err := ca.Action("UNLOAD", &v)
	check(err)
	json, err := json.Marshal(*res)
	check(err)
	return string(json)
}

func check(e error) {
	if e != nil {
		fmt.Println(e)
	}
}
