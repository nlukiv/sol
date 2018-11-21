package main

import (
	"fmt"
	"net/url"
	"reflect"
	"sol/util"
	"strconv"
	//rtt "github.com/rtt/Go-Solr"
	//vang "github.com/vanng822/go-solr/solr"
	vang "github.com/vanng822/go-solr/solr"
)

const clientId = "284"
const solrUrl = "http://192.168.0.12:8983/solr"
const path = "D:\\Soft\\solr-7.5.0\\server\\solr\\"

func main() {
	ReloadAll(solrUrl)

}

func addCDE() {
	cde := "clientdocumententities_" + clientId
	if !CoreExists(solrUrl, cde) {
		Create(solrUrl, cde)
	}
	si, err := vang.NewSolrInterface(solrUrl, cde)
	check(err)

	var documents []vang.Document
	documents = append(documents, CDE(1, 1, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
	documents = append(documents, CDE(2, 1, "PostgreSQL", "PostgreSQL", []int{88, 96}))
	documents = append(documents, CDE(3, 1, "MySQL", "MySQL", []int{100, 105, 140, 145}))
	documents = append(documents, CDE(4, 1, "JavaScript", "JavaScript", []int{70, 72}))
	documents = append(documents, CDE(5, 2, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
	documents = append(documents, CDE(6, 2, "PostgreSQL", "PostgreSQL", []int{88, 96}))
	documents = append(documents, CDE(7, 2, "MySQL", "MySQL", []int{100, 105, 140, 145}))
	documents = append(documents, CDE(8, 2, "JavaScript", "JavaScript", []int{70, 72}))

	sur, err := si.Add(documents, 1, nil)
	check(err)

	fmt.Println(sur.Result)
}
func CDE(id, documentID int, entity, englishEntity string, positions []int) vang.Document {
	d := make(vang.Document)
	d.Set("id", id)
	d.Set("DocumentID", documentID)
	d.Set("Entity", entity)
	d.Set("EnglishEntity", englishEntity)
	d.Set("Positions", positions)
	return d
}

func addCDT() {
	cdt := "clientdocumentthemes_" + clientId
	if !CoreExists(solrUrl, cdt) {
		Create(solrUrl, cdt)
	}
	si, err := vang.NewSolrInterface(solrUrl, cdt)
	check(err)

	var documents []vang.Document
	documents = append(documents, CDT(1, 1, 8))
	documents = append(documents, CDT(2, 2, 8))

	sur, err := si.Add(documents, 1, nil)
	check(err)

	fmt.Println(sur.Result)
}
func CDT(id int, documentID, themeID int) vang.Document {
	d := make(vang.Document)
	d.Set("id", id)
	d.Set("DocumentID", documentID)
	d.Set("ThemeID", themeID)
	return d
}

func addCI() {
	ci := "clientindices_" + clientId
	if !CoreExists(solrUrl, ci) {
		Create(solrUrl, ci)
	}
	si, err := vang.NewSolrInterface(solrUrl, ci)
	check(err)

	var documents []vang.Document
	documents = append(documents, CI("daskpdn228dpasud", "my_first_index"))

	sur, err := si.Add(documents, 1, nil)
	check(err)

	fmt.Println(sur.Result)
}
func CI(apikey, indexName string) vang.Document {
	d := make(vang.Document)
	d.Set("apikey", apikey)
	d.Set("indexname", indexName)
	return d
}

func addCD() {
	cd := "clientdocuments_" + clientId
	if !CoreExists(solrUrl, cd) {
		Create(solrUrl, cd)
	}
	si, err := vang.NewSolrInterface(solrUrl, cd)
	check(err)

	var documents []vang.Document
	documents = append(documents, CD(1, "my document2", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.", "en", "2018-11-17 17:03:16.148444", "neu"))
	documents = append(documents, CD(2, "my document2", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.", "en", "2018-11-17 17:17:42.764131", "pos"))

	sur, err := si.Add(documents, 1, nil)
	check(err)

	fmt.Println(sur.Result)
}
func CD(documentid int, documenttitle, document, languagecode, created, sentiment string) vang.Document {
	d := make(vang.Document)
	d.Set("documentid", documentid)
	d.Set("documenttitle", documenttitle)
	d.Set("document", document)
	d.Set("languagecode", languagecode)
	d.Set("created", created)
	d.Set("sentiment", sentiment)
	return d
}

func typeof(v interface{}) string {
	return reflect.TypeOf(v).String()
}

// needs more work
//http://localhost:8983/solr/admin/cores?action=CREATE&name=cot&instanceDir=cot&configSet=_default
//https://stackoverflow.com/questions/21619947/create-new-cores-in-solr-via-http
func Create(solrUrl string, coreName string) string {
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	v.Add("name", coreName)
	v.Add("instanceDir", coreName)
	v.Add("configSet", "_default")
	//v.Add("dataDir", path+coreName + "\\data\\")
	//v.Add("config", "solrconfig.xml")
	//v.Add("schema", "")
	//dniwe
	//	exec.Command("D:\\Soft\\solr-7.5.0\\bin\\solr", "create_core", "-c", coreName).Run()

	res, err := ca.Action("CREATE", &v)
	check(err)
	return util.ResponseJson(res)
}

// Status of all Solr cores.
// Return type - string of json
func StatusAll(solrUrl string) string {
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	res, err := ca.Action("STATUS", &v)
	check(err)
	return util.ResponseJson(res)
}

// Status of specific Solr core.
// Return type - string of json
func StatusCore(solrUrl string, coreName string) string {
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	v.Add("core", coreName)
	res, err := ca.Action("STATUS", &v)
	check(err)
	return util.ResponseJson(res)
}

// returns false if no
func CoreExists(solrUrl string, core string) bool {
	query := util.JsonQuery(StatusAll(solrUrl))
	cor, err := query.Object("Response", "status", core)
	check(err)
	return len(cor) != 0
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
	return util.ResponseJson(res)
}

// Reload specific Solr core.
// Return type - string of json
func Reload(solrUrl string, coreName string) (string, error) {
	ca, _ := vang.NewCoreAdmin(solrUrl)

	v := url.Values{}
	v.Add("core", coreName)

	res, err := ca.Action("RELOAD", &v)
	return util.ResponseJson(res), err
}

// Reload all Solr cores.
// Return type - string of json
func ReloadAll(solrUrl string) error {
	query := util.JsonQuery(StatusAll(solrUrl))
	cor, err := query.Object("Response", "status")
	for k := range cor {
		_, err := Reload(solrUrl, k)
		if err != nil {
			return err
		}
	}
	return err
}

//
//func EnsureClientStorage(solrUrl string, storage string) error{
//	status := StatusCore(solrUrl,storage)
//	jsonparser.Get([]byte(status), "Response")
//
//
//	return
//}

func check(e error) {
	if e != nil {
		fmt.Println(e)
	}
}
