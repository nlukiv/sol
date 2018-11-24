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

const clientIdTest = "2840"
const solrUrl = "http://localhost:8983/solr"

func main() {
	DeleteIndex(clientIdTest)
	write()
	DeleteDocument("345", clientIdTest)

}

func write() {
	if CheckConnection() == nil {
		documentid := uint64(123)

		entities := []util.Entity{}
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		themes := []string{"5", "8"}

		err := AddDocument(documentid, clientIdTest, "my document123", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "neu", entities, themes)
		check(err)

		documentid = 234
		entities = nil
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		err = AddDocument(documentid, clientIdTest, "my document234", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "neu", entities, themes)
		check(err)

		documentid = 345
		entities = nil
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		err = AddDocument(documentid, clientIdTest, "my document345", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "neu", entities, themes)
		check(err)

		documentid = 456
		entities = nil
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		err = AddDocument(documentid, clientIdTest, "my document456", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "neu", entities, themes)
		check(err)
	}
}

// checks if BackendStorage exists and connection is available
func CheckConnection() error {
	_, err := StatusAll()
	return err
}

// checks that client's storage exists. Creates new one if not
func EnsureClientStorage(clientID string) error {
	cores := []string{"clientdocumententities", "clientdocumentthemes", "clientdocuments"}

	for _, elem := range cores {
		core := elem + "_" + clientID
		coreExists, err := CoreExists(core)
		if err != nil {
			return err
		}

		if !coreExists {
			_, err := CreateCore(core)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// drops [к чєртям собачим] entire index with 'clientID'
func DeleteIndex(clientID string) error {
	cores := []string{"clientdocumententities", "clientdocumentthemes", "clientdocuments"}

	for _, elem := range cores {
		core := elem + "_" + clientID
		coreExists, err := CoreExists(core)
		if err != nil {
			return err
		}

		if coreExists {
			_, err := DeleteCore(core, true)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Add document to client index
func AddDocument(documentid uint64, clientID string, title, text, languageCode, sentiment string,
	entitiesFound []util.Entity, themes []string) error {

	err := EnsureClientStorage(clientID)
	if err != nil {
		return err
	}

	//1. Створюється запис в табличку `clientdocuments_%indexID%` з наступними полями: `title, text, languageCode, sentiment, created`.
	// В створеного запису береться `documetnID` (created entry primary key/unique key)

	//documentid, err := util.GetID()
	//if err != nil {
	//	return err
	//}

	cd := "clientdocuments_" + clientID

	si, err := vang.NewSolrInterface(solrUrl, cd)
	if err != nil {
		return err
	}

	var documents []vang.Document
	clientDocument := make(vang.Document)
	clientDocument.Set("documentID", documentid)
	clientDocument.Set("documentTitle", title)
	clientDocument.Set("document", text)
	clientDocument.Set("languageCode", languageCode)
	//clientDocument.Set("created", created)
	clientDocument.Set("sentiment", sentiment)
	documents = append(documents, clientDocument)

	_, err = si.Add(documents, 1, nil)
	if err != nil {
		return err
	}

	//2. Створється запис в табличку `clientdocumententities_%indexID%` з наступними полями:
	// `documentID, entity, englishEntity, positions`. всі дані беруться з аргументу `entitiesFound []*entities.Entity`

	cde := "clientdocumententities_" + clientID
	si, err = vang.NewSolrInterface(solrUrl, cde)
	if err != nil {
		return err
	}

	documents = nil
	for _, entity := range entitiesFound {
		clientDocumentEntity := make(vang.Document)
		clientDocumentEntity.Set("documentID", documentid)
		clientDocumentEntity.Set("entity", entity.Entity)
		clientDocumentEntity.Set("englishEntity", entity.EnglishEntity)
		clientDocumentEntity.Set("positions", entity.Positions)
		documents = append(documents, clientDocumentEntity)
	}

	_, err = si.Add(documents, 1, nil)
	if err != nil {
		return err
	}

	// 3. Створється запис в табличку `clientdocumentthemes_%indexID%` з наступними полями: `themeid, documentid`. (edited)

	cdt := "clientdocumentthemes_" + clientID

	si, err = vang.NewSolrInterface(solrUrl, cdt)
	if err != nil {
		return err
	}

	documents = nil
	for _, theme := range themes {
		clientDocumentTheme := make(vang.Document)
		clientDocumentTheme.Set("documentID", documentid)
		clientDocumentTheme.Set("themeID", theme)
		documents = append(documents, clientDocumentTheme)
	}

	_, err = si.Add(documents, 1, nil)
	if err != nil {
		return err
	}

	err = ReloadAll()
	if err != nil {
		return err
	}

	return nil
}

// deletes document with 'documentID' from client 'clientID' collection
func DeleteDocument(documentID, clientID string) error {

	corePrefixes := []string{"clientdocuments_", "clientdocumententities_", "clientdocumentthemes_"}

	for _, corePrefix := range corePrefixes {

		si, err := vang.NewSolrInterface(solrUrl, corePrefix+clientID)
		if err != nil {
			return err
		}

		params := &url.Values{}
		params.Add("commit", "true")
		_, err = si.Delete(map[string]interface{}{"query": "documentID:" + documentID}, params)
		if err != nil {
			return err
		}

	}

	err := ReloadAll()
	if err != nil {
		return err
	}

	return nil
}

// @Deprecated
//func addCDE() {
//	cde := "clientdocumententities_" + clientIdTest
//	//if !CoreExists(cde) {
//	//	CreateCore(cde)
//	//}
//	si, err := vang.NewSolrInterface(solrUrl, cde)
//	check(err)
//
//	var documents []vang.Document
//	documents = append(documents, CDE(1, 1, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
//	documents = append(documents, CDE(2, 1, "PostgreSQL", "PostgreSQL", []int{88, 96}))
//	documents = append(documents, CDE(3, 1, "MySQL", "MySQL", []int{100, 105, 140, 145}))
//	documents = append(documents, CDE(4, 1, "JavaScript", "JavaScript", []int{70, 72}))
//	documents = append(documents, CDE(5, 2, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
//	documents = append(documents, CDE(6, 2, "PostgreSQL", "PostgreSQL", []int{88, 96}))
//	documents = append(documents, CDE(7, 2, "MySQL", "MySQL", []int{100, 105, 140, 145}))
//	documents = append(documents, CDE(8, 2, "JavaScript", "JavaScript", []int{70, 72}))
//
//	sur, err := si.Add(documents, 1, nil)
//	check(err)
//
//	fmt.Println(sur)
//}
//func CDE(id, documentID int, entity, englishEntity string, positions []int) vang.Document {
//	d := make(vang.Document)
//	d.Set("id", id)
//	d.Set("DocumentID", documentID)
//	d.Set("Entity", entity)
//	d.Set("EnglishEntity", englishEntity)
//	d.Set("Positions", positions)
//	return d
//}
//
//func addCDT() {
//	cdt := "clientdocumentthemes_" + clientIdTest
//	//if !CoreExists(cdt) {
//	//	CreateCore(cdt)
//	//}
//	si, err := vang.NewSolrInterface(solrUrl, cdt)
//	check(err)
//
//	var documents []vang.Document
//	documents = append(documents, CDT(1, 1, 8))
//	documents = append(documents, CDT(2, 2, 8))
//
//	sur, err := si.Add(documents, 1, nil)
//	check(err)
//
//	fmt.Println(sur.Result)
//}
//func CDT(id int, documentID, themeID int) vang.Document {
//	d := make(vang.Document)
//	d.Set("id", id)
//	d.Set("DocumentID", documentID)
//	d.Set("ThemeID", themeID)
//	return d
//}
//
//func addCI() {
//	ci := "clientindices_" + clientIdTest
//	//if !CoreExists(ci) {
//	//	CreateCore(ci)
//	//}
//	si, err := vang.NewSolrInterface(solrUrl, ci)
//	check(err)
//
//	var documents []vang.Document
//	documents = append(documents, CI("daskpdn228dpasud", "my_first_index"))
//
//	sur, err := si.Add(documents, 1, nil)
//	check(err)
//
//	fmt.Println(sur.Result)
//}
//func CI(apikey, indexName string) vang.Document {
//	d := make(vang.Document)
//	d.Set("apikey", apikey)
//	d.Set("indexname", indexName)
//	return d
//}
//
//func addCD() {
//	cd := "clientdocuments_" + clientIdTest
//	//if !CoreExists(cd) {
//	//	CreateCore(cd)
//	//}
//	si, err := vang.NewSolrInterface(solrUrl, cd)
//	check(err)
//
//	var documents []vang.Document
//	documents = append(documents, CD(1, "my document2", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.", "en", "2018-11-17 17:03:16.148444", "neu"))
//	documents = append(documents, CD(2, "my document2", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.", "en", "2018-11-17 17:17:42.764131", "pos"))
//
//	sur, err := si.Add(documents, 1, nil)
//	check(err)
//
//	fmt.Println(sur.Result)
//}
//func CD(documentid int, documenttitle, document, languagecode, created, sentiment string) vang.Document {
//	d := make(vang.Document)
//	d.Set("documentid", documentid)
//	d.Set("documenttitle", documenttitle)
//	d.Set("document", document)
//	d.Set("languagecode", languagecode)
//	d.Set("created", created)
//	d.Set("sentiment", sentiment)
//	return d
//}

func typeof(v interface{}) string {
	return reflect.TypeOf(v).String()
}

// needs more work (?)
// http://localhost:8983/solr/admin/cores?action=CREATE&name=cot&instanceDir=cot&configSet=_default
// https://stackoverflow.com/questions/21619947/create-new-cores-in-solr-via-http
func CreateCore(coreName string) (string, error) {
	ca, err := vang.NewCoreAdmin(solrUrl)

	if err != nil {
		return "", err
	}

	v := url.Values{}
	v.Add("name", coreName)
	v.Add("instanceDir", coreName)
	v.Add("configSet", "_default")

	res, err := ca.Action("CREATE", &v)
	if err != nil {
		return "", err
	}
	return util.ResponseJson(res), nil
}

// Status of all Solr cores.
// Return type - string of json
func StatusAll() (string, error) {
	ca, err := vang.NewCoreAdmin(solrUrl)
	if err != nil {
		return "", err
	}
	res, err := ca.Action("STATUS", &url.Values{})
	if err != nil {
		return "", err
	}
	return util.ResponseJson(res), nil
}

// Status of specific Solr core.
// Return type - string of json
func StatusCore(coreName string) (string, error) {
	ca, err := vang.NewCoreAdmin(solrUrl)
	if err != nil {
		return "", err
	}

	v := url.Values{}
	v.Add("core", coreName)

	res, err := ca.Action("STATUS", &v)
	if err != nil {
		return "", err
	}
	return util.ResponseJson(res), nil
}

// returns false if no
func CoreExists(core string) (bool, error) {
	res, err := StatusAll()
	if err != nil {
		return false, err
	}
	query := util.JsonQuery(res)
	_, err = query.Object("Response", "status", core)

	if err != nil {
		return false, nil
	}
	return true, nil
}

// Delete(unload) specific Solr core.
// Return type - string of json
func DeleteCore(coreName string, deleteIndex bool) (string, error) {
	ca, err := vang.NewCoreAdmin(solrUrl)
	if err != nil {
		return "", err
	}
	v := url.Values{}
	v.Add("core", coreName)
	v.Add("deleteIndex", strconv.FormatBool(deleteIndex))

	res, err := ca.Action("UNLOAD", &v)
	if err != nil {
		return "", err
	}
	return util.ResponseJson(res), nil
}

// Reload specific Solr core.
// Return type - string of json
func Reload(coreName string) (string, error) {
	ca, err := vang.NewCoreAdmin(solrUrl)
	if err != nil {
		return "", err
	}

	v := url.Values{}
	v.Add("core", coreName)

	res, err := ca.Action("RELOAD", &v)
	if err != nil {
		return "", err
	}

	return util.ResponseJson(res), nil
}

// Reload all Solr cores.
// Return type - string of json
func ReloadAll() error {
	res, err := StatusAll()
	if err != nil {
		return err
	}
	query := util.JsonQuery(res)
	cor, err := query.Object("Response", "status")
	if err != nil {
		return err
	}
	for k := range cor {
		_, err := Reload(k)
		if err != nil {
			return err
		}
	}
	return nil
}

func check(e error) {
	if e != nil {
		fmt.Println(e)
	}
}
