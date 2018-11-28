package main

import (
	"fmt"
	"github.com/vanng822/go-solr/solr"
	"net/url"
	"reflect"
	"sol/util"
	"strconv"
	"time"
)

const clientIdTest = "2840"
const solrUrl = "http://localhost:8983/solr"

type Core string

const (
	DOCUMENTS Core = "clientdocuments"
	ENTITIES  Core = "clientdocumententities"
	THEMES    Core = "clientdocumentthemes"
)

type DocPropertyType int

const (
	LANGUAGE DocPropertyType = iota
	SENTIMENT
	THEME
	ENTITY
	CREATED
)

type ComparerType int

const (
	EQUAL ComparerType = iota
	LIKE
)

func (p DocPropertyType) String() string {
	if p == LANGUAGE {
		return "languagecode"
	}
	if p == SENTIMENT {
		return "sentiment"
	}
	if p == THEME {
		return "themeid"
	}
	if p == ENTITY {
		return "englishentity"
	}
	if p == CREATED {
		return "created"
	}

	return `"unknown property"`
}

func (c ComparerType) String() string {
	if c == EQUAL {
		return "="
	}
	if c == LIKE {
		return "LIKE"
	}

	return `"unknown comparer type"`
}

type PropertyCondition struct {
	Condition ComparerType
	Value     string
	Property  DocPropertyType
}

type ClientDocument struct {
	ClientDocumentShort
	DocumentTitle string    `gorm:"column:documenttitle;not null"`
	Document      string    `gorm:"column:document;not null"`
	LanguageCode  string    `gorm:"column:languagecode;not null"`
	CreatedAt     time.Time `gorm:"column:created;not null"`
	Sentiment     string    `gorm:"column:sentiment;not null"`
}

type ClientDocumentShort struct {
	DocumentID uint64 `gorm:"primary_key;column:documentid;not null"`
}

func main() {

	//pc := PropertyCondition{Condition: LIKE, Value: "keu", Property: SENTIMENT}

	//di, err := GetDocumentIDs(clientIdTest, pc)
	//check(err)
	//
	//fmt.Println(di)

	for i := 0; i < 33; i++ {
		id, _ := util.GetID()
		fmt.Println(id)
	}

	//ids := []uint64{321, 123}
	//docs, _ := GetDocuments(clientIdTest, ids)
	//
	//fmt.Println(docs)

	//DeleteIndex(clientIdTest)
	//write()
	//DeleteDocument("345", clientIdTest)

}

// get docs with sentiment=neu: PropertyCondition.Condition=EQUAL PropertyCondition.Value=neu, PropertyCondition.Property=sentiment
func GetDocumentIDs(clientID string, cond PropertyCondition) ([]uint64, error) {

	var core string

	// define which core to send query to
	switch cond.Property {
	case LANGUAGE, SENTIMENT, CREATED:
		core = fmt.Sprintf("%s_%s", DOCUMENTS, clientID)
	case ENTITY:
		core = fmt.Sprintf("%s_%s", ENTITIES, clientID)
	case THEME:
		core = fmt.Sprintf("%s_%s", THEMES, clientID)
	}

	// create connection to the core
	si, err := solr.NewSolrInterface("http://localhost:8983/solr", core)
	if err != nil {
		return nil, err
	}
	query := solr.NewQuery()
	query.FieldList("documentID")

	switch cond.Condition {
	case EQUAL:
		query.Q(fmt.Sprintf("%s:%s", cond.Property, cond.Value))
	case LIKE:
		query.Q(fmt.Sprintf("%s:%s~", cond.Property, cond.Value))
	}

	s := si.Search(query)
	r, err := s.Result(nil)
	if err != nil {
		return nil, err
	}

	var res []uint64
	for _, doc := range r.Results.Docs {
		k := doc.Get("documentID")
		res = append(res, uint64(k.([]interface{})[0].(float64)))
	}

	return res, nil
}

//func GetEntities(clientID string, cond PropertyCondition) ([]*domain.ClientDocumentEntity, error){
//	var core string
//
//	switch cond.Property {
//	case LANGUAGE, SENTIMENT, CREATED:
//		core = "clientdocuments_" + clientID
//	case ENTITY:
//		core = "clientdocumententities_" + clientID
//	case THEME:
//		core = "clientdocumentthemes_" + clientID
//	}
//
//	si, err := solr.NewSolrInterface("http://localhost:8983/solr", core)
//	if err != nil {
//		return nil, err
//	}
//	query := solr.NewQuery()
//
//	switch cond.Condition {
//	case EQUAL:
//		query.Q(fmt.Sprintf("%s:%s", cond.Property, cond.Value))
//	case LIKE:
//		query.Q(fmt.Sprintf("%s:%s~", cond.Property, cond.Value))
//	}
//
//	s := si.Search(query)
//	r, err := s.Result(nil)
//	if err != nil {
//		return nil, err
//	}
//
//	var res []uint64
//	for _, doc := range r.Results.Docs {
//		k := doc.Get("documentID")
//		res = append(res, uint64(k.([]interface{})[0].(float64)))
//	}
//
//	return res, nil
//}

func GetDocuments(clientID string, ids []uint64) ([]*ClientDocument, error) {
	var docs []*ClientDocument

	si, err := solr.NewSolrInterface("http://localhost:8983/solr", fmt.Sprintf("%s_%s", DOCUMENTS, clientID))
	if err != nil {
		return nil, err
	}

	for _, id := range ids {

		// another possible implementation is to not send each id in a separate query, but
		// use filterQuery and put all ids in there. example
		// http://localhost:8983/solr/clientdocuments_2840/select?fq=documentID:(123%20OR%20234)&q=*:*
		// downside - this way, queries may get TOO long if the id list is long
		// https://stackoverflow.com/questions/7594915/apache-solr-or-in-filter-query

		query := solr.NewQuery()
		query.Q(fmt.Sprintf("documentID:%d", id))
		s := si.Search(query)
		r, err := s.Result(nil)
		if err != nil {
			return nil, err
		}

		doc := r.Results.Docs[0] // we definitely get one element, since querying by unique id
		docID := doc.Get("documentID")
		title := doc.Get("documentTitle").(string)
		document := doc.Get("document").(string)
		lang := doc.Get("languageCode").(string)
		created, err := util.StringToTime(doc.Get("created").(string))
		if err != nil {
			return nil, err
		}
		sentiment := doc.Get("sentiment").(string)

		cds := ClientDocumentShort{docID.(uint64)}
		clientDocument := ClientDocument{
			cds,
			title,
			document,
			lang,
			created,
			sentiment,
		}

		docs = append(docs, &clientDocument)
	}

	return docs, nil
}

func write() {
	if CheckConnection() == nil {
		documentid := uint64(321)

		entities := []util.Entity{}
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		themes := []string{"5", "8"}

		err := AddDocument(documentid, clientIdTest, "my document321", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "reu", entities, themes)
		check(err)

		documentid = 432
		entities = nil
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		err = AddDocument(documentid, clientIdTest, "my document432", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "sia", entities, themes)
		check(err)

		documentid = 543
		entities = nil
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		err = AddDocument(documentid, clientIdTest, "my document543", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "bia", entities, themes)
		check(err)

		documentid = 654
		entities = nil
		entities = append(entities, util.MakeEntity(documentid, "MongoDB", "MongoDB", []int{75, 82, 115, 120, 108, 113}))
		entities = append(entities, util.MakeEntity(documentid, "PostgreSQL", "PostgreSQL", []int{88, 96}))
		entities = append(entities, util.MakeEntity(documentid, "MySQL", "MySQL", []int{100, 105, 140, 145}))
		entities = append(entities, util.MakeEntity(documentid, "JavaScript", "JavaScript", []int{70, 72}))
		err = AddDocument(documentid, clientIdTest, "my document654", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.",
			"en", "mia", entities, themes)
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
	corePrefixes := []Core{DOCUMENTS, ENTITIES, THEMES}

	for _, elem := range corePrefixes {
		core := fmt.Sprintf("%s_%s", elem, clientID)

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

// drops entire index with 'clientID'
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
func AddDocument(documentID uint64, clientID string, title, text, languageCode, sentiment string,
	entitiesFound []util.Entity, themes []string) error {

	err := EnsureClientStorage(clientID)
	if err != nil {
		return err
	}

	cd := "clientdocuments_" + clientID

	si, err := solr.NewSolrInterface(solrUrl, cd)
	if err != nil {
		return err
	}

	var documents []solr.Document
	clientDocument := make(solr.Document)
	clientDocument.Set("documentID", documentID)
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
	si, err = solr.NewSolrInterface(solrUrl, cde)
	if err != nil {
		return err
	}

	documents = nil
	for _, entity := range entitiesFound {
		clientDocumentEntity := make(solr.Document)
		clientDocumentEntity.Set("documentID", documentID)
		clientDocumentEntity.Set("entity", entity.Entity)
		clientDocumentEntity.Set("englishEntity", entity.EnglishEntity)
		clientDocumentEntity.Set("positions", entity.Positions)
		documents = append(documents, clientDocumentEntity)
	}

	_, err = si.Add(documents, 1, nil)
	if err != nil {
		return err
	}

	// 3. Створється запис в табличку `clientdocumentthemes_%indexID%` з наступними полями: `themeid, documentID`. (edited)

	cdt := "clientdocumentthemes_" + clientID

	si, err = solr.NewSolrInterface(solrUrl, cdt)
	if err != nil {
		return err
	}

	documents = nil
	for _, theme := range themes {
		clientDocumentTheme := make(solr.Document)
		clientDocumentTheme.Set("documentID", documentID)
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
	corePrefixes := []Core{DOCUMENTS, ENTITIES, THEMES}

	for _, corePrefix := range corePrefixes {
		core := fmt.Sprintf("%s_%s", corePrefix, clientID)
		si, err := solr.NewSolrInterface(solrUrl, core)
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

	return ReloadAll()
}

// @Deprecated
//func addCDE() {
//	cde := "clientdocumententities_" + clientIdTest
//	//if !CoreExists(cde) {
//	//	CreateCore(cde)
//	//}
//	si, err := solr.NewSolrInterface(solrUrl, cde)
//	check(err)
//
//	var documents []solr.Document
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
//func CDE(id, documentID int, entity, englishEntity string, positions []int) solr.Document {
//	d := make(solr.Document)
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
//	si, err := solr.NewSolrInterface(solrUrl, cdt)
//	check(err)
//
//	var documents []solr.Document
//	documents = append(documents, CDT(1, 1, 8))
//	documents = append(documents, CDT(2, 2, 8))
//
//	sur, err := si.Add(documents, 1, nil)
//	check(err)
//
//	fmt.Println(sur.Result)
//}
//func CDT(id int, documentID, themeID int) solr.Document {
//	d := make(solr.Document)
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
//	si, err := solr.NewSolrInterface(solrUrl, ci)
//	check(err)
//
//	var documents []solr.Document
//	documents = append(documents, CI("daskpdn228dpasud", "my_first_index"))
//
//	sur, err := si.Add(documents, 1, nil)
//	check(err)
//
//	fmt.Println(sur.Result)
//}
//func CI(apikey, indexName string) solr.Document {
//	d := make(solr.Document)
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
//	si, err := solr.NewSolrInterface(solrUrl, cd)
//	check(err)
//
//	var documents []solr.Document
//	documents = append(documents, CD(1, "my document2", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.", "en", "2018-11-17 17:03:16.148444", "neu"))
//	documents = append(documents, CD(2, "my document2", "Just look at any bootcamps for devs this days , it's basically always JS   MongoDB. Not Postgres or MySQL , Mongo. Mongo has become the new MySQL for a lot of devs these days.", "en", "2018-11-17 17:17:42.764131", "pos"))
//
//	sur, err := si.Add(documents, 1, nil)
//	check(err)
//
//	fmt.Println(sur.Result)
//}
//func CD(documentid int, documenttitle, document, languagecode, created, sentiment string) solr.Document {
//	d := make(solr.Document)
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
	ca, err := solr.NewCoreAdmin(solrUrl)

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
	ca, err := solr.NewCoreAdmin(solrUrl)
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
	ca, err := solr.NewCoreAdmin(solrUrl)
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
	ca, err := solr.NewCoreAdmin(solrUrl)
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
	ca, err := solr.NewCoreAdmin(solrUrl)
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
