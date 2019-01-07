package main

import (
	"./util"
	"encoding/json"
	"fmt"
	"github.com/vanng822/go-solr/solr"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"time"
)

const clientIdTest = "4444"

//const solrUrl = "http://185.227.108.145:8983/solr"
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
	DocumentTitle string
	Document      string
	LanguageCode  string
	CreatedAt     int
	Sentiment     string
}

type ClientDocumentShort struct {
	DocumentID uint64
}

type ClientDocumentEntity struct {
	ClientDocumentEntityShort

	ID        uint64
	Entity    string
	Positions Positions
}

type ClientDocumentEntityShort struct {
	DocumentID    uint64
	EnglishEntity string
}

type Positions []int

type Position struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

type Entity struct {
	Phrase          string      `json:"title"`
	Title           string      `json:"full_title"`
	Classifications []string    `json:"classifications"`
	Positions       []*Position `json:"positions"`
	UrlPageTitle    string
	inbound         int
	checkContext    bool
	doc2vecID       string
	aliases         []string
}

type ClientStorage interface {
	// checks if ClientStorage exists and connection is ok
	//CheckConnection() error

	// retrieves documents Id for documents that satisfy given condition
	// if `since` is non-nil and set then query returns IDs for documents dated from `since`
	// if `until` is non-nil and set then query returns IDs for documents dated before `until`
	GetDocumentIDs(clientId string, cond PropertyCondition, since, until *int) ([]uint64, error)

	// retrieves document entities across al documents
	// if `since` is non-nil and set then query returns entities for documents dated from `since`
	// if `until` is non-nil and set then query returns entities for documents dated before `until`
	GetEntities(clientId string, since, until *int) ([]*ClientDocumentEntityShort, error)

	// retrieves documents with given IDs
	// if full is not set, only document ID and timestamp should be filled
	GetDocuments(clientId string, ids []uint64, full bool) ([]*ClientDocument, error)

	// build index with data provided
	AddDocument(clientId string, doc *ClientDocument, entitiesFound []*Entity, themes []uint64) error

	// deletes document with 'docID' from client 'clientID' collection
	// DeleteDocument(clientID, docID string) error

	// checks that client's storage exists
	// Exists(clientID string) bool

	// creates new storage for client
	// Create(clientId, apikey, index string) error

	// returns all indices stored for account with ApiKey
	// GetIndicesForAccount(apikey string) ([]string, error)

	// drops entire index with 'indexID'
	// DeleteIndex(indexID, clientID, apiKey string) error
}

func main() {

	//pc := PropertyCondition{Condition: LIKE, Value: "keu", Property: SENTIMENT}
	//
	//di, err := GetDocumentIDs(clientIdTest, pc)
	//check(err)
	//
	//fmt.Println(di)
	//a, e := GetEntities(clientIdTest)
	//check(e)
	//fmt.Println(a)
	//ids := []uint64{321, 123}
	//docs, _ := GetDocuments(clientIdTest, ids)
	//
	//fmt.Println(docs)
	//DeleteIndex(clientIdTest)
	//write()
	//DeleteDocument("345", clientIdTest)
	rand.Seed(time.Now().UnixNano())

	err := Create(clientIdTest, "apikey"+util.RandStringBytes(16), "index"+util.RandStringBytes(8))
	check(err)

	for i := 0; i < 3; i++ {
		write()
	}
	//_, err := GetEntities(clientIdTest,nil,nil)
	//check(err)

}

// get docs with sentiment=neu: PropertyCondition.Condition=EQUAL PropertyCondition.Value=neu, PropertyCondition.Property=sentiment
func GetDocumentIDs(clientID string, cond PropertyCondition, since, until *int) ([]uint64, error) {

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
	si, err := solr.NewSolrInterface(solrUrl, core)
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

//func GetEntities(clientID string) ([]*ClientDocumentEntity, error)

//retrieves document entities across al documents
//if `since` is non-nil and set then query returns entities for documents dated from `since`
//if `until` is non-nil and set then query returns entities for documents dated before `until`
func GetEntities(clientID string, since, until *int) ([]*ClientDocumentEntityShort, error) {
	if *since > *until {
		return nil, fmt.Errorf("since is later than until")
	}

	var entities []*ClientDocumentEntityShort

	si, err := solr.NewSolrInterface(solrUrl, fmt.Sprintf("clientdocuments_%s", clientID))
	if err != nil {
		return nil, err
	}

	sinceStr := "*"
	untilStr := "*"
	if since != nil {
		sinceStr = strconv.Itoa(*since)
	}
	if until != nil {
		untilStr = strconv.Itoa(*until)
	}

	query := solr.NewQuery()
	query.Q(fmt.Sprintf("createdAt:[%s TO %s]", sinceStr, untilStr))
	query.Rows(2147483647) // todo add sophisticated rows handling - pagination
	s := si.Search(query)

	r, err := s.Result(nil)
	if err != nil {
		return nil, err
	}
	fmt.Println(len(r.Results.Docs))

	data, err := json.Marshal(r.Results.Docs)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(data))

	var doc []solr.Document
	err = json.Unmarshal(data, &doc)
	if err != nil {
		return nil, err
	}
	//fmt.Println(doc)

	//for _, item := range r.Results.Docs {
	//	docID := uint64(item.Get("documentID").([]interface{})[0].(float64))
	//	englishEntity := item.Get("englishEntity").([]interface{})[0].(string)
	//	entity := item.Get("entity").([]interface{})[0].(string)
	//	positions := util.ToIntSlice(item.Get("positions").([]interface{}))
	//
	//	cdes := ClientDocumentEntityShort{docID, englishEntity}
	//
	//	clientDocumentEntity := ClientDocumentEntityShort{
	//		cdes,
	//		docID, // todo not sure if this ID is required here
	//		entity,
	//		positions,
	//	}
	//
	//	entities = append(entities, &clientDocumentEntity)
	//}
	//
	//for i, a := range entities {
	//	fmt.Printf("%d:  %t\n", i, a)
	//}
	return entities, nil
}

// get all documents for specified ids

//todo
//func GetDocuments(clientID string, ids []uint64) ([]*ClientDocument, error) {
//	var docs []*ClientDocument
//
//	si, err := solr.NewSolrInterface("http://localhost:8983/solr", fmt.Sprintf("%s_%s", DOCUMENTS, clientID))
//	if err != nil {
//		return nil, err
//	}
//
//	for _, id := range ids {
//
//		// another possible implementation is to not send each id in a separate query, but
//		// use filterQuery and put all ids in there. example
//		// http://localhost:8983/solr/clientdocuments_2840/select?fq=documentID:(123%20OR%20234)&q=*:*
//		// downside - this way, queries may get TOO long if the id list is long
//		// https://stackoverflow.com/questions/7594915/apache-solr-or-in-filter-query
//
//		query := solr.NewQuery()
//		query.Q(fmt.Sprintf("documentID:%d", id))
//		s := si.Search(query)
//		r, err := s.Result(nil)
//		if err != nil {
//			return nil, err
//		}
//		//todo handle absence of document for current id
//		doc := r.Results.Docs[0] // we definitely get one element, since querying by unique id
//		docID := doc.Get("documentID")
//		title := doc.Get("documentTitle").(string)
//		document := doc.Get("document").(string)
//		lang := doc.Get("languageCode").(string)
//		created, err := util.StringToTime(doc.Get("created").(string))
//		if err != nil {
//			return nil, err
//		}
//		sentiment := doc.Get("sentiment").(string)
//
//		cds := ClientDocumentShort{docID.(uint64)}
//		clientDocument := ClientDocument{
//			cds,
//			title,
//			document,
//			lang,
//			created,
//			sentiment,
//		}
//
//		docs = append(docs, &clientDocument)
//	}
//
//	return docs, nil
//}

// checks if BackendStorage exists and connection is available
func CheckConnection() error {
	_, err := StatusAll()
	return err
}

// checks that client's storage exists. Creates new one if not

//func EnsureClientStorage(clientID string) error {

//	corePrefixes := []Core{DOCUMENTS, ENTITIES, THEMES}
//
//	for _, elem := range corePrefixes {
//		core := fmt.Sprintf("%s_%s", elem, clientID)
//
//		coreExists, err := Exists(core)
//		if err != nil {
//			return err
//		}
//
//		if !coreExists {
//			_, err := Create(core)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}

// drops entire index with 'clientID'
func DeleteIndex(indexID, clientID, apiKey string) error {
	cd := "clientdocuments_" + clientID

	coreExists, err := Exists(cd)
	if err != nil {
		return err
	}

	if coreExists {
		_, err := DeleteCore(cd, true)
		if err != nil {
			return err
		}
	}

	ci := "clientindices"

	si, err := solr.NewSolrInterface(solrUrl, ci)
	if err != nil {
		return err
	}

	params := &url.Values{}
	params.Add("commit", "true")

	_, err = si.Delete(map[string]interface{}{"query": fmt.Sprintf("apikey:%s AND index:%s", apiKey, indexID)}, params)
	if err != nil {
		return err
	}

	_, err = Reload(cd)
	return err

	return nil
}

// returns all indices stored for account with ApiKey
func GetIndicesForAccount(apikey string) ([]string, error) {
	var indices []string

	ci := "clientindices"

	si, err := solr.NewSolrInterface(solrUrl, ci)
	if err != nil {
		return []string{}, err
	}

	query := solr.NewQuery()
	query.Q(fmt.Sprintf("apikey:%s", apikey))
	query.Rows(2147483647) // todo add sophisticated rows handling - pagination
	s := si.Search(query)
	r, err := s.Result(nil)
	if err != nil {
		return nil, err
	}

	for _, item := range r.Results.Docs {
		index := item.Get("index").([]interface{})
		indices = append(indices, index[0].(string))
	}

	return indices, nil

}

//// Add document to client index
//func AddDocument(documentID uint64, clientID string, title, text, languageCode, sentiment string,
//	entitiesFound []util.Entity, themes []string) error

// build index with data provided
func AddDocument(clientId string, doc *ClientDocument, entitiesFound []*Entity, themes []uint64) error {

	cd := "clientdocuments_" + clientId

	si, err := solr.NewSolrInterface(solrUrl, cd)
	if err != nil {
		return err
	}

	cde := []solr.Document{}
	ee := make(solr.Document)
	for _, entity := range entitiesFound {
		var positions []int
		for _, pos := range entity.Positions {
			positions = append(positions, pos.Start, pos.End)
		}
		e := make(solr.Document)
		e.Set("DocumentID", doc.DocumentID)
		e.Set("EnglishEntity", entity.UrlPageTitle)
		e.Set("ID", util.GetID())
		e.Set("Entity", entity.UrlPageTitle)
		e.Set("Positions", positions)
		cde = append(cde, e)
		ee = e
	}

	var documents []solr.Document
	clientDocument := make(solr.Document)
	clientDocument.Set("documentID", doc.DocumentID)
	clientDocument.Set("documentTitle", doc.DocumentTitle)
	clientDocument.Set("document", doc.Document)
	clientDocument.Set("languageCode", doc.LanguageCode)
	clientDocument.Set("createdAt", doc.CreatedAt)
	clientDocument.Set("sentiment", doc.Sentiment)
	clientDocument.Set("themes", themes)
	clientDocument.Set("entities", cde)
	clientDocument.Set("entityTry", ee)
	documents = append(documents, clientDocument)

	_, err = si.Add(documents, 1, nil)
	if err != nil {
		return err
	}

	//
	////2. Створється запис в табличку `clientdocumententities_%indexID%` з наступними полями:
	//// `documentID, entity, englishEntity, positions`. всі дані беруться з аргументу `entitiesFound []*entities.Entity`
	//
	//cde := "clientdocumententities_" + clientID
	//si, err = solr.NewSolrInterface(solrUrl, cde)
	//if err != nil {
	//	return err
	//}
	//
	//documents = nil
	//for _, entity := range entitiesFound {
	//	clientDocumentEntity := make(solr.Document)
	//	clientDocumentEntity.Set("documentID", documentID)
	//	clientDocumentEntity.Set("entity", entity.Entity)
	//	clientDocumentEntity.Set("englishEntity", entity.EnglishEntity)
	//	clientDocumentEntity.Set("positions", entity.Positions)
	//	documents = append(documents, clientDocumentEntity)
	//}
	//
	//_, err = si.Add(documents, 1, nil)
	//if err != nil {
	//	return err
	//}
	//
	//// 3. Створється запис в табличку `clientdocumentthemes_%indexID%` з наступними полями: `themeid, documentID`. (edited)
	//
	//cdt := "clientdocumentthemes_" + clientID
	//
	//si, err = solr.NewSolrInterface(solrUrl, cdt)
	//if err != nil {
	//	return err
	//}
	//
	//documents = nil
	//for _, theme := range themes {
	//	clientDocumentTheme := make(solr.Document)
	//	clientDocumentTheme.Set("documentID", documentID)
	//	clientDocumentTheme.Set("themeID", theme)
	//	documents = append(documents, clientDocumentTheme)
	//}

	_, err = Reload(cd)
	if err != nil {
		return err
	}

	return nil
}

// deletes document with 'documentID' from client 'clientID' collection
func DeleteDocument(documentID, clientID string) error {

	cd := "clientdocuments_" + clientID

	si, err := solr.NewSolrInterface(solrUrl, cd)
	if err != nil {
		return err
	}

	params := &url.Values{}
	params.Add("commit", "true")

	_, err = si.Delete(map[string]interface{}{"query": "documentID:" + documentID}, params)
	if err != nil {
		return err
	}

	_, err = Reload(cd)
	return err
}

func typeof(v interface{}) string {
	return reflect.TypeOf(v).String()
}

// needs more work (?)
// http://localhost:8983/solr/admin/cores?action=CREATE&name=cot&instanceDir=cot&configSet=_default
// https://stackoverflow.com/questions/21619947/create-new-cores-in-solr-via-http
func Create(clientId, apikey, index string) error {
	ca, err := solr.NewCoreAdmin(solrUrl)
	if err != nil {
		return err
	}

	// add apikey and index to clientindices
	// start of clientindices region
	clientIndicesExist, err := Exists("clientindices")
	if err != nil {
		return err
	}

	if !clientIndicesExist {
		v := url.Values{}
		v.Add("name", "clientindices")
		v.Add("instanceDir", "clientindices")
		v.Add("configSet", "_default")

		_, err := ca.Action("CREATE", &v)
		if err != nil {
			return err
		}
	}

	ci := "clientindices"

	indicescore, err := solr.NewSolrInterface(solrUrl, ci)
	if err != nil {
		return err
	}

	var documents []solr.Document
	clientIndice := make(solr.Document)
	clientIndice.Set("apikey", apikey)
	clientIndice.Set("index", index)
	documents = append(documents, clientIndice)

	_, err = indicescore.Add(documents, 1, nil)
	if err != nil {
		return err
	}
	_, err = Reload(ci)
	if err != nil {
		return err
	}
	// end of clientindices region

	clientDocumentsExist, err := Exists("clientdocuments_" + clientId)
	if err != nil {
		return err
	}

	if !clientDocumentsExist {
		v := url.Values{}
		v.Add("name", "clientdocuments_"+clientId)
		v.Add("instanceDir", "clientdocuments_"+clientId)
		v.Add("configSet", "_default")

		_, err = ca.Action("CREATE", &v)
		if err != nil {
			return err
		}
	}

	return nil
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
func Exists(core string) (bool, error) {
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

func randomint(min int, max int) int {
	return rand.Intn(max-min) + min
}

func randombool() bool {
	return randomint(1, 100) > 50
}

func write() {

	cd := ClientDocument{
		ClientDocumentShort: ClientDocumentShort{DocumentID: util.GetID()},
		DocumentTitle:       fmt.Sprintf("Document-Title-%s", util.RandStringBytes(4)),
		Document:            fmt.Sprintf("Document-%s", util.RandStringBytes(64)),
		LanguageCode:        fmt.Sprintf("LanguageCode-%s", util.RandStringBytes(2)),
		CreatedAt:           randomint(1, 100),
		Sentiment:           fmt.Sprintf("Sentiment-%s", util.RandStringBytes(2)),
	}

	//type Entity struct {
	//	Phrase          string      `json:"title"`
	//	Title           string      `json:"full_title"`
	//	Classifications []string    `json:"classifications"`
	//	Positions       []*Position `json:"positions"`
	//	UrlPageTitle    string
	//	inbound         int
	//	checkContext    bool
	//	doc2vecID       string
	//	aliases         []string
	//}

	esf := []*Entity{}
	for i := 0; i < randomint(3, 7); i++ {

		poss := []*Position{}
		end := 1
		for i := 0; i < randomint(3, 7); i++ {
			start := randomint(end, end+100)
			end = randomint(start, start+100)
			p := Position{Start: start, End: end}
			poss = append(poss, &p)
		}

		e := Entity{
			Phrase:          fmt.Sprintf("Phrase-%s", util.RandStringBytes(16)),
			Title:           fmt.Sprintf("Title-%s", util.RandStringBytes(4)),
			Classifications: []string{util.RandStringBytes(8), util.RandStringBytes(8), util.RandStringBytes(8), util.RandStringBytes(8)},
			Positions:       poss,
			UrlPageTitle:    fmt.Sprintf("UrlPageTitle-%s", util.RandStringBytes(2)),
			inbound:         randomint(1, 100),
			checkContext:    randombool(),
			doc2vecID:       util.RandStringBytes(8),
			aliases:         []string{util.RandStringBytes(8), util.RandStringBytes(8), util.RandStringBytes(8), util.RandStringBytes(8)},
		}
		esf = append(esf, &e)
	}

	ts := []uint64{}
	for i := 0; i < randomint(3, 17); i++ {
		ts = append(ts, util.GetID())
	}

	err := AddDocument(clientIdTest, &cd, esf, ts)
	check(err)

	//func AddDocument(clientId string, doc *ClientDocument, entitiesFound []*Entity, themes []uint64) error {

}
