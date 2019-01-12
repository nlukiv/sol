package main

import (
	"./util"
	"fmt"
	"github.com/vanng822/go-solr/solr"
	"math"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
)

const clientIdTest = "222"

//const solrUrl = "http://185.227.108.145:8983/solr"
const solrUrl = "http://localhost:8983/solr"

type Core string

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
	// GetDocumentIDs(clientId string, cond PropertyCondition, since, until *int) ([]uint64, error)

	// retrieves document entities across al documents
	// if `since` is non-nil and set then query returns entities for documents dated from `since`
	// if `until` is non-nil and set then query returns entities for documents dated before `until`
	// GetEntities(clientId string, since, until *int) ([]*ClientDocumentEntityShort, error)

	// retrieves documents with given IDs
	// if full is not set, only document ID and timestamp should be filled
	// GetDocuments(clientId string, ids []uint64, full bool) ([]*ClientDocument, error)

	// build index with data provided
	// AddDocument(clientId string, doc *ClientDocument, entitiesFound []*Entity, themes []uint64) error

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

}

// checks if BackendStorage exists and connection is available
func CheckConnection() error {

	_, err := StatusAll() // StatusAll method is required for Exists method
	return err
}

// retrieves documents Id for documents that satisfy given condition
// if `since` is non-nil and set then query returns IDs for documents dated from `since`
// if `until` is non-nil and set then query returns IDs for documents dated before `until`
func GetDocumentIDs(clientId string, cond PropertyCondition, since, until *int) ([]uint64, error) {

	cd := "clientdocuments_" + clientId

	si, err := solr.NewSolrInterface(solrUrl, cd)
	if err != nil {
		return nil, err
	}

	type PropertyCondition struct {
		Condition ComparerType
		Value     string
		Property  DocPropertyType
	}

	sinceStr := "*"
	untilStr := "*"
	if since != nil {
		sinceStr = strconv.Itoa(*since)
	}
	if until != nil {
		untilStr = strconv.Itoa(*until)
	}

	var conditionInQuery string
	switch cond.Condition {
	case EQUAL:
		conditionInQuery = fmt.Sprintf("%s:%s", cond.Property, cond.Value)
	case LIKE:
		conditionInQuery = fmt.Sprintf("%s:%s~", cond.Property, cond.Value)
	default:
		return nil, fmt.Errorf("condition not recognized as EQUAL or LIKE")
	}

	query := solr.NewQuery()
	query.Q(fmt.Sprintf("%s AND createdAt:[%s TO %s]", conditionInQuery, sinceStr, untilStr))
	query.Rows(math.MaxInt32)
	query.FieldList("documentID")

	s := si.Search(query)
	r, err := s.Result(nil)
	if err != nil {
		return nil, err
	}

	var documentIDs []uint64
	for _, doc := range r.Results.Docs {
		k := doc.Get("documentID")
		documentIDs = append(documentIDs, uint64(k.([]interface{})[0].(float64)))
	}

	return documentIDs, nil
}

//retrieves document entities across al documents
//if `since` is non-nil and set then query returns entities for documents dated from `since`
//if `until` is non-nil and set then query returns entities for documents dated before `until`
func GetEntities(clientID string, since, until *int) ([]*ClientDocumentEntityShort, error) {
	if *since > *until {
		return nil, fmt.Errorf("since is later than until")
	}

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
	query.Rows(math.MaxInt32) // todo add sophisticated rows handling - pagination
	query.FieldList("documentID AND englishEntities")
	s := si.Search(query)

	r, err := s.Result(nil)
	if err != nil {
		return nil, err
	}
	fmt.Println(len(r.Results.Docs))
	fmt.Println(r.Results.Docs)

	clientDocumentEntitySlice := []*ClientDocumentEntityShort{}
	for _, item := range r.Results.Docs {
		a := item.Get("documentID").([]interface{})[0].(float64)
		fmt.Printf("%f\n", a)
		docID := uint64(a)
		englishEntities := item.Get("englishEntities").([]interface{})
		for _, entity := range englishEntities {
			clientDocumentEntity := ClientDocumentEntityShort{docID, entity.(string)}
			clientDocumentEntitySlice = append(clientDocumentEntitySlice, &clientDocumentEntity)
		}
	}

	return clientDocumentEntitySlice, nil
}

// retrieves documents with given IDs
// if full is not set, only document ID and timestamp should be filled
func GetDocuments(clientId string, ids []uint64, full bool) ([]*ClientDocument, error) {
	cd := "clientdocuments_" + clientId

	si, err := solr.NewSolrInterface(solrUrl, cd)
	if err != nil {
		return nil, err
	}

	clientDocumentsSlice := []*ClientDocument{}

	chunks, err := splitToChunks(ids, 300)
	if err != nil {
		return nil, err
	}

	for _, chunk := range chunks {
		query := solr.NewQuery()
		query.Q(fmt.Sprintf("documentID:(%s)", strings.Join(chunk, " OR ")))
		query.Rows(math.MaxInt32) // todo add sophisticated row
		s := si.Search(query)

		r, err := s.Result(nil)
		if err != nil {
			return nil, err
		}

		for _, item := range r.Results.Docs {

			var title, document, lang, sentiment string

			if full {
				title = item.Get("documentTitle").([]interface{})[0].(string)
				document = item.Get("document").([]interface{})[0].(string)
				lang = item.Get("languageCode").([]interface{})[0].(string)
				sentiment = item.Get("sentiment").([]interface{})[0].(string)

			}

			cds := ClientDocumentShort{DocumentID: uint64(item.Get("documentID").([]interface{})[0].(float64))}

			doc := ClientDocument{
				ClientDocumentShort: cds,
				DocumentTitle:       title,
				Document:            document,
				LanguageCode:        lang,
				CreatedAt:           item.Get("createdAt").([]interface{})[0].(int),
				Sentiment:           sentiment,
			}

			clientDocumentsSlice = append(clientDocumentsSlice, &doc)
		}
	}

	return clientDocumentsSlice, nil

}

func splitToChunks(slice []uint64, chunkSize int) ([][]string, error) {
	if chunkSize < 1 {
		return nil, fmt.Errorf("invalid chunkSize input")
	}

	idsStr := []string{}
	for _, number := range slice {
		idsStr = append(idsStr, strconv.FormatUint(number, 10))
	}

	chunks := [][]string{}

	chunkNumber := len(idsStr) / chunkSize

	for i := 0; i < chunkNumber; i++ {
		chunk := idsStr[i*chunkSize : (i+1)*chunkSize]
		chunks = append(chunks, chunk)
	}

	if len(idsStr)%chunkSize != 0 {
		chunks = append(chunks, idsStr[chunkSize*len(chunks):])
	}

	return chunks, nil
}

// build index with data provided
func AddDocument(clientId string, doc *ClientDocument, entitiesFound []*Entity, themes []uint64) error {

	cd := "clientdocuments_" + clientId

	si, err := solr.NewSolrInterface(solrUrl, cd)
	if err != nil {
		return err
	}

	var englishEntities, entities []string
	for _, entity := range entitiesFound {
		englishEntities = append(englishEntities, entity.UrlPageTitle)
		entities = append(entities, entity.UrlPageTitle)
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
	clientDocument.Set("entities", entities)
	clientDocument.Set("englishEntities", englishEntities)
	documents = append(documents, clientDocument)

	_, err = si.Add(documents, 1, nil)
	if err != nil {
		return err
	}

	err = Reload(cd)
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

	err = Reload(cd)
	if err != nil {
		return err
	}

	return nil
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

// creates new storage for client
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
	err = Reload(ci)
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

	err = Reload(cd)
	if err != nil {
		return err
	}

	return nil
}

// Reload specific Solr core.
// Return type - string of json
func Reload(coreName string) error {
	ca, err := solr.NewCoreAdmin(solrUrl)
	if err != nil {
		return err
	}

	v := url.Values{}
	v.Add("core", coreName)

	_, err = ca.Action("RELOAD", &v)
	if err != nil {
		return err
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
		err = Reload(k)
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
