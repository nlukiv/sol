package solr

import (
//"repustate-deepsearch/storage/domain"
)

type DocPropertyType int

const (
	LANGUAGE DocPropertyType = iota
	SENTIMENT
	THEME
	ENTITY
)

type ComparerType int

const (
	EQUAL ComparerType = iota
	LIKE
)

type PropertyCondition struct {
	Condition ComparerType
	Value     string
	Property  DocPropertyType
}

type BackendStorage0 interface {
	// checks if BackendStorage exists and connection is available
	CheckConnection() error

	// checks that client's storage exists. Creates new one if not
	EnsureClientStorage(clientID string) error

	// build index with data provided
	AddDocument(indexID string, title, text, languageCode, sentiment,
		clientId string, entitiesFound /*[]*entities.Entity*/, themes []string) error

	// deletes document with 'docID' from client 'clientID' collection
	DeleteDocument(indexID, docID, clientID string) error

	// drops entire index with 'indexID'
	DeleteIndex(indexID, clientID, apiKey string) error

	// Support client' entities retrieval
	ClientDataGetter
}

type ClientDataGetter interface {
	GetDocumentIDs(indexID string, cond PropertyCondition) ([]uint64, error)

	//GetEntities(indexID string, cond PropertyCondition) ([]*domain.ClientDocumentEntity, error)

	//GetDocuments(indexID string, ids []uint64) ([]*domain.ClientDocument, error)
}

type QueryParams struct {
	Order string
	Limit int
	Skip  int
	Take  int
}
