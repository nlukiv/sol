package backend_interface

import (
	"time"
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
