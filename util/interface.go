package solr

type Bs interface {
	// checks if storage exists and connection is available
	CheckConnection() error

	// checks that client's storage exists. Creates new one if not
	EnsureClientStorage(clientID string) error

	// build index with data provided
	Index(title, text, languageCode, sentiment,
		clientId string, entitiesFound /*[]*entities.Entity*/, themes []string) error

	// deletes document with 'docID' from client 'clientID' collection
	DeleteDocument(docID, clientID string) error

	// drops entire index with name 'index'
	DeleteIndex(index, clientID, apiKey string) error
}
