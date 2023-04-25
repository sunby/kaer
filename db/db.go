package db

import (
	"context"
	"errors"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const InternalDocName = "_m_doc"
const InternalIdName = "_m_id"

var ErrFieldLengthMismatch error = errors.New("documents and metadatas are mismatch")

type DB struct {
	*mongo.Database
	collections map[string]*Collection
	cfg         *Config
}

func (db *DB) CreateCollection(name string) (*Collection, error) {
	c := db.Database.Collection(name)
	return NewCollection(c, name, db.cfg)
}

func NewDB(docdb *mongo.Database, cfg *Config) *DB {
	return &DB{
		Database:    docdb,
		collections: make(map[string]*Collection),
		cfg:         cfg,
	}
}

type Data struct {
	documents []string
	metadatas []bson.M
}

func (d *Data) Documents(documents []string) *Data {
	d.documents = documents
	return d
}

func (d *Data) Metadatas(metadatas []bson.M) *Data {
	d.metadatas = metadatas
	return d
}

type Collection struct {
	*mongo.Collection
	name      string
	index     *HNSWIndex
	embedding *CohereEmbedding
	id        uint32
}

func (c *Collection) Insert(data *Data) error {
	if len(data.documents) != len(data.metadatas) {
		return ErrFieldLengthMismatch
	}

	var insertions []interface{}
	for i, doc := range data.metadatas {
		doc[InternalDocName] = data.documents[i]
		c.id++
		doc[InternalIdName] = c.id
		insertions = append(insertions, doc)
	}

	_, err := c.InsertMany(context.TODO(), insertions)
	if err != nil {
		return err
	}

	embeddings, err := c.getEmbeddings(data)
	if err != nil {
		return err
	}

	for i, embedding := range embeddings {
		c.index.Add(embedding, data.metadatas[i][InternalIdName].(uint32))
	}

	return nil
}

func (c *Collection) getEmbeddings(data *Data) ([][]float32, error) {
	res := make([][]float32, 0, len(data.documents))
	i := 0
	l := len(data.documents)
	for ; i < l; i += CohereMaxTexts {
		if l-i <= CohereMaxTexts {
			e, err := c.embedding.GetEmbedding(data.documents[i:l])
			if err != nil {
				return nil, err
			}
			res = append(res, e...)
			break
		}
		e, err := c.embedding.GetEmbedding(data.documents[i : i+CohereMaxTexts])
		if err != nil {
			return nil, err
		}
		res = append(res, e...)

	}
	return res, nil
}

func (c *Collection) Query(document string, k int, filter interface{}) ([]bson.M, error) {
	embedding, err := c.embedding.GetEmbedding([]string{document})
	if err != nil {
		return nil, err
	}
	ids := c.index.Search(embedding[0], 10*k, k)
	if err != nil {
		return nil, err
	}
	var res []bson.M
	for {
		item := ids.Pop()
		log.Printf("result: %v", item.ID)
		if item == nil {
			break
		}
		andFilter := bson.D{
			{"$and",
				bson.A{
					bson.D{{InternalIdName, item.ID}},
					filter,
				},
			},
		}
		var doc bson.M
		docres := c.FindOne(context.Background(), andFilter)
		if docres.Err() == mongo.ErrNoDocuments {
			continue
		}
		err := docres.Decode(&doc)
		if err != nil {
			return nil, err
		}
		res = append(res, doc)
		if len(res) == k {
			return res, nil
		}
	}
	return res, nil
}

func NewCollection(collection *mongo.Collection, name string, cfg *Config) (*Collection, error) {
	embedding, err := NewCohereEmbedding(&cfg.Cohere)
	if err != nil {
		return nil, err
	}
	return &Collection{
		Collection: collection,
		index:      NewHNSWIndex(&cfg.HNSW, CohereModel2Dim[cfg.Cohere.Model]),
		name:       name,
		embedding:  embedding,
		id:         0,
	}, nil
}
