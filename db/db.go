package db

import (
	"context"
	"errors"
	"log"

	"github.com/sunby/go-hnsw"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

const InternalDocName = "_m_doc"
const InternalIdName = "_m_id"

var (
	ErrCollectionNotFound  = errors.New("collection not found")
	ErrFieldLengthMismatch = errors.New("documents and metadatas are mismatch")
	ErrMetaCorrupted       = errors.New("meta corrupted")
)

type DB struct {
	*mongo.Database
	collections map[string]*Collection
	cfg         *Config
	meta        *Meta
}

func (db *DB) CreateCollection(ctx context.Context, name string) (*Collection, error) {
	err := db.Database.CreateCollection(ctx, name)
	if err != nil {
		return nil, err
	}
	c := db.Database.Collection(name)
	collection, err := NewCollection(ctx, db.meta, c, name, db.cfg)
	if err != nil {
		return nil, err
	}
	db.collections[name] = collection
	return collection, nil
}

func (db *DB) GetCollection(ctx context.Context, name string) (*Collection, error) {
	if collection, ok := db.collections[name]; ok {
		return collection, nil
	}

	collectionNames, err := db.Database.ListCollectionNames(ctx, bson.M{"name": name})
	if err != nil {
		return nil, err
	}

	if collectionNames == nil || len(collectionNames) == 0 {
		return nil, ErrCollectionNotFound
	}

	c := db.Database.Collection(name)
	collection, err := NewCollection(ctx, db.meta, c, name, db.cfg)
	if err != nil {
		return nil, err
	}
	db.collections[name] = collection
	return collection, nil
}

func (db *DB) DropCollection(ctx context.Context, name string) error {
	err := db.Database.Collection(name).Drop(ctx)
	if err != nil {
		return err
	}
	err = db.meta.Drop(ctx, name)
	if err != nil {
		return err
	}
	delete(db.collections, name)
	return nil
}

func NewDB(meta *Meta, docdb *mongo.Database, cfg *Config) *DB {
	return &DB{
		Database:    docdb,
		collections: make(map[string]*Collection),
		cfg:         cfg,
		meta:        meta,
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
	meta      *Meta
	cfg       *Config
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

func getHnswFilterFunc(collection *Collection, filter interface{}) hnsw.FilterFunc {
	return func(id uint32) bool {
		andFilter := bson.D{
			{"$and",
				bson.A{
					bson.D{{InternalIdName, id}},
					filter,
				},
			},
		}
		var doc bson.M
		docres := collection.FindOne(context.TODO(), andFilter)
		if docres.Err() == mongo.ErrNoDocuments {
			return false
		}
		err := docres.Decode(&doc)
		if err != nil {
			log.Printf("decode error: %v", err)
			return false
		}
		return true
	}
}
func (c *Collection) Query(document string, k int, filter interface{}) ([]bson.M, error) {
	embedding, err := c.embedding.GetEmbedding([]string{document})
	if err != nil {
		return nil, err
	}
	ids := c.index.Search(embedding[0], 10*k, k, getHnswFilterFunc(c, filter))
	var res []bson.M
	for {
		item := ids.Pop()
		if item == nil {
			break
		}
		filter := bson.D{{InternalIdName, item.ID}}
		docres := c.FindOne(context.TODO(), filter)
		var doc bson.M
		if docres.Err() == mongo.ErrNoDocuments {
			log.Print("can not find document with id: ", item.ID)
			continue
		}
		err := docres.Decode(&doc)
		if err != nil {
			log.Printf("decode error: %v", err)
			continue
		}
		res = append(res, doc)
	}
	return res, nil
}

func (c *Collection) getNextID(ctx context.Context) (uint32, error) {
	sortStage := bson.D{{"$sort", bson.D{{InternalIdName, 1}}}}
	limitStage := bson.D{{"$limit", 1}}
	r, err := c.Collection.Aggregate(ctx, mongo.Pipeline{sortStage, limitStage})
	if err != nil {
		return 0, err
	}

	if r.Err() == mongo.ErrNoDocuments {
		return 0, nil
	}

	if r.Err() != nil {
		log.Printf("error: %v", r.Err())
	} else {
		log.Printf("there is no error for aggregate")
	}

	var res []bson.M
	if err := r.All(ctx, &res); err != nil {
		return 0, err
	}

	if len(res) == 0 {
		return 0, nil
	}

	return res[0]["max"].(uint32), nil
}

func (c *Collection) loadIndexIfExists(ctx context.Context) error {
	m := c.meta.Read(ctx, c.name)
	if m.Err() == mongo.ErrNoDocuments {
		c.index = NewHNSWIndex(&c.cfg.HNSW, CohereModel2Dim[c.cfg.Cohere.Model])
		return nil
	}

	var res bson.M
	if err := m.Decode(&res); err != nil {
		return err
	}

	pathValue := res[HnswIndexPathKey]
	idValue := res[HnswIdKey]
	sizeValue := res[HnswSizeKey]

	if pathValue == nil || idValue == nil || sizeValue == nil {
		return ErrMetaCorrupted
	}

	indexfile := pathValue.(string)
	id := idValue.(uint32)
	size := sizeValue.(uint32)
	index, err := NewHNSWIndexFromFile(indexfile, size, id)
	if err != nil {
		return err
	}
	c.index = index
	return nil
}

func (c *Collection) init(ctx context.Context) error {
	id, err := c.getNextID(ctx)
	if err != nil {
		return err
	}
	c.id = id

	if err := c.loadIndexIfExists(ctx); err != nil {
		return err
	}

	return nil
}

func NewCollection(ctx context.Context, meta *Meta, collection *mongo.Collection, name string, cfg *Config) (*Collection, error) {
	embedding, err := NewCohereEmbedding(&cfg.Cohere)
	if err != nil {
		return nil, err
	}
	c := &Collection{
		Collection: collection,
		index:      NewHNSWIndex(&cfg.HNSW, CohereModel2Dim[cfg.Cohere.Model]),
		name:       name,
		embedding:  embedding,
		meta:       meta,
		cfg:        cfg,
	}

	err = c.init(ctx)
	if err != nil {
		return nil, err
	}
	return c, nil
}
