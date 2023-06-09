package db

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/RoaringBitmap/roaring"
	"github.com/sunby/kaer/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	InternalDocName       = "_m_doc"
	InternalIdName        = "_m_id"
	InternalEmbeddingName = "_m_embedding"
	MetaPersistBatch      = 1000
)

var (
	ErrCollectionNotFound  = errors.New("collection not found")
	ErrFieldLengthMismatch = errors.New("documents and metadatas are mismatch")
	ErrMetaCorrupted       = errors.New("meta corrupted")
)

type Data struct {
	documents []string
	metadatas []bson.M
}

func NewData() *Data {
	return &Data{}
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
	name          string
	index         *HNSWIndex
	embedding     *CohereEmbedding
	id            uint32
	nextPersistId uint32
	meta          *Meta
	cfg           *config.Config
}

func (c *Collection) Insert(data *Data) error {
	if len(data.documents) != len(data.metadatas) {
		return ErrFieldLengthMismatch
	}

	log.Printf("embdedding: %v", c.embedding)
	embeddings, err := c.embedding.GetEmbedding(data.documents)
	if err != nil {
		return err
	}

	// insert into document database
	var insertions []interface{}
	for i, doc := range data.metadatas {
		doc[InternalDocName] = data.documents[i]
		c.id++
		doc[InternalIdName] = c.id
		doc[InternalEmbeddingName] = embeddings[i]
		insertions = append(insertions, doc)
	}

	_, err = c.InsertMany(context.TODO(), insertions)
	if err != nil {
		return err
	}

	// update hnsw index
	for i, embedding := range embeddings {
		c.index.Add(embedding, data.metadatas[i][InternalIdName].(uint32))
	}

	if c.id > c.nextPersistId {
		// TODO: put this in a background goroutine
		c.nextPersistId += MetaPersistBatch
		if err := c.persistMeta(context.Background()); err != nil {
			return err
		}
	}

	return nil
}

func (c *Collection) persistMeta(ctx context.Context) error {
	indexFile := fmt.Sprintf("%s/%s/%s_%d.hnsw", c.cfg.DB.Dir, "index", c.name, c.id)
	if err := c.index.Save(indexFile); err != nil {
		return err
	}
	info := newMetaInfo(c.name, indexFile, c.index.ID(), c.index.Size())
	return c.meta.Write(ctx, info)
}

func (c *Collection) Query(document string, k int, filter interface{}) ([]bson.M, error) {
	// first query in document database
	cursor, err := c.Collection.Find(context.TODO(), filter)
	if err != nil {
		return nil, err
	}
	var filteredData []bson.M
	if err := cursor.All(context.TODO(), &filteredData); err != nil {
		return nil, err
	}

	// create roaring bitmap
	bitset := roaring.NewBitmap()
	documents := make(map[uint32]bson.M)
	for _, doc := range filteredData {
		docId := (uint32)(doc[InternalIdName].(int64))
		bitset.Add(docId)
		documents[docId] = doc
	}

	embedding, err := c.embedding.GetEmbedding([]string{document})
	if err != nil {
		return nil, err
	}

	// search in hnsw index
	ids := c.index.Search(embedding[0], 200, k, bitset.Contains)
	var res []bson.M
	for item := ids.Pop(); item != nil; item = ids.Pop() {
		res = append(res, documents[item.ID])
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

	log.Printf("max id: %d", res[0][InternalIdName].(int64))
	return (uint32)(res[0][InternalIdName].(int64)), nil
}

func (c *Collection) loadIndexIfExists(ctx context.Context) error {
	m, err := c.meta.Read(ctx, c.name)
	if err == mongo.ErrNoDocuments {
		c.index = NewHNSWIndex(&c.cfg.HNSW, config.CohereModel2Dim[c.cfg.Cohere.Model])
		return nil
	}

	index, err := NewHNSWIndexFromFile(m.hnswFile, m.hnswSize, m.hnswId)
	if err != nil {
		return err
	}
	c.index = index
	return nil
}

func (c *Collection) updateIndexFromLastId(ctx context.Context) error {
	// find id >= lastIndexId by sort
	filter := bson.D{{InternalIdName, bson.D{{"$gt", c.index.ID()}}}}
	cursor, err := c.Collection.Find(ctx, filter, options.Find().SetSort(bson.D{{InternalIdName, 1}}))
	if err != nil {
		return err
	}
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return err
		}

		c.index.Add(Convert2Float32(doc[InternalEmbeddingName].(primitive.A)), (uint32)(doc[InternalIdName].(int64)))
	}
	return nil
}

func (c *Collection) init(ctx context.Context) error {
	id, err := c.getNextID(ctx)
	if err != nil {
		return err
	}
	c.id = id
	c.nextPersistId = c.id + MetaPersistBatch

	if err := c.loadIndexIfExists(ctx); err != nil {
		return err
	}

	if err := c.updateIndexFromLastId(ctx); err != nil {
		return err
	}

	return nil
}

func NewCollection(ctx context.Context, meta *Meta, collection *mongo.Collection, name string, cfg *config.Config) (*Collection, error) {
	embedding, err := NewCohereEmbedding(&cfg.Cohere)
	if err != nil {
		return nil, err
	}
	c := &Collection{
		Collection: collection,
		index:      NewHNSWIndex(&cfg.HNSW, config.CohereModel2Dim[cfg.Cohere.Model]),
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
