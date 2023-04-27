package db

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type metaInfo struct {
	collection string `bson: collection`
	hnswFile   string `bson: hnsw_file`
	hnswId     uint32 `bson: hnsw_id`
	hnswSize   uint32 `bson: hnsw_size`
}

func newMetaInfo(collection string, hnswFile string, hnswId uint32, hnswSize uint32) metaInfo {
	return metaInfo{
		collection: collection,
		hnswFile:   hnswFile,
		hnswId:     hnswId,
		hnswSize:   hnswSize,
	}
}

const metaDatabase = "_m_meta_db"
const metaCollection = "_m_meta_collection"

type Meta struct {
	client *mongo.Client
}

func (m *Meta) Write(ctx context.Context, info metaInfo) error {
	collection := m.client.Database(metaDatabase).Collection(metaCollection)
	filter := bson.M{"collection": info.collection}
	_, err := collection.ReplaceOne(ctx, filter, info, options.Replace().SetUpsert(true))
	if err != nil {
		return err
	}
	return nil
}

func (m *Meta) Read(ctx context.Context, collectionName string) (metaInfo, error) {
	collection := m.client.Database(metaDatabase).Collection(metaCollection)
	filter := bson.M{"collection": collectionName}
	var info metaInfo
	err := collection.FindOne(ctx, filter).Decode(&info)
	return info, err
}

func (m *Meta) Drop(ctx context.Context, collectionName string) error {
	collection := m.client.Database(metaDatabase).Collection(metaCollection)
	filter := bson.M{"collection": collectionName}
	_, err := collection.DeleteOne(ctx, filter)
	return err
}

func NewMeta(client *mongo.Client) *Meta {
	return &Meta{client: client}
}
