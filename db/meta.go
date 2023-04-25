package db

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MetaDatabase     = "_m_meta_db"
	HnswIndexPathKey = "_m_hnsw_index_path"
	HnswIdKey        = "_m_hnsw_id"
	HnswSizeKey      = "_m_hnsw_size"
)

var metaFilter bson.M = bson.M{"_m_meta_field": 1}

type Meta struct {
	client *mongo.Client
}

func (m *Meta) Write(ctx context.Context, collectionName string, indexfile string, id uint32, size uint32) error {
	collection := m.client.Database(MetaDatabase).Collection(collectionName)
	_, err := collection.UpdateOne(ctx, metaFilter,
		bson.M{"$set": bson.M{HnswIndexPathKey: indexfile, HnswIdKey: id, HnswSizeKey: size}},
		options.Update().SetUpsert(true))
	if err != nil {
		return err
	}
	return nil
}

func (m *Meta) Read(ctx context.Context, collectionName string) *mongo.SingleResult {
	collection := m.client.Database(MetaDatabase).Collection(collectionName)
	return collection.FindOne(ctx, metaFilter)
}

func (m *Meta) Drop(ctx context.Context, collectionName string) error {
	collection := m.client.Database(MetaDatabase).Collection(collectionName)
	return collection.Drop(ctx)
}

func NewMeta(client *mongo.Client) *Meta {
	return &Meta{client: client}
}
