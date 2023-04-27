package main

import (
	"context"
	"log"
	"os"

	"github.com/sunby/kaer/db"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	log.SetOutput(os.Stderr)

	cfg := db.Config{
		Cohere: db.CohereCfg{
			APIKey: "OrQd40L1gUacvSWLD9wVj8Sg01YNGDz34p8Q8QT3",
			Model:  "small",
		},
		DB: db.DbCfg{
			Name:             "test",
			Dir:              "/tmp/testkaer",
			PostgresUsername: "postgres",
			PostgresPassword: "password",
			PostresPort:      5432,
			FerretDBTcp:      "localhost:8080",
			FerretDBHandler:  "pg",
		},
		HNSW: db.HNSWCfg{
			M: 32, EfConstruction: 400,
		},
	}

	kaer, err := db.CreateKaer(&cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		kaer.Close()
	}()

	// coll, err := kaer.CreateCollection(context.TODO(), "test")
	coll, err := kaer.GetCollection(context.TODO(), "test")
	if err != nil {
		log.Fatal(err)
	}

	// data := &db.Data{}
	// data = data.Documents([]string{"hello world", "nihao, shijie"}).
	// 	Metadatas([]bson.M{
	// 		{"attr1": 1, "attr2": "str1"},
	// 		{"attr1": 200, "attr2": "str2"},
	// 	})
	// err = coll.Insert(data)
	if err != nil {
		log.Fatal(err)
	}
	res, err := coll.Query("h, world", 1, bson.D{{"attr1", bson.D{{"$eq", 1}}}})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Data: %v", res)
}
