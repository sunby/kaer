package main

import (
	"context"
	"log"
	"os"

	"github.com/sunby/kaer/config"
	"github.com/sunby/kaer/db"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	log.SetOutput(os.Stderr)

	cfg, err := config.ParseFrom("./config/config.toml.example")
	if err != nil {
		log.Fatal(err)
	}

	kaer, err := db.CreateKaer(&cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		kaer.Close()
	}()

	coll, err := kaer.CreateCollection(context.TODO(), "test")
	// coll, err := kaer.GetCollection(context.TODO(), "test")
	if err != nil {
		log.Fatal(err)
	}

	data := db.NewData().
		Documents([]string{"hello world", "nihao, shijie"}).
		Metadatas([]bson.M{
			{"attr1": 1, "attr2": "str1"},
			{"attr1": 200, "attr2": "str2"},
		})

	err = coll.Insert(data)
	if err != nil {
		log.Fatal(err)
	}

	res, err := coll.Query("h, world", 1, bson.D{{"attr1", bson.D{{"$eq", 1}}}})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Data: %v", res)
}
