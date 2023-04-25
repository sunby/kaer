package main

import (
	"context"
	"log"
	"os"

	"github.com/FerretDB/FerretDB/ferretdb"
	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/sunby/kaer/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// runExampleClient shows an example of running MongoDB client with embedded FerretDB.
func runExampleClient(uri string) {
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	cfg := db.Config{
		Cohere: db.CohereCfg{
			APIKey: "OrQd40L1gUacvSWLD9wVj8Sg01YNGDz34p8Q8QT3",
			Model:  "small",
		},
		DB: db.DbCfg{
			Name: "test",
			Dir:  "/tmp/testkaer",
		},
		HNSW: db.HNSWCfg{
			M: 32, EfConstruction: 400,
		},
	}

	docdb := client.Database(cfg.DB.Name)
	kaer := db.NewDB(docdb, &cfg)
	coll, err := kaer.CreateCollection("test_collection")
	if err != nil {
		log.Fatal(err)
	}
	data := &db.Data{}
	data = data.Documents([]string{"hello world", "nihao, shijie"}).Metadatas([]bson.M{
		bson.M{"attr1": 1, "attr2": "attr2value"}, bson.M{"attr1": 200, "attr2": "attr2valuexxxx"},
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

func createFerretDB(database *embeddedpostgres.EmbeddedPostgres) *ferretdb.FerretDB {
	f, err := ferretdb.New(&ferretdb.Config{
		Listener: ferretdb.ListenerConfig{
			TCP: "127.0.0.1:27017",
		},
		Handler:       "pg",
		PostgreSQLURL: "postgres://postgres:password@127.0.0.1:5432/postgres",
	})
	if err != nil {
		log.Fatal(err)
	}
	return f
}

func main() {
	log.SetOutput(os.Stderr)

	database := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().Username("postgres").Password("password").Port(5432).Database("test").DataPath("/tmp/testkaer"))
	if err := database.Start(); err != nil {
		panic(err)
	}

	f := createFerretDB(database)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- f.Run(ctx)
	}()

	uri := f.MongoDBURI()
	log.Printf("Embedded FerretDB started, use %s to connect.\n", uri)

	runExampleClient(uri)

	cancel()
	database.Stop()
	log.Fatal(<-done)
}
