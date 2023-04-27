# kaer

A vector database built on document database.

This project is in a very early stage. Do not use it in a production environment.

# Usage

At present, you can utilize this project in a Golang application, and we will soon develop a Python library for it.

## Golang

Currently, we only support insertion and querying functions, and you can use any document query language when querying. The data will be automatically persisted on disk and can be recovered upon the next opening.

### Example
You can find codes in main.go.
```golang
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
```

# Dependencies
* [FerretDB](https://github.com/FerretDB/FerretDB)
* [embedded postgres](https://github.com/fergusstrange/embedded-postgres)
* [go-hnsw](https://github.com/Bithack/go-hnsw)
* [cohere](https://cohere.com/)
