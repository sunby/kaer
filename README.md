# kaer

A vector database built on document database.

This project is in a very early stage. Do not use it in a production environment.

# Usage

At present, you can utilize this project in a Golang application, and we will soon develop a Python library for it.

## Golang

Currently, we only support insertion and querying functions, and you can use any document query language when querying. The data will be automatically persisted on disk and can be recovered upon the next opening.

You can find codes in main.go.
```
kaer, err := db.CreateKaer(&cfg)
if err != nil {
	log.Fatal(err)
}
defer func() {
	kaer.Close()
}()

database := kaer.Database("test")
coll, err := database.CreateCollection(context.TODO(), "test")
if err != nil {
	log.Fatal(err)
}

data := &db.Data{}
data = data.Documents([]string{"hello world", "nihao, shijie"}).Metadatas([]bson.M{
	bson.M{"attr1": 1, "attr2": "str1"}, bson.M{"attr1": 200, "attr2": "str2"},
})
err = coll.Insert(data)
if err != nil {
	log.Fatal(err)
}
res, err := coll.Query("h, world", 1, bson.D{{"attr1", bson.D{{"$eq", 1}}}})
if err != nil {
	log.Fatal(err)
}

```

# Dependencies
* [FerretDB](https://github.com/FerretDB/FerretDB)
* [embedded postgres](https://github.com/fergusstrange/embedded-postgres)
* [go-hnsw](https://github.com/Bithack/go-hnsw)
* [cohere](https://cohere.com/)
