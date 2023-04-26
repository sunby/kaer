package db

import (
	"context"
	"fmt"

	"github.com/FerretDB/FerretDB/ferretdb"
	postgres "github.com/fergusstrange/embedded-postgres"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Kaer struct {
	cfg              *Config
	done             chan error
	cancel           context.CancelFunc
	embeddedPostgres *postgres.EmbeddedPostgres
	client           *mongo.Client
	meta             *Meta
}

// Get or create a database
func (k *Kaer) Database(name string) *DB {
	fdb := k.client.Database(name)
	return NewDB(k.meta, fdb, k.cfg)
}

func (k *Kaer) Close() error {
	k.cancel()
	<-k.done
	return k.embeddedPostgres.Stop()
}

func CreateKaer(cfg *Config) (*Kaer, error) {
	postgresDB, err := StartEmbeddedPostgres(cfg)
	if err != nil {
		return nil, err
	}

	ferretdb, err := StartFerretDB(postgresDB, cfg)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() {
		done <- ferretdb.Run(ctx)
	}()

	uri := ferretdb.MongoDBURI()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		cancel()
		return nil, err
	}

	return &Kaer{
		cfg:              cfg,
		done:             done,
		cancel:           cancel,
		embeddedPostgres: postgresDB,
		client:           client,
		meta:             NewMeta(client),
	}, nil
}

func StartEmbeddedPostgres(cfg *Config) (*postgres.EmbeddedPostgres, error) {
	database := postgres.NewDatabase(
		postgres.
			DefaultConfig().
			Username(cfg.DB.PostgresUsername).
			Password(cfg.DB.PostgresPassword).
			Port(cfg.DB.PostresPort).
			DataPath(fmt.Sprintf("%s/%s", cfg.DB.Dir, "postgres")))
	if err := database.Start(); err != nil {
		return nil, err
	}
	return database, nil
}

func StartFerretDB(database *postgres.EmbeddedPostgres, cfg *Config) (*ferretdb.FerretDB, error) {
	f, err := ferretdb.New(&ferretdb.Config{
		Listener: ferretdb.ListenerConfig{
			TCP: cfg.DB.FerretDBTcp,
		},
		Handler:       cfg.DB.FerretDBHandler,
		PostgreSQLURL: fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/postgres", cfg.DB.PostgresUsername, cfg.DB.PostgresPassword, cfg.DB.PostresPort),
	})
	if err != nil {
		return nil, err
	}
	return f, nil
}
