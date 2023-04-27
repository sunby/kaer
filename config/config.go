package config

import (
	"github.com/BurntSushi/toml"
)

type Config struct {
	Cohere CohereCfg `toml:"cohere"`
	DB     DbCfg     `toml:"db"`
	HNSW   HNSWCfg   `toml:"hnsw"`
}

type CohereCfg struct {
	APIKey string `toml:"api_key"`
	Model  string `toml:"model"`
}

type DbCfg struct {
	Dir              string `toml:"persist_dir"`
	PostgresUsername string `toml:"postgres_username"`
	PostgresPassword string `toml:"postgres_password"`
	PostresPort      uint32 `toml:"postgres_port"`
	FerretDBTcp      string `toml:"ferretdb_tcp"`
	FerretDBHandler  string `toml:"ferretdb_handler"`
}

type HNSWCfg struct {
	M              int `toml:"m"`
	EfConstruction int `toml:"ef_construction"`
}

var defaultCfg = Config{
	Cohere: CohereCfg{
		APIKey: "",
		Model:  "multilingual-22-12",
	},
	DB: DbCfg{
		Dir:              "/tmp/kaer",
		PostgresUsername: "postgres",
		PostgresPassword: "password",
		PostresPort:      5432,
		FerretDBTcp:      "localhost:8080",
		FerretDBHandler:  "pg",
	},
	HNSW: HNSWCfg{
		M:              32,
		EfConstruction: 400,
	},
}

func ParseFrom(file string) (Config, error) {
	cfg := defaultCfg
	_, err := toml.DecodeFile(file, &cfg)
	// TODO handle validation
	return cfg, err

}

var CohereModel2Dim = map[string]int{"multilingual-22-12": 768, "small": 1024, "large": 4096}
