package db

type Config struct {
	Cohere CohereCfg
	DB     DbCfg
	HNSW   HNSWCfg
}

type CohereCfg struct {
	APIKey string
	Model  string
}

type DbCfg struct {
	Dir              string
	Name             string
	PostgresUsername string
	PostgresPassword string
	PostresPort      uint32
	FerretDBTcp      string
	FerretDBHandler  string
}

type HNSWCfg struct {
	M              int
	EfConstruction int
}

var CohereModel2Dim = map[string]int{"multilingual-22-12": 768, "small": 1024, "large": 4096}
