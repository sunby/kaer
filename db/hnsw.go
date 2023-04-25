package db

import (
	"log"

	hnsw "github.com/Bithack/go-hnsw"
)

type HNSWIndex struct {
	*hnsw.Hnsw
	cfg *HNSWCfg
	dim int
}

func NewHNSWIndex(cfg *HNSWCfg, dim int) *HNSWIndex {
	zero := make([]float32, dim)
	index := hnsw.New(cfg.M, cfg.EfConstruction, zero)
	log.Printf("hnswindex, dim: %d", dim)
	index.Grow(2)
	return &HNSWIndex{
		Hnsw: index,
		cfg:  cfg,
		dim:  dim,
	}
}
