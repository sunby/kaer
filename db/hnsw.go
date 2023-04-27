package db

import (
	hnsw "github.com/sunby/go-hnsw"
	"github.com/sunby/kaer/config"
)

const HnswGrowSize = 1000

type HNSWIndex struct {
	*hnsw.Hnsw
	size uint32
	id   uint32
}

func (h *HNSWIndex) Add(point hnsw.Point, id uint32) {
	if id > h.size {
		h.Grow(HnswGrowSize)
		h.size += HnswGrowSize
	}
	h.Hnsw.Add(point, id)
	h.id = id
}

func (h *HNSWIndex) ID() uint32 {
	return h.id
}

func (h *HNSWIndex) Size() uint32 {
	return h.size
}

func NewHNSWIndex(cfg *config.HNSWCfg, dim int) *HNSWIndex {
	zero := make([]float32, dim)
	index := hnsw.New(cfg.M, cfg.EfConstruction, zero)
	return &HNSWIndex{
		Hnsw: index,
	}
}

func NewHNSWIndexFromFile(file string, size, id uint32) (*HNSWIndex, error) {
	index, _, err := hnsw.Load(file)
	if err != nil {
		return nil, err
	}
	return &HNSWIndex{
		Hnsw: index,
		size: size,
		id:   id,
	}, nil
}
