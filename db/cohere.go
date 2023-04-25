package db

import (
	"errors"

	cohere "github.com/cohere-ai/cohere-go"
)

const CohereMaxTexts = 96
const CohereTextMaxLen = 512

var ErrTooManyTexts error = errors.New("too many texts")

type CohereEmbedding struct {
	client *cohere.Client
	cfg    *CohereCfg
}

func (c *CohereEmbedding) GetEmbedding(texts []string) ([][]float32, error) {
	if len(texts) > CohereMaxTexts {
		return nil, ErrTooManyTexts
	}
	response, err := c.client.Embed(cohere.EmbedOptions{
		Texts: texts,
		Model: c.cfg.Model,
	})

	if err != nil {
		return nil, err
	}

	return Convert2Float32(response.Embeddings), nil
}

func NewCohereEmbedding(cfg *CohereCfg) (*CohereEmbedding, error) {
	cli, err := cohere.CreateClient(cfg.APIKey)
	if err != nil {
		return nil, err
	}
	return &CohereEmbedding{
		client: cli,
		cfg:    cfg,
	}, nil
}

func Convert2Float32(embeddings [][]float64) [][]float32 {
	res := make([][]float32, len(embeddings))
	for i, embedding := range embeddings {
		res[i] = make([]float32, len(embedding))
		for j, val := range embedding {
			res[i][j] = float32(val)
		}
	}
	return res
}
