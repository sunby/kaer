package db

import (
	"errors"

	cohere "github.com/cohere-ai/cohere-go"
	"github.com/sunby/kaer/config"
)

const CohereMaxTexts = 96
const CohereTextMaxLen = 512

var ErrTooManyTexts error = errors.New("too many texts")

type CohereEmbedding struct {
	client *cohere.Client
	cfg    *config.CohereCfg
}

func (c *CohereEmbedding) GetEmbedding(texts []string) ([][]float32, error) {

	res := make([][]float32, 0, len(texts))
	l := len(texts)
	for i := 0; i < l; i += CohereMaxTexts {
		end := min(l, i+CohereMaxTexts)
		embeddings, err := c.getEmbedding(texts[i:end])
		if err != nil {
			return nil, err
		}
		res = append(res, embeddings...)
	}
	return res, nil
}

func (c *CohereEmbedding) getEmbedding(texts []string) ([][]float32, error) {
	response, err := c.client.Embed(cohere.EmbedOptions{
		Texts: texts,
		Model: c.cfg.Model,
	})

	if err != nil {
		return nil, err
	}

	res := make([][]float32, len(response.Embeddings))
	for i := range res {
		res[i] = Convert2Float32(response.Embeddings[i])
	}
	return res, nil
}

func NewCohereEmbedding(cfg *config.CohereCfg) (*CohereEmbedding, error) {
	cli, err := cohere.CreateClient(cfg.APIKey)

	if err != nil {
		return nil, err
	}
	return &CohereEmbedding{
		client: cli,
		cfg:    cfg,
	}, nil
}

func Convert2Float32[T any](embedding []T) []float32 {
	res := make([]float32, len(embedding))
	for i, val := range embedding {
		res[i] = (float32)(any(val).(float64))
	}
	return res
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
