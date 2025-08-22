package reader

import (
	"errors"

	"github.com/osamikoyo/leviathan/models"
)

type ReaderClient struct {
	output chan *models.ReadRequest
}

func NewReaderClient(output chan *models.ReadRequest) *ReaderClient {
	return &ReaderClient{
		output: output,
	}
}

func (r *ReaderClient) Read(sql string, args ...any) (Result, error) {
	if len(sql) == 0 {
		return nil, errors.New("nil input")
	}

	resp := make(chan *models.ReadResponse, 1)

	req := models.NewReadRequest(sql, args, resp)

	r.output <- req

	result := <-resp

	close(resp)

	return result.Values, result.Error
}
