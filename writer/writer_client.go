package writer

import (
	"errors"

	"github.com/osamikoyo/leviathan/models"
)

type WriterClient struct {
	output chan *models.WriteRequest
}

func NewWriterClient(output chan *models.WriteRequest) *WriterClient {
	return &WriterClient{
		output: output,
	}
}

func (w *WriterClient) Write(sql string, args ...interface{}) error {
	if len(sql) == 0 {
		return errors.New("sql is empty")
	}

	errChan := make(chan error, 1)
	defer close(errChan)

	req := models.NewWriteRequest(errChan, sql, args)

	w.output <- req

	err := <-req.Error

	close(errChan)

	return err
}
