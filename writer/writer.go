package writer

import (
	"context"
	"database/sql"
	"time"

	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	"go.uber.org/zap"
)

type Writer struct {
	db     *sql.DB
	logger *logger.Logger
	input  chan *models.WriteRequest
}

func NewWriter(input chan *models.WriteRequest, db *sql.DB, logger *logger.Logger) *Writer {
	return &Writer{
		db:     db,
		logger: logger,
		input:  input,
	}
}

func (w *Writer) Close() error {
	close(w.input)

	return w.db.Close()
}

func (w *Writer) StartDaemon(ctx context.Context) {
	for {
		select {
		case req := <-w.input:
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			_, err := w.db.ExecContext(ctx, req.Sql, req.Args)
			if err != nil {
				w.logger.Error("failed exec sql",
					zap.String("sql", req.Sql),
					zap.Any("args", req.Args),
					zap.Error(err))

				req.Error <- err
			} else {
				req.Error <- nil
			}

		case <-ctx.Done():
			w.logger.Info("stopping daemin")
			return
		}
	}
}
