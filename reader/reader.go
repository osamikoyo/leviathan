package reader

import (
	"context"
	"database/sql"
	"time"

	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/models"
	"go.uber.org/zap"
)

type (
	Reader struct {
		db     *sql.DB
		logger *logger.Logger
		input  chan *models.ReadRequest
	}

	Result []map[string]interface{}
)

func NewReader(input chan *models.ReadRequest, db *sql.DB, logger *logger.Logger) *Reader {
	return &Reader{
		input:  input,
		db:     db,
		logger: logger,
	}
}

func (r *Reader) Close() error {
	close(r.input)

	return r.db.Close()
}

func (r *Reader) StartDaemon(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-r.input:
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			rows, err := r.db.QueryContext(ctx, req.Sql, req.Args...)
			if err != nil {
				req.Resp <- &models.ReadResponse{
					Error:  err,
					Values: nil,
				}
			}
			if rows == nil {
				r.logger.Warn("rows is nil", zap.Any("request", req))
			}

			defer func() {
				if rows != nil {
					rows.Close()
				}
			}()

			columns, err := rows.Columns()
			if err != nil {
				req.Resp <- &models.ReadResponse{
					Error:  err,
					Values: nil,
				}
			}

			var results Result
			for rows.Next() {
				values := make([]interface{}, len(columns))
				pointers := make([]interface{}, len(columns))

				for i := range values {
					pointers[i] = &values[i]
				}

				err := rows.Scan(pointers...)
				if err != nil {
					req.Resp <- &models.ReadResponse{
						Error:  err,
						Values: nil,
					}
				}

				entry := make(map[string]interface{})

				for i, col := range columns {
					val := values[i]
					b, ok := val.([]byte)
					if ok {
						entry[col] = string(b)
					} else {
						entry[col] = val
					}
				}

				results = append(results, entry)
			}

			req.Resp <- &models.ReadResponse{
				Error:  nil,
				Values: results,
			}
		}
	}
}
