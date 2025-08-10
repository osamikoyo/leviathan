package models

type WriteRequest struct {
	Error chan error
	Sql   string
	Args  []any
}

func NewWriteRequest(err chan error, sql string, args []any) *WriteRequest {
	return &WriteRequest{
		Error: err,
		Sql:   sql,
		Args:  args,
	}
}
