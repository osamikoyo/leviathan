package models

type (
	ReadResponse struct {
		Error  error
		Values []map[string]interface{}
	}

	ReadRequest struct {
		Resp chan *ReadResponse
		Sql  string
		Args []any
	}
)

func NewReadRequest(sql string, args []any, resp chan *ReadResponse) *ReadRequest {
	return &ReadRequest{
		Sql:  sql,
		Args: args,
		Resp: resp,
	}
}
