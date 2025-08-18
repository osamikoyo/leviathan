package models

import (
	"time"
)

type Request struct {
	WriteRequest bool          `json:"write_request"`
	SQL          string        `json:"sql"`
	Args         []interface{} `json:"args"`
	Timestamp    time.Time     `json:"timestamp"`
}

func NewRequest(sql string, args []interface{}, writerequest bool) *Request {
	return &Request{
		Timestamp:    time.Now(),
		SQL:          sql,
		Args:         args,
		WriteRequest: writerequest,
	}
}
