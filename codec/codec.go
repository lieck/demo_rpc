package codec

import (
	"io"
)

type Header struct {
	// format "Service.Method"
	ServiceMethod string
	// sequence number chosen by client
	Seq   uint64
	Error string
}

type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCoderFunc func(io.ReadWriteCloser) Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCoderFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCoderFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
