package codec

import (
	"bufio"
	"encoding/gob"
	"io"
)

type GobCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func (g *GobCodec) Close() error {
	return g.conn.Close()
}

func (g *GobCodec) ReadHeader(header *Header) error {
	return g.dec.Decode(header)
}

func (g *GobCodec) ReadBody(i interface{}) error {
	return g.dec.Decode(i)
}

func (g *GobCodec) Write(header *Header, body interface{}) (err error) {
	defer func() {
		_ = g.buf.Flush()
	}()

	err = g.enc.Encode(header)
	if err != nil {
		return err
	}

	err = g.enc.Encode(body)
	if err != nil {
		return err
	}

	return nil
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec(conn io.ReadWriteCloser) Codec {
	return &GobCodec{
		conn: conn,
		buf:  bufio.NewWriter(conn),
		dec:  gob.NewDecoder(conn),
		enc:  gob.NewEncoder(conn),
	}
}
