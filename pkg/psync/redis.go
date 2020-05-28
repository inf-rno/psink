package psync

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type redis struct {
	ctx    context.Context
	cancel context.CancelFunc
	addr   string
	conn   net.Conn
	reader *reader
	writer *writer
}

func newRedis(addr string) *redis {
	return &redis{
		addr: addr,
	}
}

func (r *redis) String() string {
	return r.addr
}

func (r *redis) connect(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}
	r.ctx = ctx
	r.cancel = cancel
	r.conn = conn
	r.reader = newReader(r.conn)
	r.writer = newWriter(r.conn)
	return nil
}

func (r *redis) close() {
	r.cancel()
	err := r.conn.Close()
	if err != nil {
		panic(fmt.Errorf("failed to close redis connection: %w", err))
	}
}

type reader struct {
	buf *bufio.Reader
}

func newReader(r io.Reader) *reader {
	return &reader{
		buf: bufio.NewReader(r),
	}
}

func (r *reader) readLine() (string, error) {
	return r.buf.ReadString('\n')
}

func (r *reader) readRDB() error {
	str, err := r.buf.ReadString('\n')
	if err != nil {
		return fmt.Errorf("failed to read size of rdb data: %w", err)
	}
	//ignore the idle \n while rdb file is building
	for len(str) == 1 {
		str, err = r.buf.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read size of rdb data: %w", err)
		}
	}

	l, err := strconv.Atoi(str[1 : len(str)-2])
	if str[0] != '$' || err != nil {
		return fmt.Errorf("failed to read size of rdb data: %w, %s", err, str)
	}

	p := make([]byte, 4096)
	for i := 0; i < l; {
		n, err := r.buf.Read(p)
		if err != nil {
			return fmt.Errorf("failed to read rdb data: %w", err)
		}
		i += n
	}

	fmt.Printf("finished loading %d bytes of rdb data\n", l)
	return nil
}

func (r *reader) readCommand() ([]byte, error) {
	b, err := r.buf.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read command: %w", err)
	}
	return b, nil
}

type writer struct {
	buf *bufio.Writer
}

func newWriter(w io.Writer) *writer {
	return &writer{
		buf: bufio.NewWriter(w),
	}
}

func (w *writer) raw(b []byte) error {
	w.buf.Write(b)
	return w.flush()
}

func (w *writer) ping() error {
	w.buf.Write(([]byte)("PING\r\n"))
	return w.flush()
}

func (w *writer) capa() error {
	w.buf.Write(([]byte)("REPLCONF capa psync2\r\n"))
	return w.flush()
}

func (w *writer) sync() error {
	w.buf.Write(([]byte)("SYNC\r\n"))
	return w.flush()
}

func (w *writer) flushall() error {
	w.buf.Write(([]byte)("FLUSHALL\r\n"))
	return w.flush()
}

func (w *writer) flush() error {
	err := w.buf.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	return nil
}
