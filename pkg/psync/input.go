package psync

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

type input struct {
	data  []byte
	index int
}

func newInput(data []byte) *input {
	return &input{
		data: data,
	}
}

func (buf *input) Slice(n int) ([]byte, error) {
	if buf.index+n > len(buf.data) {
		return nil, io.EOF
	}
	b := buf.data[buf.index : buf.index+n]
	buf.index = buf.index + n
	return b, nil
}

func (buf *input) ReadByte() (byte, error) {
	if buf.index >= len(buf.data) {
		return 0, io.EOF
	}
	b := buf.data[buf.index]
	buf.index++
	return b, nil
}

func (buf *input) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if buf.index >= len(buf.data) {
		return 0, io.EOF
	}
	n := copy(b, buf.data[buf.index:])
	buf.index = buf.index + n
	return n, nil
}

func (buf *input) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case 0:
		abs = offset
	case 1:
		abs = int64(buf.index) + offset
	case 2:
		abs = int64(len(buf.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	if abs < 0 {
		return 0, fmt.Errorf("negative position")
	}
	if abs >= 1<<31 {
		return 0, fmt.Errorf("position out of range")
	}
	buf.index = int(abs)
	return abs, nil
}

func loadZipmapItem(buf *input, readFree bool) ([]byte, error) {
	length, free, err := loadZipmapItemLength(buf, readFree)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	value, err := buf.Slice(length)
	if err != nil {
		return nil, err
	}
	_, err = buf.Seek(int64(free), 1)
	return value, err
}

func countZipmapItems(buf *input) (int, error) {
	n := 0
	for {
		strLen, free, err := loadZipmapItemLength(buf, n%2 != 0)
		if err != nil {
			return 0, err
		}
		if strLen == -1 {
			break
		}
		_, err = buf.Seek(int64(strLen)+int64(free), 1)
		if err != nil {
			return 0, err
		}
		n++
	}
	_, err := buf.Seek(0, 0)
	return n, err
}

func loadZipmapItemLength(buf *input, readFree bool) (int, int, error) {
	b, err := buf.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	switch b {
	case 253:
		s, err := buf.Slice(5)
		if err != nil {
			return 0, 0, err
		}
		return int(binary.BigEndian.Uint32(s)), int(s[4]), nil
	case 254:
		return 0, 0, fmt.Errorf("invalid zipmap item length")
	case 255:
		return -1, 0, nil
	}
	var free byte
	if readFree {
		free, err = buf.ReadByte()
	}

	return int(b), int(free), err
}

func loadZiplistLength(buf *input) (int64, error) {
	buf.Seek(8, 0)
	lenBytes, err := buf.Slice(2)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint16(lenBytes)), nil
}

func loadZiplistEntry(buf *input) ([]byte, error) {
	prevLen, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	if prevLen == ZipBigPrevLen {
		buf.Seek(4, 1) // skip the 4-byte prevlen
	}

	header, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}
	switch {
	case header>>6 == ZipStr06B:
		return buf.Slice(int(header & 0x3f))
	case header>>6 == ZipStr14B:
		b, err := buf.ReadByte()
		if err != nil {
			return nil, err
		}
		return buf.Slice((int(header&0x3f) << 8) | int(b))
	case header>>6 == ZipStr32B:
		lenBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return buf.Slice(int(binary.BigEndian.Uint32(lenBytes)))
	case header == ZipInt08B:
		b, err := buf.ReadByte()
		return []byte(strconv.FormatInt(int64(int8(b)), 10)), err
	case header == ZipInt16B:
		intBytes, err := buf.Slice(2)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)), nil
	case header == ZipInt32B:
		intBytes, err := buf.Slice(4)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)), nil
	case header == ZipInt64B:
		intBytes, err := buf.Slice(8)
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(binary.LittleEndian.Uint64(intBytes)), 10)), nil
	case header == ZipInt24B:
		intBytes := make([]byte, 4)
		_, err := buf.Read(intBytes[1:])
		if err != nil {
			return nil, err
		}
		return []byte(strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))>>8), 10)), nil
	case header>>4 == ZipInt04B:
		return []byte(strconv.FormatInt(int64(header&0x0f)-1, 10)), nil
	}

	return nil, fmt.Errorf("rdb: unknown ziplist header byte: %d", header)
}
