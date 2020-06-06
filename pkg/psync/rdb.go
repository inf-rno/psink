package psync

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"

	redigo "github.com/gomodule/redigo/redis"
)

const (
	// Redis Object type
	TypeString = iota
	TypeList
	TypeSet
	TypeZset
	TypeHash
	TypeZset2
	TypeModule
	TypeModule2
	_
	TypeHashZipMap
	TypeListZipList
	TypeSetIntSet
	TypeZsetZipList
	TypeHashZipList
	TypeListQuickList
	TypeStreamListPacks

	// Redis RDB protocol
	FlagOpcodeIdle         = 248
	FlagOpcodeFreq         = 249
	FlagOpcodeAux          = 250
	FlagOpcodeResizeDB     = 251
	FlagOpcodeExpireTimeMs = 252
	FlagOpcodeExpireTime   = 253
	FlagOpcodeSelectDB     = 254
	FlagOpcodeEOF          = 255

	// Redis length type
	Type6Bit   = 0
	Type14Bit  = 1
	Type32Bit  = 0x80
	Type64Bit  = 0x81
	TypeEncVal = 3

	// Redis ziplist types
	ZipStr06B = 0
	ZipStr14B = 1
	ZipStr32B = 2

	// Redis ziplist entry
	ZipInt04B = 15
	ZipInt08B = 0xfe        // 11111110
	ZipInt16B = 0xc0 | 0<<4 // 11000000
	ZipInt24B = 0xc0 | 3<<4 // 11110000
	ZipInt32B = 0xc0 | 1<<4 // 11010000
	ZipInt64B = 0xc0 | 2<<4 //11100000

	ZipBigPrevLen = 0xfe

	// Redis listpack
	StreamItemFlagNone       = 0
	StreamItemFlagDeleted    = 1 << 0
	StreamItemFlagSameFields = 1 << 1
)
const (
	EncodeInt8 = iota
	EncodeInt16
	EncodeInt32
	EncodeLZF

	VersionMin = 1
	VersionMax = 9
)

var (
	buff   = make([]byte, 8)
	PosInf = math.Inf(1)
	NegInf = math.Inf(-1)
	Nan    = math.NaN()
)

type rdb struct {
	ctx  context.Context
	buf  *bufio.Reader
	conn redigo.Conn
	i, n int
}

func loadRDB(ctx context.Context, buf *bufio.Reader, destAddr string, size int) error {
	fmt.Printf("loading %d bytes of rdb to %s\n", size, destAddr)
	c, err := redigo.DialURL(fmt.Sprintf("redis://%s", destAddr))
	if err != nil {
		return fmt.Errorf("failed to connect to dest: %w", err)
	}
	defer c.Close()
	r := &rdb{
		ctx:  ctx,
		buf:  buf,
		conn: c,
		n:    size,
	}

	res, err := r.checkHeader()
	if res == false || err != nil {
		return err
	}

	err = r.loadData()
	if err != nil {
		return err
	}
	return nil
}

// 9 bytes length include: 5 bytes "REDIS" and 4 bytes version in rdb.file
func (r *rdb) checkHeader() (bool, error) {
	header := make([]byte, 9)
	n, err := io.ReadFull(r.buf, header)
	if err != nil {
		return false, fmt.Errorf("failed to read RDB header: %w", err)
	}
	r.i += n

	// Check "REDIS" string and version.
	rdbVersion, err := strconv.Atoi(string(header[5:]))
	if !bytes.Equal(header[0:5], []byte("REDIS")) || err != nil || (rdbVersion < VersionMin || rdbVersion > VersionMax) {
		return false, fmt.Errorf("invalid header: %w", err)
	}
	return true, nil
}

func (r *rdb) loadData() error {
	var expire int64
	var hasSelectDb bool
	var t byte
	var err error
	for r.i < r.n {
		t, err = r.loadByte()
		if err != nil {
			return err
		}
		if t == FlagOpcodeIdle {
			_, _, err := r.loadLen()
			if err != nil {
				return err
			}
			continue
		} else if t == FlagOpcodeFreq {
			_, err := r.loadByte()
			if err != nil {
				return err
			}
			continue
		} else if t == FlagOpcodeAux {
			key, err := r.loadString()
			if err != nil {
				return fmt.Errorf("parse Aux key failed: %w", err)
			}
			val, err := r.loadString()
			if err != nil {
				return fmt.Errorf("parse Aux value failed: %w", err)
			}
			if string(key) == "lua" {
				r.loadScript(val)
			} else {
				fmt.Printf("Aux field: %s, %s\n", key, val)
			}
			continue
		} else if t == FlagOpcodeResizeDB {
			dbSize, _, err := r.loadLen()
			if err != nil {
				return fmt.Errorf("parse ResizeDB size failed: %w", err)
			}
			expiresSize, _, err := r.loadLen()
			if err != nil {
				return fmt.Errorf("parse ResizeDB size failed: %w", err)
			}
			fmt.Printf("DBSize: %d, ExpireSize: %d\n", dbSize, expiresSize)
			continue
		} else if t == FlagOpcodeExpireTimeMs {
			res, err := r.loadUint64()
			if err != nil {
				return fmt.Errorf("parse ExpireTime_ms failed: %w", err)
			}
			expire = int64(res)
			continue
		} else if t == FlagOpcodeExpireTime {
			res, err := r.loadUint64()
			if err != nil {
				return fmt.Errorf("parse ExpireTime failed: %w", err)
			}
			expire = int64(res) * 1000
			continue
		} else if t == FlagOpcodeSelectDB {
			if hasSelectDb == true {
				continue
			}
			dbindex, _, err := r.loadLen()
			if err != nil {
				return fmt.Errorf("parse db index failed: %w", err)
			}
			r.selectDB(dbindex)
			hasSelectDb = false
			continue
		} else if t == FlagOpcodeEOF {
			n, err := io.ReadFull(r.buf, buff)
			if err != nil {
				return fmt.Errorf("failed to read checksum: %w", err)
			}
			r.i += n
			fmt.Printf("rdb checksum: %x\n", buff)
			// TODO rdb checksum
			err = nil
			break
		}
		key, err := r.loadString()
		if err != nil {
			return err
		}
		if err := r.loadValue(key, t, expire); err != nil {
			return err
		}
		expire = -1
	}

	return err
}

func (r *rdb) loadValue(key []byte, t byte, expire int64) error {
	fmt.Printf("loading key %s, %d\n", key, t)
	if t == TypeString {
		val, err := r.loadString()
		if err != nil {
			return err
		}
		res, err := redigo.String(r.conn.Do("SET", key, val))
		if err != nil || res != "OK" {
			return fmt.Errorf("failed to SET val %s, %s: %w", key, val, err)
		}
	} else if t == TypeList {
		if err := r.loadList(key); err != nil {
			return err
		}
	} else if t == TypeSet {
		if err := r.loadSet(key); err != nil {
			return err
		}
	} else if t == TypeZset || t == TypeZset2 {
		if err := r.loadZSet(key, t); err != nil {
			return err
		}
	} else if t == TypeHash {
		if err := r.loadHashMap(key); err != nil {
			return err
		}
	} else if t == TypeListQuickList {
		if err := r.loadListWithQuickList(key); err != nil {
			return err
		}
	} else if t == TypeHashZipMap {
		if err := r.loadHashMapWithZipmap(key); err != nil {
			return err
		}
	} else if t == TypeListZipList {
		if err := r.loadListWithZipList(key); err != nil {
			return err
		}
	} else if t == TypeSetIntSet {
		if err := r.loadIntSet(key); err != nil {
			return err
		}
	} else if t == TypeZsetZipList {
		if err := r.loadZipListSortSet(key); err != nil {
			return err
		}
	} else if t == TypeHashZipList {
		if err := r.loadHashMapZiplist(key); err != nil {
			return err
		}
	} else if t == TypeStreamListPacks {
		return fmt.Errorf("streams are not supported")
	} else if t == TypeModule || t == TypeModule2 {
		return fmt.Errorf("modules are not supported")
	} else {
		return fmt.Errorf("unhandled redis type: %d", t)
	}

	if expire > 0 {
		_, err := r.conn.Do("PEXPIREAT", key, expire)
		if err != nil {
			return fmt.Errorf("failed to expire key %s: %w", key, err)
		}
	}

	return nil
}

func (r *rdb) loadByte() (buf byte, err error) {
	buf, err = r.buf.ReadByte()
	if err != nil {
		return
	}
	r.i++
	return
}

func (r *rdb) loadLen() (length uint64, isEncode bool, err error) {
	var n int
	buf, err := r.loadByte()
	if err != nil {
		return
	}
	typeLen := (buf & 0xc0) >> 6
	if typeLen == TypeEncVal || typeLen == Type6Bit {
		// Read a 6 bit encoding type or 6 bit len.
		if typeLen == TypeEncVal {
			isEncode = true
		}
		length = uint64(buf) & 0x3f
	} else if typeLen == Type14Bit {
		// Read a 14 bit len, need read next byte.
		nb, err := r.loadByte()
		if err != nil {
			return 0, false, err
		}
		length = (uint64(buf)&0x3f)<<8 | uint64(nb)
	} else if buf == Type32Bit {
		n, err = io.ReadFull(r.buf, buff[0:4])
		if err != nil {
			return
		}
		r.i += n
		length = uint64(binary.BigEndian.Uint32(buff))
	} else if buf == Type64Bit {
		n, err = io.ReadFull(r.buf, buff)
		if err != nil {
			return
		}
		r.i += n
		length = binary.BigEndian.Uint64(buff)
	} else {
		err = errors.New(fmt.Sprintf("unknown length encoding %d in loadLen()", typeLen))
	}
	return
}

func (r *rdb) loadString() ([]byte, error) {
	length, needEncode, err := r.loadLen()
	if err != nil {
		return nil, err
	}

	if needEncode {
		switch length {
		case EncodeInt8:
			b, err := r.loadByte()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeInt16:
			b, err := r.loadUint16()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeInt32:
			b, err := r.loadUint32()
			return []byte(strconv.Itoa(int(b))), err
		case EncodeLZF:
			res, err := r.loadLZF()
			return res, err
		default:
			return []byte{}, fmt.Errorf("unknown string encode type: %d", length)
		}
	}

	res := make([]byte, length)
	n, err := io.ReadFull(r.buf, res)
	r.i += n
	return res, err
}

func (r *rdb) loadUint16() (res uint16, err error) {
	_, err = io.ReadFull(r.buf, buff[:2])
	if err != nil {
		return
	}
	r.i += 2
	res = binary.LittleEndian.Uint16(buff[:2])
	return
}

func (r *rdb) loadUint32() (res uint32, err error) {
	_, err = io.ReadFull(r.buf, buff[:4])
	if err != nil {
		return
	}
	r.i += 4
	res = binary.LittleEndian.Uint32(buff[:4])
	return
}

func (r *rdb) loadUint64() (res uint64, err error) {
	_, err = io.ReadFull(r.buf, buff)
	if err != nil {
		return
	}
	r.i += 8
	res = binary.LittleEndian.Uint64(buff)
	return
}

func (r *rdb) loadFloat() (float64, error) {
	b, err := r.loadByte()
	if err != nil {
		return 0, err
	}
	if b == 0xff {
		return NegInf, nil
	} else if b == 0xfe {
		return PosInf, nil
	} else if b == 0xfd {
		return Nan, nil
	}

	floatBytes := make([]byte, b)
	n, err := io.ReadFull(r.buf, floatBytes)
	if err != nil {
		return 0, err
	}
	r.i += n
	float, err := strconv.ParseFloat(string(floatBytes), 64)
	return float, err
}

// 8 bytes float64, follow IEEE754 float64 stddef (standard definitions)
func (r *rdb) loadBinaryFloat() (float64, error) {
	n, err := io.ReadFull(r.buf, buff)
	if err != nil {
		return 0, err
	}
	r.i += n
	bits := binary.LittleEndian.Uint64(buff)
	return math.Float64frombits(bits), nil
}

func (r *rdb) loadLZF() (res []byte, err error) {
	ilength, _, err := r.loadLen()
	if err != nil {
		return
	}
	ulength, _, err := r.loadLen()
	if err != nil {
		return
	}
	val := make([]byte, ilength)
	n, err := io.ReadFull(r.buf, val)
	if err != nil {
		return
	}
	r.i += n
	res = lzfDecompress(val, int(ilength), int(ulength))
	return
}

func (r *rdb) selectDB(index uint64) error {
	fmt.Printf("selecting db %d\n", index)
	res, err := redigo.String(r.conn.Do("SELECT", index))
	if err != nil || res != "OK" {
		return fmt.Errorf("failed to select db %d: %w", index, err)
	}
	return nil
}

func (r *rdb) loadScript(script []byte) error {
	fmt.Printf("loading script %s\n", script)
	_, err := redigo.Int(r.conn.Do("SCRIPT LOAD", script))
	if err != nil {
		return fmt.Errorf("failed to load script %s: %w", script, err)
	}
	return nil
}

func (r *rdb) loadList(key []byte) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	var ent []interface{}
	for i := uint64(0); i < length; i++ {
		val, err := r.loadString()
		if err != nil {
			return err
		}
		ent = append(ent, val)
	}
	n, err := redigo.Int(r.conn.Do("RPUSH", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(length) != n {
		return fmt.Errorf("failed to RPUSH list %s, %d: %w", key, length, err)
	}
	return nil
}

func (r *rdb) loadListWithQuickList(key []byte) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}

	for i := uint64(0); i < length; i++ {
		listItems, err := r.loadZipList()
		if err != nil {
			return err
		}
		n, err := redigo.Int(r.conn.Do("RPUSH", redigo.Args{}.Add(key).AddFlat(listItems)...))
		if err != nil || len(listItems) != n {
			return fmt.Errorf("failed to RPUSH quickList %s, %d: %w", key, len(listItems), err)
		}
	}
	return nil
}

func (r *rdb) loadListWithZipList(key []byte) error {
	entries, err := r.loadZipList()
	if err != nil {
		return err
	}
	n, err := redigo.Int(r.conn.Do("RPUSH", redigo.Args{}.Add(key).AddFlat(entries)...))
	if err != nil || len(entries) != n {
		return fmt.Errorf("failed to RPUSH zipList %s, %d: %w", key, len(entries), err)
	}
	return nil
}

func (r *rdb) loadZipList() ([][]byte, error) {
	b, err := r.loadString()
	if err != nil {
		return nil, err
	}
	buf := newInput(b)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return nil, err
	}

	items := make([][]byte, 0, length)
	for i := int64(0); i < length; i++ {
		entry, err := loadZiplistEntry(buf)
		if err != nil {
			return nil, err
		}
		items = append(items, entry)
	}
	return items, nil
}

func (r *rdb) loadHashMap(key []byte) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	var ent []interface{}
	for i := uint64(0); i < length; i++ {
		field, err := r.loadString()
		if err != nil {
			return err
		}
		value, err := r.loadString()
		if err != nil {
			return err
		}
		ent = append(ent, field, value)
	}
	n, err := redigo.Int(r.conn.Do("HSET", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(length) != n {
		return fmt.Errorf("failed to HSET %s, %d: %w", key, length, err)
	}
	return nil
}

func (r *rdb) loadHashMapWithZipmap(key []byte) error {
	zipmap, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(zipmap)
	blen, err := buf.ReadByte()
	if err != nil {
		return err
	}

	length := int(blen)
	if blen > 254 {
		length, err = countZipmapItems(buf)
		if err != nil {
			return err
		}
		length /= 2
	}

	var ent []interface{}
	for i := 0; i < length; i++ {
		field, err := loadZipmapItem(buf, false)
		if err != nil {
			return err
		}
		value, err := loadZipmapItem(buf, true)
		if err != nil {
			return err
		}
		ent = append(ent, field, value)
	}
	n, err := redigo.Int(r.conn.Do("HSET", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(length) != n {
		return fmt.Errorf("failed to HSET %s, %d: %w", key, length, err)
	}
	return nil
}

func (r *rdb) loadHashMapZiplist(key []byte) error {
	b, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	length, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	length /= 2

	var ent []interface{}
	for i := int64(0); i < length; i++ {
		field, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		value, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		ent = append(ent, field, value)
	}
	n, err := redigo.Int(r.conn.Do("HSET", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(length) != n {
		return fmt.Errorf("failed to HSET %s, %d: %w", key, length, err)
	}
	return nil
}

func (r *rdb) loadSet(key []byte) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	var ent []interface{}
	for i := uint64(0); i < length; i++ {
		member, err := r.loadString()
		if err != nil {
			return err
		}
		ent = append(ent, member)
	}
	n, err := redigo.Int(r.conn.Do("SADD", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(length) != n {
		return fmt.Errorf("failed to SADD %s, %d: %w", key, length, err)
	}
	return nil
}

func (r *rdb) loadIntSet(key []byte) error {
	b, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	sizeBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	intSize := binary.LittleEndian.Uint32(sizeBytes)
	if intSize != 2 && intSize != 4 && intSize != 8 {
		return fmt.Errorf("unknown intset encoding: %d", intSize)
	}
	lenBytes, err := buf.Slice(4)
	if err != nil {
		return err
	}
	cardinality := binary.LittleEndian.Uint32(lenBytes)
	var ent []interface{}
	for i := uint32(0); i < cardinality; i++ {
		intBytes, err := buf.Slice(int(intSize))
		if err != nil {
			return err
		}
		var intString string
		switch intSize {
		case 2:
			intString = strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(intBytes))), 10)
		case 4:
			intString = strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(intBytes))), 10)
		case 8:
			intString = strconv.FormatInt(int64(int64(binary.LittleEndian.Uint64(intBytes))), 10)
		}
		ent = append(ent, intString)
	}
	n, err := redigo.Int(r.conn.Do("SADD", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(cardinality) != n {
		return fmt.Errorf("failed to SADD %s, %d: %w", key, cardinality, err)
	}
	return nil
}

func (r *rdb) loadZSet(key []byte, t byte) error {
	length, _, err := r.loadLen()
	if err != nil {
		return err
	}
	var ent []interface{}
	for i := uint64(0); i < length; i++ {
		member, err := r.loadString()
		if err != nil {
			return err
		}
		var score float64
		if t == TypeZset2 {
			score, err = r.loadBinaryFloat()
		} else {
			score, err = r.loadFloat()
		}
		if err != nil {
			return err
		}
		ent = append(ent, score, member)
	}
	n, err := redigo.Int(r.conn.Do("ZADD", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(length) != n {
		return fmt.Errorf("failed to ZADD %s, %d: %w", key, length, err)
	}
	return nil
}

func (r *rdb) loadZipListSortSet(key []byte) error {
	b, err := r.loadString()
	if err != nil {
		return err
	}
	buf := newInput(b)
	cardinality, err := loadZiplistLength(buf)
	if err != nil {
		return err
	}
	cardinality /= 2

	var ent []interface{}
	for i := int64(0); i < cardinality; i++ {
		member, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		scoreBytes, err := loadZiplistEntry(buf)
		if err != nil {
			return err
		}
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			return err
		}
		ent = append(ent, score, member)
	}
	n, err := redigo.Int(r.conn.Do("ZADD", redigo.Args{}.Add(key).AddFlat(ent)...))
	if err != nil || int(cardinality) != n {
		return fmt.Errorf("failed to ZADD %s, %d: %w", key, cardinality, err)
	}
	return nil
}
