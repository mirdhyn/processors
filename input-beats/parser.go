package beatsinput

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

const (
	ack         = "2A"
	maxKeyLen   = 100 * 1024 * 1024 // 100 mb
	maxValueLen = 250 * 1024 * 1024 // 250 mb
)

type Parser struct {
	Conn       net.Conn
	out        chan map[string]interface{}
	wlen, plen uint32
	buffer     io.Reader
}

func NewParser(c net.Conn, dc chan map[string]interface{}) *Parser {
	return &Parser{
		Conn: c,
		out:  dc,
	}
}

// ack acknowledges that the payload was received successfully
func (p *Parser) ack(seq uint32) error {
	buffer := bytes.NewBuffer([]byte(ack))
	binary.Write(buffer, binary.BigEndian, seq)
	//log.Printf("Sending ACK with seq %d", seq)

	if _, err := p.Conn.Write(buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

// readKV parses key value pairs from within the payload
func (p *Parser) readKV() ([]byte, []byte, error) {
	var klen, vlen uint32

	// Read key len
	binary.Read(p.buffer, binary.BigEndian, &klen)

	if klen > maxKeyLen {
		return nil, nil, fmt.Errorf("key exceeds max len %d, got %d bytes", maxKeyLen, klen)
	}

	// Read key
	key := make([]byte, klen)
	_, err := p.buffer.Read(key)
	if err != nil {
		return nil, nil, err
	}

	// Read value len
	binary.Read(p.buffer, binary.BigEndian, &vlen)
	if vlen > maxValueLen {
		return nil, nil, fmt.Errorf("value exceeds max len %d, got %d bytes", maxValueLen, vlen)
	}

	// Read value
	value := make([]byte, vlen)
	_, err = p.buffer.Read(value)
	if err != nil {
		return nil, nil, err
	}

	return key, value, nil
}

// read parses the compressed data frame
func (p *Parser) read() (uint32, error) {
	var seq, count uint32
	// var k, v []byte
	var err error

	r, err := zlib.NewReader(p.Conn)
	if err != nil {
		return seq, err
	}
	defer r.Close()

	// Decompress
	buff := new(bytes.Buffer)
	io.Copy(buff, r)
	p.buffer = buff

	b := make([]byte, 2)
	for i := uint32(0); i < p.wlen; i++ {
		n, err := buff.Read(b)
		if err == io.EOF {
			return seq, err
		}

		if n == 0 {
			continue
		}

		switch string(b) {
		case "2D": // window size
			binary.Read(buff, binary.BigEndian, &seq)
			binary.Read(buff, binary.BigEndian, &count)

			fields := make(map[string]interface{})
			// fields["@timestamp"] = time.Now().Format(veino.VeinoTime)

			// for j := uint32(0); j < count; j++ {
			// 	if k, v, err = p.readKV(); err != nil {
			// 		return seq, err
			// 	}
			// 	fields[string(k)] = string(v)
			// }

			// fields["Source"] = fmt.Sprintf("lumberjack://%s%s", fields["host"], fields["file"])
			// fields["Offset"], _ = strconv.ParseInt(fields["offset"].(string), 10, 64)
			// fields["Line"] = uint64(seq)
			// t := fields["line"].(string)
			// fields["Text"] = &t

			// Send to the receiver which is a buffer. We block because...
			p.out <- fields
		case "2J": // JSON
			//log.Printf("Got JSON data")
			binary.Read(buff, binary.BigEndian, &seq)
			binary.Read(buff, binary.BigEndian, &count)
			jsonData := make([]byte, count)
			_, err := p.buffer.Read(jsonData)
			//log.Printf("Got message: %s", jsonData)

			if err != nil {
				return seq, err
			}

			var fields map[string]interface{}
			decoder := json.NewDecoder(strings.NewReader(string(jsonData)))
			//decoder.UseNumber()
			err = decoder.Decode(&fields)

			if err != nil {
				return seq, err
			}
			//fields["Source"] = fmt.Sprintf("lumberjack://%s%s", fields["host"], fields["file"])
			// jsonNumber := fields["offset"].(json.Number)
			// fields["Offset"], _ = jsonNumber.Int64()
			// fields["Line"] = uint64(seq)
			// t := fields["message"].(string)
			// fields["Text"] = &t

			// Send to the receiver which is a buffer. We block because...
			p.out <- fields
		default:
			return seq, fmt.Errorf("unknown type: %s", b)
		}
	}

	return seq, nil
}

// Parse initialises the read loop and begins parsing the incoming request
func (p *Parser) Parse() {
	defer close(p.out)
	b := make([]byte, 2)

Read:
	for {
		//TODO : handle congestion_threshold (readtimeout)
		n, err := p.Conn.Read(b)

		if err != nil || n == 0 {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				break Read
			}
			log.Printf("[%s] error reading %v", p.Conn.RemoteAddr().String(), err)
			break Read
		}

		switch string(b) {
		case "2W": // window length
			binary.Read(p.Conn, binary.BigEndian, &p.wlen)
		case "2C": // frame length
			binary.Read(p.Conn, binary.BigEndian, &p.plen)
			var seq uint32
			seq, err := p.read()

			if err != nil {
				log.Printf("[%s] error parsing %v", p.Conn.RemoteAddr().String(), err)
				break Read
			}

			if err := p.ack(seq); err != nil {
				log.Printf("[%s] error acking %v", p.Conn.RemoteAddr().String(), err)
				break Read
			}
		default:
			// This really shouldn't happen
			log.Printf("[%s] Received unknown type (%s): %s", p.Conn.RemoteAddr().String(), b, err)
			break Read
		}
	}
}
