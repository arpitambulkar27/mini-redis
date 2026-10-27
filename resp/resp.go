package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reader reads and deserializes RESP2 commands from an underlying io.Reader.
type Reader struct {
	rd *bufio.Reader
}

// NewReader wraps an io.Reader (or reuses an existing *bufio.Reader) into a RESP Reader.
func NewReader(rd io.Reader) *Reader {
	if br, ok := rd.(*bufio.Reader); ok {
		return &Reader{rd: br}
	}
	return &Reader{rd: bufio.NewReader(rd)}
}

// ReadCommand parses an incoming RESP command from the reader.
// It supports RESP Array of Bulk Strings (the standard format redis-cli sends,
// starting with *<count>\r\n followed by $<len>\r\n<data>\r\n) as well as
// inline commands (e.g. from netcat or telnet).
// Tokens are extracted into a slice of strings (e.g. []string{"PING"} or []string{"ECHO", "hello"}).
func (r *Reader) ReadCommand() ([]string, error) {
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}

		line = strings.TrimRight(line, "\r\n")
		if len(line) == 0 {
			// Skip empty lines between commands or trailing newlines
			continue
		}

		// RESP Array of Bulk Strings: *<count>\r\n
		if line[0] == '*' {
			count, err := strconv.Atoi(line[1:])
			if err != nil || count < 0 {
				return nil, fmt.Errorf("protocol error: invalid multibulk length %q", line)
			}
			if count == 0 {
				return []string{}, nil
			}

			tokens := make([]string, 0, count)
			for i := 0; i < count; i++ {
				bulkHeader, err := r.readLine()
				if err != nil {
					return nil, err
				}
				bulkHeader = strings.TrimRight(bulkHeader, "\r\n")
				if len(bulkHeader) == 0 || bulkHeader[0] != '$' {
					return nil, fmt.Errorf("protocol error: expected '$' for bulk string, got %q", bulkHeader)
				}

				bulkLen, err := strconv.Atoi(bulkHeader[1:])
				if err != nil {
					return nil, fmt.Errorf("protocol error: invalid bulk length %q", bulkHeader)
				}

				if bulkLen < 0 {
					// Null bulk string
					tokens = append(tokens, "")
					continue
				}

				data := make([]byte, bulkLen)
				if _, err := io.ReadFull(r.rd, data); err != nil {
					return nil, err
				}

				// Consume trailing \r\n after data
				crlf := make([]byte, 2)
				if _, err := io.ReadFull(r.rd, crlf); err != nil {
					return nil, err
				}
				if crlf[0] != '\r' || crlf[1] != '\n' {
					return nil, errors.New("protocol error: expected CRLF following bulk string data")
				}

				tokens = append(tokens, string(data))
			}
			return tokens, nil
		}

		// Inline command fallback (e.g. "PING\r\n" or "ECHO hello\r\n")
		fields := strings.Fields(line)
		if len(fields) > 0 {
			return fields, nil
		}
	}
}

// readLine reads a line up to '\n'.
func (r *Reader) readLine() (string, error) {
	return r.rd.ReadString('\n')
}

// ReadCommand is a helper function to parse a command directly from a *bufio.Reader.
func ReadCommand(rd *bufio.Reader) ([]string, error) {
	return NewReader(rd).ReadCommand()
}

// --- RESP Serializer (Writer) Helpers ---

// WriteSimpleString writes a RESP simple string: +<string>\r\n.
// Uses fast path if w implements *bufio.Writer to minimize allocations.
func WriteSimpleString(w io.Writer, s string) error {
	if bw, ok := w.(*bufio.Writer); ok {
		bw.WriteByte('+')
		bw.WriteString(s)
		_, err := bw.WriteString("\r\n")
		return err
	}
	_, err := fmt.Fprintf(w, "+%s\r\n", s)
	return err
}

// WriteBulkString writes a RESP bulk string: $<len>\r\n<data>\r\n.
// Uses fast path if w implements *bufio.Writer.
func WriteBulkString(w io.Writer, s string) error {
	if bw, ok := w.(*bufio.Writer); ok {
		bw.WriteByte('$')
		bw.WriteString(strconv.Itoa(len(s)))
		bw.WriteString("\r\n")
		bw.WriteString(s)
		_, err := bw.WriteString("\r\n")
		return err
	}
	_, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// WriteNullBulkString writes a RESP null bulk string: $-1\r\n.
func WriteNullBulkString(w io.Writer) error {
	if bw, ok := w.(*bufio.Writer); ok {
		_, err := bw.WriteString("$-1\r\n")
		return err
	}
	_, err := fmt.Fprintf(w, "$-1\r\n")
	return err
}

// WriteInteger writes a RESP integer: :<number>\r\n.
func WriteInteger(w io.Writer, n int) error {
	if bw, ok := w.(*bufio.Writer); ok {
		bw.WriteByte(':')
		bw.WriteString(strconv.Itoa(n))
		_, err := bw.WriteString("\r\n")
		return err
	}
	_, err := fmt.Fprintf(w, ":%d\r\n", n)
	return err
}

// WriteError writes a RESP error: -ERR <message>\r\n.
func WriteError(w io.Writer, msg string) error {
	if bw, ok := w.(*bufio.Writer); ok {
		bw.WriteByte('-')
		if !strings.HasPrefix(msg, "ERR ") {
			bw.WriteString("ERR ")
		}
		bw.WriteString(msg)
		_, err := bw.WriteString("\r\n")
		return err
	}
	if strings.HasPrefix(msg, "ERR ") {
		_, err := fmt.Fprintf(w, "-%s\r\n", msg)
		return err
	}
	_, err := fmt.Fprintf(w, "-ERR %s\r\n", msg)
	return err
}
