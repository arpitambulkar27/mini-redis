package resp

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected [][]string
	}{
		{
			name:     "Single RESP command PING",
			input:    "*1\r\n$4\r\nPING\r\n",
			expected: [][]string{{"PING"}},
		},
		{
			name:     "RESP command ECHO with argument",
			input:    "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n",
			expected: [][]string{{"ECHO", "hello"}},
		},
		{
			name:     "Multiple RESP commands in single stream",
			input:    "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n",
			expected: [][]string{{"PING"}, {"ECHO", "hello world"}},
		},
		{
			name:     "Inline PING command",
			input:    "PING\r\n",
			expected: [][]string{{"PING"}},
		},
		{
			name:     "Inline ECHO command",
			input:    "ECHO hello\r\n",
			expected: [][]string{{"ECHO", "hello"}},
		},
		{
			name:     "RESP with bulk string containing spaces and newlines",
			input:    "*2\r\n$4\r\nECHO\r\n$8\r\nfoo\r\nbar\r\n",
			expected: [][]string{{"ECHO", "foo\r\nbar"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := NewReader(strings.NewReader(tc.input))
			for _, exp := range tc.expected {
				got, err := reader.ReadCommand()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !reflect.DeepEqual(got, exp) {
					t.Errorf("expected %v, got %v", exp, got)
				}
			}

			// Next read should be EOF
			_, err := reader.ReadCommand()
			if err != io.EOF {
				t.Errorf("expected EOF, got %v", err)
			}
		})
	}
}

func TestWriteSimpleString(t *testing.T) {
	var buf bytes.Buffer
	err := WriteSimpleString(&buf, "PONG")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "+PONG\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriteBulkString(t *testing.T) {
	var buf bytes.Buffer
	err := WriteBulkString(&buf, "hello")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "$5\r\nhello\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriteNullBulkString(t *testing.T) {
	var buf bytes.Buffer
	err := WriteNullBulkString(&buf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := "$-1\r\n"
	if buf.String() != expected {
		t.Errorf("expected %q, got %q", expected, buf.String())
	}
}

func TestWriteInteger(t *testing.T) {
	tests := []struct {
		val      int
		expected string
	}{
		{0, ":0\r\n"},
		{1, ":1\r\n"},
		{42, ":42\r\n"},
		{-5, ":-5\r\n"},
	}

	for _, tc := range tests {
		var buf bytes.Buffer
		err := WriteInteger(&buf, tc.val)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if buf.String() != tc.expected {
			t.Errorf("expected %q, got %q", tc.expected, buf.String())
		}
	}
}

func TestWriteError(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    "unknown command 'foo'",
			expected: "-ERR unknown command 'foo'\r\n",
		},
		{
			input:    "ERR unknown command 'bar'",
			expected: "-ERR unknown command 'bar'\r\n",
		},
	}

	for _, tc := range tests {
		var buf bytes.Buffer
		err := WriteError(&buf, tc.input)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if buf.String() != tc.expected {
			t.Errorf("expected %q, got %q", tc.expected, buf.String())
		}
	}
}

func TestHelperReadCommand(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n"
	rd := bufio.NewReader(strings.NewReader(input))
	tokens, err := ReadCommand(rd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tokens) != 1 || tokens[0] != "PING" {
		t.Errorf("expected [PING], got %v", tokens)
	}
}
