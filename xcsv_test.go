package xcsv

import (
	"bytes"
	"encoding/csv"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestUnmarshal(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		v1, err := Unmarshal[struct{}](mustReader(t, [][]string{}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{}{}, v1)

		v2, err := Unmarshal[struct{ A int }](mustReader(t, [][]string{}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A int }{}, v2)
	})

	t.Run("single", func(t *testing.T) {
		v1, err := Unmarshal[struct{ A int }](mustReader(t, [][]string{
			{"1"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A int }{{1}}, v1)

		v2, err := Unmarshal[struct{ A int }](mustReader(t, [][]string{
			{"1", "2"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A int }{{1}}, v2)

		v3, err := Unmarshal[struct{ A string }](mustReader(t, [][]string{
			{"foo"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A string }{{"foo"}}, v3)

		v4, err := Unmarshal[struct{ A string }](mustReader(t, [][]string{
			{"foo", "bar"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A string }{{"foo"}}, v4)
	})

	t.Run("multiple", func(t *testing.T) {
		v1, err := Unmarshal[struct{ A, B int }](mustReader(t, [][]string{
			{"1", "2"},
			{"3", "4"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A, B int }{{1, 2}, {3, 4}}, v1)

		v2, err := Unmarshal[struct{ A, B string }](mustReader(t, [][]string{
			{"foo", "bar"},
			{"baz", "qux"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A, B string }{{"foo", "bar"}, {"baz", "qux"}}, v2)
	})

	t.Run("text marshaling", func(t *testing.T) {
		v1, err := Unmarshal[struct{ A textMarshaling }](mustReader(t, [][]string{
			{"foo"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, []struct{ A textMarshaling }{{textMarshaling{"foo"}}}, v1)
	})
}

func mustReader(t *testing.T, records [][]string) *csv.Reader {
	t.Helper()

	var buf bytes.Buffer

	w := csv.NewWriter(&buf)
	if err := w.WriteAll(records); err != nil {
		t.Fatal(err)
	}

	return csv.NewReader(&buf)
}

func TestMarshal(t *testing.T) {
	t.Run("numbers", func(t *testing.T) {
		var b bytes.Buffer
		err := Marshal(csv.NewWriter(&b), []struct{ A, B int }{
			{1, 2},
			{3, 4},
		})
		assert.NoError(t, err)
		assert.Equal(t, "1,2\n3,4\n", b.String())
	})

	t.Run("strings", func(t *testing.T) {
		var b bytes.Buffer
		err := Marshal(csv.NewWriter(&b), []struct{ A, B string }{
			{"foo", "bar"},
			{"baz", "qux"},
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo,bar\nbaz,qux\n", b.String())
	})

	t.Run("text marshaling", func(t *testing.T) {
		var b bytes.Buffer
		err := Marshal(csv.NewWriter(&b), []struct{ A textMarshaling }{
			{textMarshaling{"1"}},
			{textMarshaling{"2"}},
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo\nfoo\n", b.String())
	})
}

type textMarshaling struct {
	thing string
}

func (textMarshaling) MarshalText() ([]byte, error) {
	return []byte("foo"), nil
}

func (m *textMarshaling) UnmarshalText(text []byte) error {
	m.thing = string(text)
	return nil
}
