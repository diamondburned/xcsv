package xcsv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"iter"
	"slices"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestUnmarshal(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		v1 := Unmarshal[struct{}](mustReader(t, [][]string{}))
		assertIterSlice(t, v1, []struct{}{})

		v2 := Unmarshal[struct{ A int }](mustReader(t, [][]string{}))
		assertIterSlice(t, v2, []struct{ A int }{})
	})

	t.Run("single", func(t *testing.T) {
		v1 := Unmarshal[struct{ A int }](mustReader(t, [][]string{
			{"1"},
		}))
		assertIterSlice(t, v1, []struct{ A int }{{1}})

		v2 := Unmarshal[struct{ A int }](mustReader(t, [][]string{
			{"1", "2"},
		}))
		assertIterSlice(t, v2, []struct{ A int }{{1}})

		v3 := Unmarshal[struct{ A string }](mustReader(t, [][]string{
			{"foo"},
		}))
		assertIterSlice(t, v3, []struct{ A string }{{"foo"}})

		v4 := Unmarshal[struct{ A string }](mustReader(t, [][]string{
			{"foo", "bar"},
		}))
		assertIterSlice(t, v4, []struct{ A string }{{"foo"}})
	})

	t.Run("multiple", func(t *testing.T) {
		v1 := Unmarshal[struct{ A, B int }](mustReader(t, [][]string{
			{"1", "2"},
			{"3", "4"},
		}))
		assertIterSlice(t, v1, []struct{ A, B int }{
			{1, 2},
			{3, 4},
		})

		v2 := Unmarshal[struct{ A, B string }](mustReader(t, [][]string{
			{"foo", "bar"},
			{"baz", "qux"},
		}))
		assertIterSlice(t, v2, []struct{ A, B string }{
			{"foo", "bar"},
			{"baz", "qux"},
		})
	})

	t.Run("text marshaling", func(t *testing.T) {
		v1 := Unmarshal[struct{ A textMarshaling }](mustReader(t, [][]string{
			{"foo"},
		}))
		assertIterSlice(t, v1, []struct{ A textMarshaling }{{
			textMarshaling{"foo"},
		}})
	})

	t.Run("allow missing fields", func(t *testing.T) {
		v1 := Unmarshal[struct{ A, B int }](mustReader(t, [][]string{
			{"1"},
			{"3"},
		}), AllowMissingFields())
		assertIterSlice(t, v1, []struct{ A, B int }{
			{1, 0},
			{3, 0},
		})
	})
}

func assertIterSlice[T any](t *testing.T, seq iter.Seq2[T, error], expected []T) {
	t.Helper()

	var i int
	for v, err := range seq {
		t.Logf("index %d: %v", i, v)
		assert.NoError(t, err)
		assert.Equal(t, expected[i], v, fmt.Sprintf("index %d", i))
		i++
	}
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
		err := Marshal(csv.NewWriter(&b), slices.Values([]struct{ A, B int }{
			{1, 2},
			{3, 4},
		}))
		assert.NoError(t, err)
		assert.Equal(t, "1,2\n3,4\n", b.String())
	})

	t.Run("strings", func(t *testing.T) {
		var b bytes.Buffer
		err := Marshal(csv.NewWriter(&b), slices.Values([]struct{ A, B string }{
			{"foo", "bar"},
			{"baz", "qux"},
		}))
		assert.NoError(t, err)
		assert.Equal(t, "foo,bar\nbaz,qux\n", b.String())
	})

	t.Run("text marshaling", func(t *testing.T) {
		var b bytes.Buffer
		err := Marshal(csv.NewWriter(&b), slices.Values([]struct{ A textMarshaling }{
			{textMarshaling{"1"}},
			{textMarshaling{"2"}},
		}))
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
