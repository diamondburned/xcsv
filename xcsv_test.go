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
		testUnmarshal(t, "1",
			[][]string{{"1"}},
			[]struct{ A int }{{1}},
		)
		testUnmarshal(t, "2",
			[][]string{{"1", "2"}},
			[]struct{ A int }{{1}},
		)
		testUnmarshal(t, "3",
			[][]string{{"foo"}},
			[]struct{ A string }{{"foo"}},
		)
		testUnmarshal(t, "4",
			[][]string{{"foo", "bar"}},
			[]struct{ A string }{{"foo"}},
			AllowMissingFields(),
		)
	})

	t.Run("multiple", func(t *testing.T) {
		testUnmarshal(t, "1",
			[][]string{
				{"1", "2"},
				{"3", "4"},
			},
			[]struct{ A, B int }{
				{1, 2},
				{3, 4},
			},
		)
		testUnmarshal(t, "2",
			[][]string{
				{"foo", "bar"},
				{"baz", "qux"},
			},
			[]struct{ A, B string }{
				{"foo", "bar"},
				{"baz", "qux"},
			},
		)
	})

	t.Run("text marshaling", func(t *testing.T) {
		testUnmarshal(t, "1",
			[][]string{{"foo"}},
			[]struct{ A textMarshaling }{{textMarshaling{"foo"}}},
		)
	})

	t.Run("allow missing fields", func(t *testing.T) {
		testUnmarshal(t, "1",
			[][]string{
				{"1"},
				{"3"},
			},
			[]struct{ A, B int }{
				{1, 0},
				{3, 0},
			},
			AllowMissingFields(),
		)
	})

	t.Run("skip header", func(t *testing.T) {
		// v1 := Unmarshal[struct{ A, B int }](mustReader(t, [][]string{
		// 	{"A", "B"},
		// 	{"1", "2"},
		// 	{"3", "4"},
		// }), SkipHeader())
		// assertIterSlice(t, v1, []struct{ A, B int }{
		// 	{1, 2},
		// 	{3, 4},
		// })
		testUnmarshal(t, "1",
			[][]string{
				{"A", "B"},
				{"1", "2"},
				{"3", "4"},
			},
			[]struct{ A, B int }{
				{1, 2},
				{3, 4},
			},
			SkipHeader(),
		)
	})
}

func testUnmarshal[T any](t *testing.T, name string, records [][]string, want []T, opts ...UnmarshalOpt) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		v := Unmarshal[T](mustReader(t, records), opts...)
		assertIterSlice(t, v, want)
	})
}

func assertIterSlice[T any](t *testing.T, seq iter.Seq2[T, error], expected []T) {
	t.Helper()

	var i int
	for v, err := range seq {
		t.Logf("record %d: %v", i, v)
		assert.NoError(t, err, fmt.Sprintf("record %d should have no error", i))
		assert.Equal(t, expected[i], v, fmt.Sprintf("record %d should be equal", i))
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
