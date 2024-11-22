package xcsv

import (
	"encoding"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"reflect"
	"slices"
	"strconv"
)

type unmarshalOpts struct {
	allowMissingFields bool
	errorEarly         bool
	skipHeader         bool
}

// UnmarshalOpt is a function that modifies the behavior of Unmarshal.
type UnmarshalOpt func(o *unmarshalOpts)

// AllowMissingFields allows the CSV file to potentially have fewer columns than
// the struct. Columns that are missing will be set to their zero value.
//
// Note that Unmarshal will always allow the CSV file to have *more* columns
// than the struct regardless if this option is used.
func AllowMissingFields() UnmarshalOpt {
	return func(o *unmarshalOpts) { o.allowMissingFields = true }
}

// ErrorEarly stops unmarshalling as soon as an error is encountered.
func ErrorEarly() UnmarshalOpt {
	return func(o *unmarshalOpts) { o.errorEarly = true }
}

// SkipHeader skips the first row of the CSV file if it matches the column
// names of the struct.
func SkipHeader() UnmarshalOpt {
	return func(o *unmarshalOpts) { o.skipHeader = true }
}

// RecordUnmarshalingError is an error that occurs when unmarshaling a single
// record. It contains thee record itself and the error that occurred.
type RecordUnmarshalingError struct {
	Record []string `json:"record"`
	Line   int      `json:"line"`
	err    error
}

// Error returns the error message.
func (e *RecordUnmarshalingError) Error() string {
	return fmt.Sprintf("error at record %d %q: %s", e.Line, e.Record, e.err)
}

// Unwrap returns the underlying error.
func (e *RecordUnmarshalingError) Unwrap() error {
	return e.err
}

// UnmarshalFile reads the CSV file from filepath and unmarshals it into v.
func UnmarshalFile[T any](filepath string, opts ...UnmarshalOpt) (iter.Seq2[T, error], error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %q: %w", filepath, err)
	}
	defer f.Close()

	return Unmarshal[T](csv.NewReader(f), opts...), nil
}

// Unmarshal reads the CSV file from r and unmarshals it into v.
func Unmarshal[T any](r *csv.Reader, opts ...UnmarshalOpt) iter.Seq2[T, error] {
	var o unmarshalOpts
	for _, opt := range opts {
		opt(&o)
	}

	var z T
	zt := reflect.TypeOf(z)
	if zt.Kind() != reflect.Struct {
		panic("expected struct")
	}

	numFields := zt.NumField()

	newValue := reflect.New(zt).Elem()

	fieldTypes := make([]reflect.Type, numFields)
	for i := range numFields {
		fieldTypes[i] = zt.Field(i).Type
	}

	return func(yield func(T, error) bool) {
		var errored bool
		for line := 0; !o.errorEarly || !errored; line++ {
			record, err := r.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				err = fmt.Errorf("failed to read CSV record: %w", err)
				if !yield(z, err) {
					return
				}
				errored = true
				continue
			}

			if !o.allowMissingFields && len(record) < numFields {
				if !yield(z, &RecordUnmarshalingError{
					Record: record,
					Line:   line,
					err:    fmt.Errorf("wanted exactly %d columns but record had %d", numFields, len(record)),
				}) {
					return
				}
				errored = true
				continue
			}

			// This is the same if allowMissingFields is false but allows us to
			// skip columns if it's true.
			numCols := min(numFields, len(record))

			if line == 0 && o.skipHeader {
				columnNames := ColumnNames[T]()
				if slices.Equal(record[:numCols], columnNames[:numCols]) {
					continue
				}
				// Not the header, so treat it as a normal record.
			}

			col := 0
			for ; col < numCols; col++ {
				if err := unmarshalCell(record[col], newValue.Field(col)); err != nil {
					if !yield(z, &RecordUnmarshalingError{
						Record: record,
						Line:   line,
						err:    fmt.Errorf("failed to unmarshal field %d: %w", col, err),
					}) {
						return
					}
					errored = true
					continue
				}
			}
			for ; col < numFields; col++ {
				newValue.Field(col).Set(reflect.Zero(fieldTypes[col]))
			}

			if !yield(newValue.Interface().(T), nil) {
				return
			}
		}
	}
}

func unmarshalCell(v string, dst reflect.Value) error {
	t := dst.Type()
	if textUnmarshaler, ok := dst.Addr().Interface().(encoding.TextUnmarshaler); ok {
		return textUnmarshaler.UnmarshalText([]byte(v))
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		i, err := strconv.ParseInt(v, 10, t.Bits())
		if err != nil {
			return fmt.Errorf("cannot parse %q as %s: %w", v, t, err)
		}
		dst.SetInt(i)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		i, err := strconv.ParseUint(v, 10, t.Bits())
		if err != nil {
			return fmt.Errorf("cannot parse %q as %s: %w", v, t, err)
		}
		dst.SetUint(i)
		return nil
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(v, t.Bits())
		if err != nil {
			return fmt.Errorf("cannot parse %q as %s: %w", v, t, err)
		}
		dst.SetFloat(f)
		return nil
	case reflect.String:
		dst.SetString(v)
		return nil
	default:
		return fmt.Errorf("cannot parse %q as %s: unsupported type", v, t)
	}
}

// ColumnNames returns the names of the fields in a struct.
// It can be used with [Marshal] to write a CSV file with a column header row.
// If a field has a `csv` tag, its value will be used instead of the field name.
// Note that this does not support `csv:"-"`.
func ColumnNames[T any]() []string {
	zt := reflect.TypeFor[T]()
	if zt.Kind() != reflect.Struct {
		panic("expected struct")
	}

	numFields := zt.NumField()

	columns := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		name := zt.Field(i).Name
		if tag := zt.Field(i).Tag.Get("csv"); tag != "" {
			name = tag
		}
		columns[i] = name
	}

	return columns
}

// MarshalFile writes the CSV representation of values to filepath.
func MarshalFile[T any](filepath string, values iter.Seq[T]) error {
	f, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create %q: %w", filepath, err)
	}

	if err := Marshal(csv.NewWriter(f), values); err != nil {
		return fmt.Errorf("failed to marshal: %w", err)
	}

	return nil
}

// Marshal writes the CSV representation of values to w.
func Marshal[T any](w *csv.Writer, values iter.Seq[T]) error {
	zt := reflect.TypeFor[T]()
	if zt.Kind() != reflect.Struct {
		panic("expected struct")
	}

	numFields := zt.NumField()

	for v := range values {
		record, err := fieldsValue(v, numFields)
		if err != nil {
			return fmt.Errorf("xcsv: type %s: %w", zt.String(), err)
		}

		if err := w.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	w.Flush()
	return w.Error()
}

// FieldsValue returns the values of the fields in a struct as a slice of
// strings (a CSV row).
func FieldsValue(v any) ([]string, error) {
	return fieldsValue(v, reflect.TypeOf(v).NumField())
}

func fieldsValue(v any, numFields int) ([]string, error) {
	rvalue := reflect.ValueOf(v)

	record := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		v, err := marshalField(rvalue, i)
		if err != nil {
			return nil, fmt.Errorf(
				"field %q: %w",
				rvalue.Type().Field(i).Name, err)
		}
		record[i] = v
	}

	return record, nil
}

func marshalField(rvalue reflect.Value, i int) (string, error) {
	rfield := rvalue.Field(i)
	if textMarshaler, ok := rfield.Interface().(encoding.TextMarshaler); ok {
		text, err := textMarshaler.MarshalText()
		return string(text), err
	}

	switch rfield.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rfield.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rfield.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(rfield.Float(), 'f', -1, rfield.Type().Bits()), nil
	case reflect.String:
		return rfield.String(), nil
	default:
		return "", fmt.Errorf("unsupported type %s", rfield.Type())
	}
}
