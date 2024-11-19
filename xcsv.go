package xcsv

import (
	"encoding"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
)

// UnmarshalFile reads the CSV file from filepath and unmarshals it into v.
func UnmarshalFile[T any](filepath string) ([]T, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %q: %w", filepath, err)
	}
	defer f.Close()

	return Unmarshal[T](csv.NewReader(f))
}

// Unmarshal reads the CSV file from r and unmarshals it into v.
func Unmarshal[T any](r *csv.Reader) ([]T, error) {
	var z T
	zt := reflect.TypeOf(z)
	if zt.Kind() != reflect.Struct {
		panic("expected struct")
	}

	numFields := zt.NumField()

	values := reflect.MakeSlice(reflect.SliceOf(zt), 0, 0)
	value := reflect.New(zt).Elem()

	for {
		record, err := r.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read CSV record: %w", err)
		}

		// Ensure at least numFields fields are present.
		if len(record) < numFields {
			return nil, fmt.Errorf("expected %d fields, got %d", numFields, len(record))
		}

		for i := 0; i < numFields; i++ {
			if err := unmarshalCell(record[i], value.Field(i)); err != nil {
				return nil, fmt.Errorf("failed to unmarshal field %d: %w", i, err)
			}
		}

		values = reflect.Append(values, value)
	}

	return values.Interface().([]T), nil
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
func MarshalFile[T any](filepath string, values []T) error {
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
func Marshal[T any](w *csv.Writer, values []T) error {
	zt := reflect.TypeFor[T]()
	if zt.Kind() != reflect.Struct {
		panic("expected struct")
	}

	numFields := zt.NumField()

	rvalues := reflect.ValueOf(values)

	for i := range values {
		rvalue := rvalues.Index(i)

		record := make([]string, numFields)
		for i := 0; i < numFields; i++ {
			v, err := marshalField(rvalue, i)
			if err != nil {
				return fmt.Errorf(
					"failed to marshal %s.%s: %w",
					zt, zt.Field(i).Name, err)
			}
			record[i] = v
		}

		if err := w.Write(record); err != nil {
			return fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	w.Flush()
	return w.Error()
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
