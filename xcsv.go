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
	"strconv"
)

// UnmarshalFile reads the CSV file from filepath and unmarshals it into v.
func UnmarshalFile[T any](filepath string) (iter.Seq2[T, error], error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open %q: %w", filepath, err)
	}
	defer f.Close()

	return Unmarshal[T](csv.NewReader(f)), nil
}

// Unmarshal reads the CSV file from r and unmarshals it into v.
func Unmarshal[T any](r *csv.Reader) iter.Seq2[T, error] {
	var z T
	zt := reflect.TypeOf(z)
	if zt.Kind() != reflect.Struct {
		panic("expected struct")
	}

	numFields := zt.NumField()
	value := reflect.New(zt).Elem()

	return func(yield func(T, error) bool) {
		for {
			record, err := r.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				err = fmt.Errorf("failed to read CSV record: %w", err)
				if !yield(z, err) {
					return
				}
			}

			// Ensure at least numFields fields are present.
			if len(record) < numFields {
				err := fmt.Errorf("expected %d fields, got %d", numFields, len(record))
				if !yield(z, err) {
					return
				}
			}

			for i := 0; i < numFields; i++ {
				err := unmarshalCell(record[i], value.Field(i))
				if err != nil {
					err = fmt.Errorf("failed to unmarshal field %d: %w", i, err)
					if !yield(z, err) {
						return
					}
				}
			}

			if !yield(value.Interface().(T), nil) {
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
