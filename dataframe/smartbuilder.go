// Copyright 2019 Nick Poorman
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataframe

import (
	"fmt"
	"os"
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/decimal128"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/go-bullseye/bullseye/types"
	"github.com/pkg/errors"
)

// AppenderFunc is the function to be used to convert the data to the correct type.
type AppenderFunc func(array.Builder, interface{})

// SmartBuilder knows how to convert to the correct type when building.
type SmartBuilder struct {
	recordBuilder  *array.RecordBuilder
	schema         *arrow.Schema
	fieldAppenders []AppenderFunc
}

// NewSmartBuilder creates a SmartBuilder that knows how to convert to the correct type when building.
func NewSmartBuilder(recordBuilder *array.RecordBuilder, schema *arrow.Schema) *SmartBuilder {
	sb := &SmartBuilder{
		recordBuilder:  recordBuilder,
		schema:         schema,
		fieldAppenders: make([]AppenderFunc, 0, len(schema.Fields())),
	}

	fields := sb.schema.Fields()
	for i := range fields {
		fn := initFieldAppender(&fields[i])
		sb.fieldAppenders = append(sb.fieldAppenders, fn)
	}

	return sb
}

// Append will append the value to the builder.
func (sb *SmartBuilder) Append(fieldIndex int, v interface{}) {
	field := sb.recordBuilder.Field(fieldIndex)
	appendFunc := sb.fieldAppenders[fieldIndex]
	if appendFunc == nil {
		fmt.Fprintln(os.Stderr, "warn: appendFunc is nil")
	}
	appendFunc(field, v)
}

// TODO(nickpoorman): Add the rest of the data types.
func initFieldAppender(field *arrow.Field) AppenderFunc {
	switch field.Type.(type) {
	case *arrow.BooleanType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.BooleanBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(bool)
				builder.Append(vT)
			}
		}
	case *arrow.Int8Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int8Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int8)
				builder.Append(vT)
			}
		}
	case *arrow.Int16Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int16Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int16)
				builder.Append(vT)
			}
		}
	case *arrow.Int32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int32)
				builder.Append(vT)
			}
		}
	case *arrow.Int64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(int64)
				builder.Append(vT)
			}
		}
	case *arrow.Uint8Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint8Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint8)
				builder.Append(vT)
			}
		}
	case *arrow.Uint16Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint16Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint16)
				builder.Append(vT)
			}
		}
	case *arrow.Uint32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint32)
				builder.Append(vT)
			}
		}
	case *arrow.Uint64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(uint64)
				builder.Append(vT)
			}
		}
	case *arrow.Float32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Float32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(float32)
				builder.Append(vT)
			}
		}
	case *arrow.Float64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Float64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(float64)
				builder.Append(vT)
			}
		}
	case *arrow.StringType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.StringBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(string)
				builder.Append(vT)
			}
		}

	case *arrow.TimestampType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.TimestampBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT arrow.Timestamp
				switch t := v.(type) {
				case int64:
					vT = arrow.Timestamp(t)
				case arrow.Timestamp:
					vT = t
				default:
					vT = arrow.Timestamp(v.(int64))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Time32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Time32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT arrow.Time32
				switch t := v.(type) {
				case int32:
					vT = arrow.Time32(t)
				case arrow.Time32:
					vT = t
				default:
					vT = arrow.Time32(v.(int32))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Time64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Time64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT arrow.Time64
				switch t := v.(type) {
				case int64:
					vT = arrow.Time64(t)
				case arrow.Time64:
					vT = t
				default:
					vT = arrow.Time64(v.(int64))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Date32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Date32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT arrow.Date32
				switch t := v.(type) {
				case int32:
					vT = arrow.Date32(t)
				case arrow.Date32:
					vT = t
				default:
					vT = arrow.Date32(v.(int32))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Date64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Date64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT arrow.Date64
				switch t := v.(type) {
				case int64:
					vT = arrow.Date64(t)
				case arrow.Date64:
					vT = t
				default:
					vT = arrow.Date64(v.(int64))
				}
				builder.Append(vT)
			}
		}

	case *arrow.DurationType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.DurationBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT arrow.Duration
				switch t := v.(type) {
				case int64:
					vT = arrow.Duration(t)
				case arrow.Duration:
					vT = t
				default:
					vT = arrow.Duration(v.(int64))
				}
				builder.Append(vT)
			}
		}

	case *arrow.MonthIntervalType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.MonthIntervalBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT arrow.MonthInterval
				switch t := v.(type) {
				case int32:
					vT = arrow.MonthInterval(t)
				case arrow.MonthInterval:
					vT = t
				default:
					vT = arrow.MonthInterval(v.(int32))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Float16Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Float16Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT float16.Num
				switch t := v.(type) {
				case float32:
					vT = float16.New(t)
				case float16.Num:
					vT = t
				default:
					vT = float16.New(v.(float32))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Decimal128Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Decimal128Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				var vT decimal128.Num
				switch t := v.(type) {
				case uint64:
					vT = decimal128.FromU64(t)
				case int64:
					vT = decimal128.FromI64(t)
				case decimal128.Num:
					vT = t
				case types.Signed128BitInteger:
					vT = decimal128.New(t.Hi, t.Lo)
				default:
					vT = v.(decimal128.Num)
				}
				builder.Append(vT)
			}
		}

	case *arrow.DayTimeIntervalType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.DayTimeIntervalBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT := v.(arrow.DayTimeInterval)
				builder.Append(vT)
			}
		}

	case *arrow.ListType:
		return func(b array.Builder, v interface{}) {
			builder := b.(*array.ListBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				sub := builder.ValueBuilder()
				fmt.Printf("list type value: %v\n", v)
				v := reflectValueOfNonPointer(v).Elem()
				sub.Reserve(v.Len())
				builder.Append(true)
				for i := 0; i < v.Len(); i++ {
					appendValue(sub, v.Index(i).Interface())
				}
			}
		}

	case *arrow.FixedSizeListType:
		return func(b array.Builder, v interface{}) {
			builder := b.(*array.FixedSizeListBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				sub := builder.ValueBuilder()
				v := reflect.ValueOf(v).Elem()
				sub.Reserve(v.Len())
				builder.Append(true)
				for i := 0; i < v.Len(); i++ {
					appendValue(sub, v.Index(i).Interface())
				}
			}
		}

	case *arrow.StructType:
		return func(b array.Builder, v interface{}) {
			builder := b.(*array.StructBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				builder.Append(true)
				v := reflect.ValueOf(v).Elem()
				for i := 0; i < builder.NumField(); i++ {
					f := builder.FieldBuilder(i)
					appendValue(f, v.Field(i).Interface())
				}
			}
		}

	default:
		panic(fmt.Errorf("dataframe/smartbuilder: unhandled field type %T", field.Type))
	}
}

// TODO(nickpoorman): Write test that will test all the data types.
// TODO(nickpoorman): Add the rest of the data types.
func appendValue(bldr array.Builder, v interface{}) {
	fmt.Printf("appendValue: |%v| - %T\n", v, bldr)
	switch b := bldr.(type) {
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	case *array.Int8Builder:
		b.Append(v.(int8))
	case *array.Int16Builder:
		b.Append(v.(int16))
	case *array.Int32Builder:
		b.Append(v.(int32))
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.Uint8Builder:
		b.Append(v.(uint8))
	case *array.Uint16Builder:
		b.Append(v.(uint16))
	case *array.Uint32Builder:
		b.Append(v.(uint32))
	case *array.Uint64Builder:
		b.Append(v.(uint64))
	case *array.Float32Builder:
		b.Append(v.(float32))
	case *array.Float64Builder:
		b.Append(v.(float64))
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.Date32Builder:
		b.Append(arrow.Date32(v.(int32)))

	case *array.ListBuilder:
		b.Append(true)
		sub := b.ValueBuilder()
		v := reflect.ValueOf(v)
		for i := 0; i < v.Len(); i++ {
			appendValue(sub, v.Index(i).Interface())
		}

	case *array.FixedSizeListBuilder:
		b.Append(true)
		sub := b.ValueBuilder()
		v := reflect.ValueOf(v)
		for i := 0; i < v.Len(); i++ {
			appendValue(sub, v.Index(i).Interface())
		}

	case *array.StructBuilder:
		b.Append(true)
		v := reflect.ValueOf(v)
		for i := 0; i < b.NumField(); i++ {
			f := b.FieldBuilder(i)
			appendValue(f, v.Field(i).Interface())
		}

	default:
		panic(errors.Errorf("dataframe/smartbuilder: unhandled Arrow builder type %T", b))
	}
}

// If the type of v is a pointer return the pointer as a value,
// otherwise create a new pointer to the value.
func reflectValueOfNonPointer(v interface{}) reflect.Value {
	var ptr reflect.Value
	value := reflect.ValueOf(v)
	if value.Type().Kind() == reflect.Ptr {
		ptr = value
	} else {
		ptr = reflect.New(reflect.TypeOf(v)) // create new pointer
		temp := ptr.Elem()                   // create variable to value of pointer
		temp.Set(value)                      // set value of variable to our passed in value
	}
	return ptr
}
