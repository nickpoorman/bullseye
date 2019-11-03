// Code generated by dataframe/smartbuilder.gen.go.tmpl. DO NOT EDIT.

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
	"reflect"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/go-bullseye/bullseye/types"
	"github.com/pkg/errors"
)

// TODO(nickpoorman): Add the rest of the data types.
// TODO(nickpoorman): Add null, etc. to types.tmpldata.
func (sb *SmartBuilder) initFieldAppender(field *arrow.Field) AppenderFunc {
	switch field.Type.(type) {

	case *arrow.Int64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToInt64(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to int64", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Uint64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToUint64(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to uint64", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Int32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToInt32(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to int32", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Uint32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToUint32(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to uint32", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Float64Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Float64Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToFloat64(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to float64", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Float32Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Float32Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToFloat32(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to float32", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Int16Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int16Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToInt16(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to int16", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Uint16Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint16Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToUint16(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to uint16", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Int8Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Int8Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToInt8(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to int8", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.Uint8Type:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.Uint8Builder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToUint8(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to uint8", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.TimestampType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.TimestampBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToTimestamp(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.Timestamp", v))
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
				vT, ok := types.CastToTime32(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.Time32", v))
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
				vT, ok := types.CastToTime64(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.Time64", v))
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
				vT, ok := types.CastToDate32(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.Date32", v))
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
				vT, ok := types.CastToDate64(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.Date64", v))
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
				vT, ok := types.CastToDuration(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.Duration", v))
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
				vT, ok := types.CastToMonthInterval(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.MonthInterval", v))
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
				vT, ok := types.CastToFloat16(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to float16.Num", v))
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
				vT, ok := types.CastToDecimal128(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to decimal128.Num", v))
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
				vT, ok := types.CastToDayTimeInterval(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to arrow.DayTimeInterval", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.BooleanType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.BooleanBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToBoolean(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to bool", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.StringType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.StringBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToString(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to string", v))
				}
				builder.Append(vT)
			}
		}

	case *arrow.NullType:
		return func(field array.Builder, v interface{}) {
			builder := field.(*array.NullBuilder)
			if v == nil {
				builder.AppendNull()
			} else {
				vT, ok := types.CastToNull(v)
				if !ok {
					panic(fmt.Sprintf("cannot cast %T to interface{}", v))
				}
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
					sb.appendValue(sub, v.Index(i).Interface())
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
					sb.appendValue(sub, v.Index(i).Interface())
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
					sb.appendValue(f, v.Field(i).Interface())
				}
			}
		}

	default:
		panic(fmt.Errorf("dataframe/smartbuilder: unhandled field type %T", field.Type))
	}
}

// TODO(nickpoorman): Write test that will test all the data types.
// TODO(nickpoorman): Add the rest of the data types.
func (sb *SmartBuilder) appendValue(bldr array.Builder, v interface{}) {
	fmt.Printf("appendValue: |%v| - %T\n", v, bldr)
	switch b := bldr.(type) {

	case *array.Int64Builder:
		vT, ok := types.CastToInt64(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to int64", v))
		}
		b.Append(vT)

	case *array.Uint64Builder:
		vT, ok := types.CastToUint64(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to uint64", v))
		}
		b.Append(vT)

	case *array.Int32Builder:
		vT, ok := types.CastToInt32(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to int32", v))
		}
		b.Append(vT)

	case *array.Uint32Builder:
		vT, ok := types.CastToUint32(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to uint32", v))
		}
		b.Append(vT)

	case *array.Float64Builder:
		vT, ok := types.CastToFloat64(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to float64", v))
		}
		b.Append(vT)

	case *array.Float32Builder:
		vT, ok := types.CastToFloat32(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to float32", v))
		}
		b.Append(vT)

	case *array.Int16Builder:
		vT, ok := types.CastToInt16(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to int16", v))
		}
		b.Append(vT)

	case *array.Uint16Builder:
		vT, ok := types.CastToUint16(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to uint16", v))
		}
		b.Append(vT)

	case *array.Int8Builder:
		vT, ok := types.CastToInt8(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to int8", v))
		}
		b.Append(vT)

	case *array.Uint8Builder:
		vT, ok := types.CastToUint8(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to uint8", v))
		}
		b.Append(vT)

	case *array.TimestampBuilder:
		vT, ok := types.CastToTimestamp(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.Timestamp", v))
		}
		b.Append(vT)

	case *array.Time32Builder:
		vT, ok := types.CastToTime32(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.Time32", v))
		}
		b.Append(vT)

	case *array.Time64Builder:
		vT, ok := types.CastToTime64(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.Time64", v))
		}
		b.Append(vT)

	case *array.Date32Builder:
		vT, ok := types.CastToDate32(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.Date32", v))
		}
		b.Append(vT)

	case *array.Date64Builder:
		vT, ok := types.CastToDate64(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.Date64", v))
		}
		b.Append(vT)

	case *array.DurationBuilder:
		vT, ok := types.CastToDuration(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.Duration", v))
		}
		b.Append(vT)

	case *array.MonthIntervalBuilder:
		vT, ok := types.CastToMonthInterval(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.MonthInterval", v))
		}
		b.Append(vT)

	case *array.Float16Builder:
		vT, ok := types.CastToFloat16(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to float16.Num", v))
		}
		b.Append(vT)

	case *array.Decimal128Builder:
		vT, ok := types.CastToDecimal128(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to decimal128.Num", v))
		}
		b.Append(vT)

	case *array.DayTimeIntervalBuilder:
		vT, ok := types.CastToDayTimeInterval(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to arrow.DayTimeInterval", v))
		}
		b.Append(vT)

	case *array.BooleanBuilder:
		vT, ok := types.CastToBoolean(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to bool", v))
		}
		b.Append(vT)

	case *array.StringBuilder:
		vT, ok := types.CastToString(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to string", v))
		}
		b.Append(vT)

	case *array.NullBuilder:
		vT, ok := types.CastToNull(v)
		if !ok {
			panic(fmt.Sprintf("cannot cast %T to interface{}", v))
		}
		b.Append(vT)

	case *array.ListBuilder:
		b.Append(true)
		sub := b.ValueBuilder()
		v := reflect.ValueOf(v)
		for i := 0; i < v.Len(); i++ {
			sb.appendValue(sub, v.Index(i).Interface())
		}

	case *array.FixedSizeListBuilder:
		b.Append(true)
		sub := b.ValueBuilder()
		v := reflect.ValueOf(v)
		for i := 0; i < v.Len(); i++ {
			sb.appendValue(sub, v.Index(i).Interface())
		}

	case *array.StructBuilder:
		b.Append(true)
		v := reflect.ValueOf(v)
		for i := 0; i < b.NumField(); i++ {
			f := b.FieldBuilder(i)
			sb.appendValue(f, v.Field(i).Interface())
		}

	default:
		panic(errors.Errorf("dataframe/smartbuilder: unhandled Arrow builder type %T", b))
	}
}
