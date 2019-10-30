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
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-bullseye/bullseye/dataframe/metadata"
	"github.com/go-bullseye/bullseye/internal/testdata"
)

func TestNewSmartBuilder(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: COL0NAME, Type: arrow.PrimitiveTypes.Int32},
			{Name: COL1NAME, Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	smartBuilder := NewSmartBuilder(b, schema)

	int32Vals := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, v := range int32Vals {
		smartBuilder.Append(0, v)
	}
	smartBuilder.Append(0, nil)

	float64Vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	for _, v := range float64Vals {
		smartBuilder.Append(1, v)
	}
	smartBuilder.Append(1, nil)

	rec1 := b.NewRecord()
	defer rec1.Release()

	cols := make([]array.Column, 0, len(rec1.Columns()))
	for i, cI := range rec1.Columns() {
		field := rec1.Schema().Field(i)
		chunk := array.NewChunked(field.Type, []array.Interface{cI})
		col := array.NewColumn(field, chunk)
		defer col.Release()
		cols = append(cols, *col)
		chunk.Release()
	}

	df, err := NewDataFrameFromColumns(pool, cols)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["f1-i32"]: [1 2 3 4 5 6 7 8 9 (null)]
rec[0]["f2-f64"]: [1 2 3 4 5 6 7 8 9 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func buildDf(pool *memory.CheckedAllocator, dtype arrow.DataType, vals []interface{}) (*DataFrame, error) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: fmt.Sprintf("col-%s", dtype.Name()), Type: dtype},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()

	smartBuilder := NewSmartBuilder(b, schema)
	for i := range schema.Fields() {
		for j := range vals {
			smartBuilder.Append(i, vals[j])
		}
		smartBuilder.Append(i, nil)
	}

	rec1 := b.NewRecord()
	defer rec1.Release()

	cols := make([]array.Column, 0, len(rec1.Columns()))
	for i, cI := range rec1.Columns() {
		field := rec1.Schema().Field(i)
		chunk := array.NewChunked(field.Type, []array.Interface{cI})
		col := array.NewColumn(field, chunk)
		defer col.Release()
		cols = append(cols, *col)
		chunk.Release()
	}

	return NewDataFrameFromColumns(pool, cols)
}

func TestNewSmartBuilderBoolean(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = (i%2 == 0)
	}
	df, err := buildDf(pool, arrow.FixedWidthTypes.Boolean, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-bool"]: [true false true false true false true false true (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt8(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int8(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int8, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int8"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt16(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int16(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int16, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int16"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt32(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int32(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int32, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int32"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderInt64(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = int64(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Int64, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-int64"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderUint8(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint8(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint8, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint8"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}
func TestNewSmartBuilderUint16(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint16(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint16, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint16"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderUint32(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint32(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint32, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint32"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderUint64(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = uint64(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Uint64, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-uint64"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderFloat32(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = float32(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Float32, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-float32"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderFloat64(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = float64(i)
	}
	df, err := buildDf(pool, arrow.PrimitiveTypes.Float64, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-float64"]: [0 1 2 3 4 5 6 7 8 (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

func TestNewSmartBuilderString(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	vals := make([]interface{}, 9)
	for i := range vals {
		vals[i] = fmt.Sprintf("%d", i)
	}
	df, err := buildDf(pool, arrow.BinaryTypes.String, vals)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	got := df.Display(-1)
	want := `rec[0]["col-utf8"]: ["0" "1" "2" "3" "4" "5" "6" "7" "8" (null)]
`

	if got != want {
		t.Fatalf("\ngot=\n%v\nwant=\n%v", got, want)
	}
}

type keyValue struct {
	Key   interface{}
	Value interface{}
}

// convert {"foo": "bar", "ping": 0} to [{"Key": "foo", "Value": "bar"}, {"Key": "ping", "Value": 0}]
func convertMapToKeyValueTuples(m reflect.Value) reflect.Value {
	length := m.Len()
	list := make([]keyValue, 0, length)
	iter := m.MapRange()
	for iter.Next() {
		k := iter.Key()   // foo
		v := iter.Value() // bar
		list = append(list, keyValue{Key: k.Interface(), Value: v.Interface()})
	}
	return reflect.ValueOf(list)
}

func addMapAsListOfStructsUsingSmartBuilder(fi int, t *testing.T, recordBuilder *array.RecordBuilder, valids []bool) {
	t.Helper()

	data := []map[string]float64{
		{"field_a": float64(0), "field_b": float64(0), "field_c": float64(0)},
		nil,
		{"field_a": float64(2), "field_b": float64(2), "field_c": float64(2)},
		{"field_a": float64(3), "field_b": float64(3), "field_c": float64(3)},
		{"field_a": float64(4), "field_b": float64(4), "field_c": float64(4)},
	}

	smartBuilder := NewSmartBuilder(recordBuilder, recordBuilder.Schema())
	for i, d := range data {
		if d == nil || !valids[i] {
			smartBuilder.Append(fi, nil)
			continue
		}
		o := reflect.ValueOf(d)
		v := convertMapToKeyValueTuples(o)
		kv := v.Interface()
		smartBuilder.Append(fi, kv)
	}
}

func TestSmartBuilderMaps(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			{
				Name: "col15-los",
				Type: arrow.ListOf(arrow.StructOf([]arrow.Field{
					{Name: "field_a", Type: arrow.BinaryTypes.String},
					{Name: "field_b", Type: arrow.BinaryTypes.String},
					{Name: "field_c", Type: arrow.PrimitiveTypes.Float64},
				}...)),
			},
			{
				Name: "col16-los-sb",
				Type: arrow.ListOf(arrow.StructOf([]arrow.Field{
					{Name: "Key", Type: arrow.BinaryTypes.String},
					{Name: "Value", Type: arrow.PrimitiveTypes.Float64},
				}...)),
				// map[string]float64
				// Add some metadata to let consumers of this know that we really want a map logical type.
				Metadata: metadata.AppendOriginalMapTypeMetadata(arrow.Metadata{}),
			},
		},
		nil,
	)

	recordBuilder := array.NewRecordBuilder(pool, schema)
	defer recordBuilder.Release()

	valids := []bool{true, true, true, false, true}

	// list of struct
	addListOfStructs(0, t, recordBuilder, valids)

	// list of struct using smart builder
	addMapAsListOfStructsUsingSmartBuilder(1, t, recordBuilder, valids)

	rec1 := recordBuilder.NewRecord()
	defer rec1.Release()

	df, err := NewDataFrameFromRecord(pool, rec1)
	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	var b bytes.Buffer
	err = df.ToJSON(&b)
	if err != nil {
		t.Fatal(err)
	}

	toJSONSmartBuilderResult := `{"col15-los":[{"field_a":"r0:s0:e0","field_b":"r0:s0:e0","field_c":0},{"field_a":"r0:s1:e1","field_b":"r0:s1:e1","field_c":1}],"col16-los-sb":{"field_a":0,"field_b":0,"field_c":0}}
{"col15-los":[{"field_a":"r1:s0:e2","field_b":"r1:s0:e2","field_c":0},{"field_a":"r1:s1:e3","field_b":"r1:s1:e3","field_c":1}],"col16-los-sb":null}
{"col15-los":[{"field_a":"r2:s0:e4","field_b":"r2:s0:e4","field_c":0},{"field_a":"r2:s1:e5","field_b":"r2:s1:e5","field_c":1}],"col16-los-sb":{"field_a":2,"field_b":2,"field_c":2}}
{"col15-los":[{"field_a":"r3:s0:e6","field_b":"r3:s0:e6","field_c":0},{"field_a":"r3:s1:e7","field_b":"r3:s1:e7","field_c":1}],"col16-los-sb":null}
{"col15-los":[{"field_a":"r4:s0:e8","field_b":"r4:s0:e8","field_c":0},{"field_a":"r4:s1:e9","field_b":"r4:s1:e9","field_c":1}],"col16-los-sb":{"field_a":4,"field_b":4,"field_c":4}}
`

	if got, want := b.String(), toJSONSmartBuilderResult; got != want {
		t.Fatalf("\ngot=\n%s\nwant=\n%s\n", got, want)
	}
}

func TestNewSmartBuilderTypes(t *testing.T) {
	for _, testCase := range testdata.GenerateSmartBuilderTestCases() {
		func() {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			defer pool.AssertSize(t, 0)

			df, err := buildDf(pool, testCase.Dtype, testCase.Values)
			if err != nil {
				t.Fatal(err)
			}
			defer df.Release()

			got := df.Display(-1)

			if strings.TrimSpace(got) != testCase.Want {
				t.Fatalf("\ngot=\n%v\nwant=\n%v", got, testCase.Want)
			}
		}()
	}
}
