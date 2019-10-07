package dataframe

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/float16"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/go-bullseye/bullseye/iterator"
)

const (
	toJSONResult = `{"col0-i32":1,"col1-f64":1,"col10-bool":true,"col11-string":"a","col12-list":["0:0","0:1","0:2","0:3","0:4"],"col13-struct":{"field1":"f0:0","field2":"f1:0","field3":0},"col14-list":[["0:0:0","0:0:1","0:0:2","0:0:3","0:0:4"],["0:1:0","0:1:1","0:1:2","0:1:3","0:1:4"],["0:2:0","0:2:1","0:2:2","0:2:3","0:2:4"],null,["0:4:0","0:4:1","0:4:2","0:4:3","0:4:4"]],"col2-f16":1,"col3-date32":1,"col4-date64":1,"col5-mitvl":1,"col6-dtitvl":{"days":1,"milliseconds":1},"col7-dec128":{"lo":1,"hi":1},"col8-duration-s":1,"col9-ts-s":1}
{"col0-i32":2,"col1-f64":2,"col10-bool":false,"col11-string":"b","col12-list":["1:0","1:1","1:2","1:3","1:4"],"col13-struct":{"field1":"f0:1","field2":"f1:1","field3":1},"col14-list":[["1:0:0","1:0:1","1:0:2","1:0:3","1:0:4"],["1:1:0","1:1:1","1:1:2","1:1:3","1:1:4"],["1:2:0","1:2:1","1:2:2","1:2:3","1:2:4"],null,["1:4:0","1:4:1","1:4:2","1:4:3","1:4:4"]],"col2-f16":2,"col3-date32":2,"col4-date64":2,"col5-mitvl":2,"col6-dtitvl":{"days":2,"milliseconds":2},"col7-dec128":{"lo":2,"hi":2},"col8-duration-s":2,"col9-ts-s":2}
{"col0-i32":3,"col1-f64":3,"col10-bool":true,"col11-string":"c","col12-list":["2:0","2:1","2:2","2:3","2:4"],"col13-struct":{"field1":"f0:2","field2":"f1:2","field3":2},"col14-list":[["2:0:0","2:0:1","2:0:2","2:0:3","2:0:4"],["2:1:0","2:1:1","2:1:2","2:1:3","2:1:4"],["2:2:0","2:2:1","2:2:2","2:2:3","2:2:4"],null,["2:4:0","2:4:1","2:4:2","2:4:3","2:4:4"]],"col2-f16":3,"col3-date32":3,"col4-date64":3,"col5-mitvl":3,"col6-dtitvl":{"days":3,"milliseconds":3},"col7-dec128":{"lo":3,"hi":3},"col8-duration-s":3,"col9-ts-s":3}
{"col0-i32":null,"col1-f64":null,"col10-bool":null,"col11-string":null,"col12-list":null,"col13-struct":null,"col14-list":null,"col2-f16":null,"col3-date32":null,"col4-date64":null,"col5-mitvl":null,"col6-dtitvl":null,"col7-dec128":null,"col8-duration-s":null,"col9-ts-s":null}
{"col0-i32":5,"col1-f64":5,"col10-bool":true,"col11-string":"e","col12-list":["4:0","4:1","4:2","4:3","4:4"],"col13-struct":{"field1":"f0:4","field2":"f1:4","field3":4},"col14-list":[["4:0:0","4:0:1","4:0:2","4:0:3","4:0:4"],["4:1:0","4:1:1","4:1:2","4:1:3","4:1:4"],["4:2:0","4:2:1","4:2:2","4:2:3","4:2:4"],null,["4:4:0","4:4:1","4:4:2","4:4:3","4:4:4"]],"col2-f16":5,"col3-date32":5,"col4-date64":5,"col5-mitvl":5,"col6-dtitvl":{"days":5,"milliseconds":5},"col7-dec128":{"lo":18446744073709551611,"hi":-1},"col8-duration-s":5,"col9-ts-s":5}
`
)

func f16sFrom(vs []float64) []float16.Num {
	o := make([]float16.Num, len(vs))
	for i, v := range vs {
		o[i] = float16.New(float32(v))
	}
	return o
}

func TestToJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	schema := arrow.NewSchema(
		[]arrow.Field{
			// {Name: "col0-i32", Type: arrow.PrimitiveTypes.Int32},
			// {Name: "col1-f64", Type: arrow.PrimitiveTypes.Float64},
			// {Name: "col2-f16", Type: arrow.FixedWidthTypes.Float16},
			// {Name: "col3-date32", Type: arrow.PrimitiveTypes.Date32},
			// {Name: "col4-date64", Type: arrow.PrimitiveTypes.Date64},
			// {Name: "col5-mitvl", Type: arrow.FixedWidthTypes.MonthInterval},
			// {Name: "col6-dtitvl", Type: arrow.FixedWidthTypes.DayTimeInterval},
			// {Name: "col7-dec128", Type: &arrow.Decimal128Type{Precision: 10, Scale: 1}},
			// {Name: "col8-duration-s", Type: arrow.FixedWidthTypes.Duration_s},
			// {Name: "col9-ts-s", Type: arrow.FixedWidthTypes.Timestamp_s},
			// {Name: "col10-bool", Type: arrow.FixedWidthTypes.Boolean},
			// {Name: "col11-string", Type: arrow.BinaryTypes.String},
			// {Name: "col12-list", Type: arrow.ListOf(arrow.BinaryTypes.String)},
			{Name: "col13-struct", Type: arrow.StructOf([]arrow.Field{
				{Name: "field1", Type: arrow.BinaryTypes.String},
				{Name: "field2", Type: arrow.BinaryTypes.String},
				// {Name: "field3", Type: arrow.PrimitiveTypes.Float64},
			}...)},
			{Name: "col14-list", Type: arrow.ListOf(arrow.ListOf(arrow.BinaryTypes.String))},
			{Name: "col15-los", Type: arrow.ListOf(arrow.StructOf([]arrow.Field{
				{Name: "field_a", Type: arrow.BinaryTypes.String},
				{Name: "field_b", Type: arrow.BinaryTypes.String},
				// {Name: "field_c", Type: arrow.PrimitiveTypes.Float64},
			}...))},
		},
		nil,
	)

	// print the schema
	fmt.Println(schema.String())

	recordBuilder := array.NewRecordBuilder(pool, schema)
	defer recordBuilder.Release()

	valids := []bool{true, true, true, false, true}
	f := 0
	// float16Values := f16sFrom([]float64{1, 2, 3, 4, 5})
	// dayTimeIntervalValues := []arrow.DayTimeInterval{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}}
	// decimal128Values := []decimal128.Num{decimal128.New(1, 1), decimal128.New(2, 2), decimal128.New(3, 3), {}, decimal128.FromI64(-5)}
	// recordBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3, 4, 5}, valids)
	// recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5}, valids)
	// recordBuilder.Field(2).(*array.Float16Builder).AppendValues(float16Values, valids)
	// recordBuilder.Field(3).(*array.Date32Builder).AppendValues([]arrow.Date32{1, 2, 3, 4, 5}, valids)
	// recordBuilder.Field(4).(*array.Date64Builder).AppendValues([]arrow.Date64{1, 2, 3, 4, 5}, valids)
	// recordBuilder.Field(5).(*array.MonthIntervalBuilder).AppendValues([]arrow.MonthInterval{1, 2, 3, 4, 5}, valids)
	// recordBuilder.Field(6).(*array.DayTimeIntervalBuilder).AppendValues(dayTimeIntervalValues, valids)
	// recordBuilder.Field(7).(*array.Decimal128Builder).AppendValues(decimal128Values, valids)
	// recordBuilder.Field(8).(*array.DurationBuilder).AppendValues([]arrow.Duration{1, 2, 3, 4, 5}, valids)
	// recordBuilder.Field(9).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3, 4, 5}, valids)
	// recordBuilder.Field(10).(*array.BooleanBuilder).AppendValues([]bool{true, false, true, false, true}, valids)
	// recordBuilder.Field(11).(*array.StringBuilder).AppendValues([]string{"a", "b", "c", "d", "e"}, valids)

	// list
	// addList(t, recordBuilder, valids)

	// struct
	addStruct(f, t, recordBuilder, valids)
	f++

	// list of list
	addListOfLists(f, t, recordBuilder, valids)
	f++

	// list of struct
	addListOfStructs(f, t, recordBuilder, valids)
	f++

	rec1 := recordBuilder.NewRecord()
	defer rec1.Release()

	df, err := NewDataFrameFromRecord(pool, rec1)

	fmt.Println(df.Display(0))

	if err != nil {
		t.Fatal(err)
	}
	defer df.Release()

	var b bytes.Buffer
	err = df.ToJSON(&b)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := b.String(), toJSONResult; got != want {
		t.Fatalf("\ngot=\n%s\nwant=\n%s\n", got, want)
	}
}

func addList(fi int, t *testing.T, recordBuilder *array.RecordBuilder, valids []bool) {
	t.Helper()
	lb := recordBuilder.Field(fi).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.StringBuilder)
	for i, v := range valids {
		lb.Append(v)
		for j := range valids {
			vb.Append(fmt.Sprintf("%d:%d", i, j))
		}
	}
}

func addStruct(fi int, t *testing.T, recordBuilder *array.RecordBuilder, valids []bool) {
	t.Helper()
	sb := recordBuilder.Field(fi).(*array.StructBuilder)
	fb0 := sb.FieldBuilder(0).(*array.StringBuilder)
	fb1 := sb.FieldBuilder(1).(*array.StringBuilder)
	// fb2 := sb.FieldBuilder(2).(*array.Float64Builder)
	for i, v := range valids {
		sb.Append(v)
		if v {
			fb0.Append(fmt.Sprintf("f0:%d", i))
			fb1.Append(fmt.Sprintf("f1:%d", i))
			// fb2.Append(float64(i))
		}
	}
}

func addListOfLists(fi int, t *testing.T, recordBuilder *array.RecordBuilder, valids []bool) {
	t.Helper()
	lb := recordBuilder.Field(fi).(*array.ListBuilder)
	llb := lb.ValueBuilder().(*array.ListBuilder)
	vb := llb.ValueBuilder().(*array.StringBuilder)
	for i, v := range valids {
		lb.Append(v) // 1 list
		for j, vv := range valids {
			llb.Append(vv) // 5 sub lists
			for k := range valids {
				vb.Append(fmt.Sprintf("%d:%d:%d", i, j, k)) // 5 string elements per list
			}
		}
	}
}

func addListOfStructs(fi int, t *testing.T, recordBuilder *array.RecordBuilder, valids []bool) {
	t.Helper()
	lb := recordBuilder.Field(fi).(*array.ListBuilder)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	fb0 := sb.FieldBuilder(0).(*array.StringBuilder)
	fb1 := sb.FieldBuilder(1).(*array.StringBuilder)
	// fb2 := sb.FieldBuilder(2).(*array.Float64Builder)

	element := 0

	for i, _ := range valids { // add 5 lists
		// lb.Append(v) // 1 list per row
		lb.Append(true)

		// Inside each list, add 2 structs
		for j := 0; j < 2; j++ {
			sb.Append(true)
			if true {
				fb0.Append(fmt.Sprintf("r%d:s%d:e%d", i, j, element)) // 1 field per struct
				// fb0.Append(fmt.Sprintf("e%d:f0:%d", i, j))
				fb1.Append(fmt.Sprintf("r%d:s%d:e%d", i, j, element))
				// fb2.Append(float64(j))
				element++
			}
		}
	}
}

type releaseable interface {
	Release()
}

func Test_rowElementToJSON(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	collect := make([]releaseable, 0)

	type testCase struct {
		name   string
		dtype  arrow.DataType
		value  interface{}
		result string
		err    error
	}
	testCases := []testCase{
		// testCase{
		// 	name:   "string test",
		// 	dtype:  arrow.BinaryTypes.String,
		// 	value:  "foo bar",
		// 	result: `"foo bar"`,
		// 	err:    nil,
		// },
		// testCase{
		// 	name:   "int32 test",
		// 	dtype:  arrow.PrimitiveTypes.Int32,
		// 	value:  int32(123),
		// 	result: `123`,
		// 	err:    nil,
		// },
		// testCase{
		// 	name:   "listOf(int32) test",
		// 	dtype:  arrow.ListOf(arrow.BinaryTypes.String),
		// 	value:  generateListOfStrings(pool, &collect),
		// 	result: `[["a","b","c"],["d","e","f"]]`,
		// 	err:    nil,
		// },
		testCase{
			name:  "struct test",
			dtype: arrow.ListOf(arrow.BinaryTypes.String), // TODO: For some reason, this one works just fine...
			// dtype: arrow.StructOf([]arrow.Field{
			// 	{Name: "field1", Type: arrow.BinaryTypes.String},
			// 	{Name: "field2", Type: arrow.BinaryTypes.String},
			// }...),
			value:  generateStructOfStrings(pool, &collect),
			result: `[{"field1":"s0:f0","field2":"s0:f1"},{"field1":"s1:f0","field2":"s1:f1"}]`,
			err:    nil,
		},
	}

	defer func() {
		for i := range collect {
			collect[i].Release()
		}
	}()

	for _, tc := range testCases {
		res, err := rowElementToJSON(tc.dtype, tc.value)
		if err != tc.err {
			t.Errorf("got err=%v, want err=%v for test: %s\n", err, tc.err, tc.name)
		}
		// marshal the result
		resultBytes, err := json.Marshal(res)
		if err != tc.err {
			t.Errorf("got error marshaling json for test: %s\n%v", tc.name, err)
		}
		result := string(resultBytes)

		if result != tc.result {
			t.Errorf("got result=%s, want result=%s for test: %s | %T - %T\n", result, tc.result, tc.name, result, tc.result)
		}
	}
}

func generateListOfStrings(pool memory.Allocator, releaseables *[]releaseable) interface{} {
	lb := array.NewListBuilder(pool, arrow.BinaryTypes.String)
	defer lb.Release()
	vb := lb.ValueBuilder().(*array.StringBuilder)
	lb.Append(true)
	vb.Append("a")
	vb.Append("b")
	vb.Append("c")
	lb.Append(true)
	vb.Append("d")
	vb.Append("e")
	vb.Append("f")
	iface := lb.NewArray()
	*releaseables = append(*releaseables, iface)
	return iterator.NewInterfaceValueIterator(arrow.Field{"item", arrow.ListOf(arrow.BinaryTypes.String), true, arrow.Metadata{}}, iface)
}

func generateStructOfStrings(pool memory.Allocator, releaseables *[]releaseable) interface{} {
	dtype := arrow.StructOf([]arrow.Field{
		{Name: "field1", Type: arrow.BinaryTypes.String},
		{Name: "field2", Type: arrow.BinaryTypes.String},
	}...)

	sb := array.NewStructBuilder(pool, dtype)
	defer sb.Release()
	fb0 := sb.FieldBuilder(0).(*array.StringBuilder)
	fb1 := sb.FieldBuilder(1).(*array.StringBuilder)

	sb.Append(true)
	fb0.Append("s0:f0")
	fb1.Append("s0:f1")

	sb.Append(true)
	fb0.Append("s1:f0")
	fb1.Append("s1:f1")

	iface := sb.NewArray()
	*releaseables = append(*releaseables, iface)

	return iterator.NewInterfaceValueIterator(arrow.Field{"struct", dtype, true, arrow.Metadata{}}, iface)
}
