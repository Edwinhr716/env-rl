// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// scalarSchemaType is the JSON Schema type a Go scalar of kind k is written
// as, or "" for a kind that is not a scalar.
func scalarSchemaType(k reflect.Kind) string {
	switch k {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	}
	return ""
}

// jsonFields lists the fields of struct type t under the names encoding/json
// writes them with: the name of the json tag, or the Go field name when the
// tag has none (no tag, or `json:",omitempty"`). An unexported field and a
// field tagged `json:"-"` are not written. The fields of an embedded struct
// without a tag name are written as fields of t; a field of t itself wins
// over an embedded field of the same name.
func jsonFields(t reflect.Type) map[string]reflect.Type {
	fields := map[string]reflect.Type{}
	var direct []reflect.StructField
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name := strings.Split(tag, ",")[0]
		if f.Anonymous && name == "" {
			et := f.Type
			if et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			if et.Kind() == reflect.Struct {
				for n, ft := range jsonFields(et) {
					fields[n] = ft
				}
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		direct = append(direct, f)
	}
	for _, f := range direct {
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		if name == "" {
			name = f.Name
		}
		fields[name] = f.Type
	}
	return fields
}

// TestJSONFieldsFollowsEncodingJSON holds jsonFields to what encoding/json
// does with each shape of field: a strict decode accepts each name it
// lists, and refuses the names of the fields it leaves out.
func TestJSONFieldsFollowsEncodingJSON(t *testing.T) {
	type Inner struct {
		Deep   string `json:"deep,omitempty"`
		Shared string `json:"shared,omitempty"`
	}
	type unexportedInner struct {
		Lifted int `json:"lifted,omitempty"`
	}
	type Named struct {
		X int `json:"x"`
	}
	cases := []struct {
		name string
		typ  reflect.Type
		want []string
		not  []string
	}{
		{"tagged", reflect.TypeOf(struct {
			A string `json:"a,omitempty"`
		}{}), []string{"a"}, []string{"A2"}},
		{"untagged", reflect.TypeOf(struct{ Foo string }{}), []string{"Foo"}, nil},
		{"options without a name", reflect.TypeOf(struct {
			Foo string `json:",omitempty"`
		}{}), []string{"Foo"}, nil},
		{"dash", reflect.TypeOf(struct {
			A      string `json:"a"`
			Hidden string `json:"-"`
		}{}), []string{"a"}, []string{"Hidden", "-"}},
		{"dash with a comma is the name -", reflect.TypeOf(struct {
			Dash string `json:"-,"`
		}{}), []string{"-"}, []string{"Dash"}},
		{"unexported", reflect.TypeOf(struct {
			A      string `json:"a"`
			hidden string
		}{}), []string{"a"}, []string{"hidden"}},
		{"embedded struct", reflect.TypeOf(struct {
			Inner
			A string `json:"a"`
		}{}), []string{"a", "deep", "shared"}, []string{"Inner"}},
		{"embedded pointer", reflect.TypeOf(struct {
			*Inner
		}{}), []string{"deep", "shared"}, []string{"Inner"}},
		{"embedded unexported struct type", reflect.TypeOf(struct {
			unexportedInner
		}{}), []string{"lifted"}, []string{"unexportedInner"}},
		{"embedded with a tag name is one field", reflect.TypeOf(struct {
			Named `json:"named"`
		}{}), []string{"named"}, []string{"x"}},
		{"embedded with dash", reflect.TypeOf(struct {
			Inner `json:"-"`
			A     string `json:"a"`
		}{}), []string{"a"}, []string{"deep", "Inner"}},
		{"outer field wins over embedded", reflect.TypeOf(struct {
			Inner
			Shared int `json:"shared"`
		}{}), []string{"deep", "shared"}, nil},
	}
	strictDecode := func(typ reflect.Type, name string) error {
		key, err := json.Marshal(name)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(strings.NewReader("{" + string(key) + ": null}"))
		dec.DisallowUnknownFields()
		return dec.Decode(reflect.New(typ).Interface())
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields := jsonFields(tc.typ)
			var got []string
			for name := range fields {
				got = append(got, name)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("jsonFields = %v, want %v", got, tc.want)
			}
			for _, name := range tc.want {
				if err := strictDecode(tc.typ, name); err != nil {
					t.Errorf("encoding/json does not know %q: %v", name, err)
				}
			}
			for _, name := range tc.not {
				if err := strictDecode(tc.typ, name); err == nil {
					t.Errorf("encoding/json knows %q, the case says it does not", name)
				}
			}
		})
	}
	if got := jsonFields(reflect.TypeOf(struct {
		Inner
		Shared int `json:"shared"`
	}{}))["shared"]; got == nil || got.Kind() != reflect.Int {
		t.Errorf("shared resolves to %v, want the outer int field", got)
	}
}

// schemaRefTarget is the $defs name a value schema refers to: a plain
// `$ref`, or an `allOf` with exactly one `$ref` among its subschemas (the
// form that adds a rule to a shared definition). ok is false when the
// schema refers to no definition.
func schemaRefTarget(p map[string]any) (name string, ok bool) {
	const prefix = "#/$defs/"
	var refs []string
	if r, isStr := p["$ref"].(string); isStr {
		refs = append(refs, r)
	}
	if all, isList := p["allOf"].([]any); isList {
		for _, sub := range all {
			if m, isMap := sub.(map[string]any); isMap {
				if r, isStr := m["$ref"].(string); isStr {
					refs = append(refs, r)
				}
			}
		}
	}
	if len(refs) != 1 || !strings.HasPrefix(refs[0], prefix) {
		return "", false
	}
	return strings.TrimPrefix(refs[0], prefix), true
}

// schemaDrift compares a JSON Schema document with the Go type its root
// describes and returns one line per difference. It walks the structs
// reachable from root: each JSON field is a property of the matching
// definition and the definition names no other property; a struct field
// refers to `#/$defs/<TypeName>`; a scalar has its JSON type; a slice is
// an array whose `items` match the element type; a map is an object whose
// `additionalProperties` match the value type (a map of `any` needs none).
// A definition no Go type reaches is a difference too.
func schemaDrift(doc map[string]any, root reflect.Type) []string {
	var drift []string
	errf := func(format string, args ...any) {
		drift = append(drift, fmt.Sprintf(format, args...))
	}
	defs, _ := doc["$defs"].(map[string]any)
	visited := map[string]bool{}
	var checkStruct func(t reflect.Type, def map[string]any, where string)
	var checkValue func(t reflect.Type, p map[string]any, where string)

	checkValue = func(t reflect.Type, p map[string]any, where string) {
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		ref, isRef := schemaRefTarget(p)
		switch t.Kind() {
		case reflect.Interface:
			return
		case reflect.Struct:
			if !isRef || ref != t.Name() {
				errf("%s: the Go field is the struct %s, so the schema must refer to #/$defs/%s; it has %v", where, t.Name(), t.Name(), p)
				return
			}
			if visited[ref] {
				return
			}
			visited[ref] = true
			def, ok := defs[ref].(map[string]any)
			if !ok {
				errf("%s: the schema has no $defs/%s for %s", where, ref, t.Name())
				return
			}
			checkStruct(t, def, "$defs/"+ref)
		case reflect.Slice, reflect.Array:
			if isRef || p["type"] != "array" {
				errf("%s: the Go field is %s, so the schema must have type array; it has %v", where, t, p)
				return
			}
			items, ok := p["items"].(map[string]any)
			if !ok {
				errf("%s: the array has no items schema for %s", where, t.Elem())
				return
			}
			checkValue(t.Elem(), items, where+"[]")
		case reflect.Map:
			if isRef || p["type"] != "object" {
				errf("%s: the Go field is %s, so the schema must have type object; it has %v", where, t, p)
				return
			}
			if t.Key().Kind() != reflect.String {
				errf("%s: the Go map key is %s, a JSON object needs a string key", where, t.Key())
			}
			if t.Elem().Kind() == reflect.Interface {
				return
			}
			values, ok := p["additionalProperties"].(map[string]any)
			if !ok {
				errf("%s: the object has no additionalProperties schema for %s", where, t.Elem())
				return
			}
			checkValue(t.Elem(), values, where+"{}")
		default:
			want := scalarSchemaType(t.Kind())
			if want == "" {
				errf("%s: the Go kind %s has no JSON Schema type in this test", where, t.Kind())
				return
			}
			if isRef || p["type"] != want {
				errf("%s: the Go field is %s, so the schema must have type %s; it has %v", where, t, want, p)
			}
		}
	}

	checkStruct = func(t reflect.Type, def map[string]any, where string) {
		props, _ := def["properties"].(map[string]any)
		fields := jsonFields(t)
		var missing, extra []string
		for name, ft := range fields {
			p, ok := props[name].(map[string]any)
			if !ok {
				missing = append(missing, name)
				continue
			}
			checkValue(ft, p, where+"."+name)
		}
		for name := range props {
			if _, ok := fields[name]; !ok {
				extra = append(extra, name)
			}
		}
		sort.Strings(missing)
		sort.Strings(extra)
		if len(missing) > 0 {
			errf("%s lacks the fields of %s: %v", where, t.Name(), missing)
		}
		if len(extra) > 0 {
			errf("%s has properties %s lacks: %v", where, t.Name(), extra)
		}
	}

	checkStruct(root, doc, "the root")
	for name := range defs {
		if !visited[name] {
			errf("$defs/%s describes no Go type that %s reaches", name, root.Name())
		}
	}
	sort.Strings(drift)
	return drift
}

// TestScenarioSchemaMatchesTheGoTypes keeps scenario.schema.json in step
// with schema.Scenario (see schemaDrift for what "in step" covers). A field
// added to a scenario type, or a changed field type, fails here until the
// schema follows.
func TestScenarioSchemaMatchesTheGoTypes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("v1", "scenario.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, d := range schemaDrift(doc, reflect.TypeOf(Scenario{})) {
		t.Error(d)
	}
}

// Types for TestSchemaDriftFindsEachKindOfDrift.
type (
	driftLeaf struct {
		Max int `json:"max,omitempty"`
	}
	driftOther struct {
		Max int `json:"max,omitempty"`
	}
	driftRoot struct {
		Name   string               `json:"name"`
		Leaf   *driftLeaf           `json:"leaf,omitempty"`
		Leaves []driftLeaf          `json:"leaves,omitempty"`
		ByName map[string]driftLeaf `json:"by_name,omitempty"`
		Args   []string             `json:"args,omitempty"`
		Env    map[string]string    `json:"env,omitempty"`
		Free   map[string]any       `json:"free,omitempty"`
	}
)

// TestSchemaDriftFindsEachKindOfDrift gives schemaDrift a schema that is in
// step with driftRoot, then one change at a time to the schema or to the Go
// type, and expects the difference to be found at the changed place.
func TestSchemaDriftFindsEachKindOfDrift(t *testing.T) {
	const inStep = `{
	  "type": "object",
	  "properties": {
	    "name": {"type": "string"},
	    "leaf": {"$ref": "#/$defs/driftLeaf"},
	    "leaves": {"type": "array", "items": {"$ref": "#/$defs/driftLeaf"}},
	    "by_name": {"type": "object", "additionalProperties": {"$ref": "#/$defs/driftLeaf"}},
	    "args": {"type": "array", "items": {"type": "string"}},
	    "env": {"type": "object", "additionalProperties": {"type": "string"}},
	    "free": {"type": "object"}
	  },
	  "$defs": {
	    "driftLeaf": {"type": "object", "properties": {"max": {"type": "integer"}}},
	    "driftOther": {"type": "object", "properties": {"max": {"type": "integer"}}}
	  }
	}`
	// driftOther is in $defs for the wrong-$ref cases; a root that reaches
	// it keeps the base document free of differences.
	type root struct {
		driftRoot
		Other *driftOther `json:"other,omitempty"`
	}
	rootType := reflect.TypeOf(root{})
	parse := func(t *testing.T, edit func(props, defs map[string]any)) map[string]any {
		t.Helper()
		var doc map[string]any
		if err := json.Unmarshal([]byte(inStep), &doc); err != nil {
			t.Fatal(err)
		}
		props := doc["properties"].(map[string]any)
		props["other"] = map[string]any{"$ref": "#/$defs/driftOther"}
		if edit != nil {
			edit(props, doc["$defs"].(map[string]any))
		}
		return doc
	}
	if d := schemaDrift(parse(t, nil), rootType); len(d) != 0 {
		t.Fatalf("the base schema is in step, schemaDrift found: %v", d)
	}

	obj := func(kv ...any) map[string]any {
		m := map[string]any{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i].(string)] = kv[i+1]
		}
		return m
	}
	schemaCases := []struct {
		name string
		edit func(props, defs map[string]any)
		want string // a substring of one difference
	}{
		{"wrong $ref on a struct field", func(p, _ map[string]any) {
			p["leaf"] = obj("$ref", "#/$defs/driftOther")
		}, "the root.leaf: the Go field is the struct driftLeaf"},
		{"wrong $ref in items", func(p, _ map[string]any) {
			p["leaves"] = obj("type", "array", "items", obj("$ref", "#/$defs/driftOther"))
		}, "the root.leaves[]: the Go field is the struct driftLeaf"},
		{"wrong $ref in additionalProperties", func(p, _ map[string]any) {
			p["by_name"] = obj("type", "object", "additionalProperties", obj("$ref", "#/$defs/driftOther"))
		}, "the root.by_name{}: the Go field is the struct driftLeaf"},
		{"bare object for a struct field (unregistered struct)", func(p, _ map[string]any) {
			p["leaf"] = obj("type", "object")
		}, "the root.leaf: the Go field is the struct driftLeaf"},
		{"$ref to a definition the schema lacks", func(_, d map[string]any) {
			delete(d, "driftLeaf")
		}, "the schema has no $defs/driftLeaf"},
		{"a definition no Go type reaches", func(_, d map[string]any) {
			d["Orphan"] = obj("type", "object")
		}, "$defs/Orphan describes no Go type"},
		{"slice element type", func(p, _ map[string]any) {
			p["args"] = obj("type", "array", "items", obj("type", "integer"))
		}, "the root.args[]: the Go field is string"},
		{"slice without items", func(p, _ map[string]any) {
			p["args"] = obj("type", "array")
		}, "the root.args: the array has no items schema"},
		{"map value type", func(p, _ map[string]any) {
			p["env"] = obj("type", "object", "additionalProperties", obj("type", "integer"))
		}, "the root.env{}: the Go field is string"},
		{"map without additionalProperties", func(p, _ map[string]any) {
			p["env"] = obj("type", "object")
		}, "the root.env: the object has no additionalProperties schema"},
		{"$ref on a scalar", func(p, _ map[string]any) {
			p["name"] = obj("$ref", "#/$defs/driftLeaf")
		}, "the root.name: the Go field is string"},
		{"a field inside a definition", func(_, d map[string]any) {
			d["driftLeaf"] = obj("type", "object", "properties", obj("max", obj("type", "string")))
		}, "$defs/driftLeaf.max: the Go field is int"},
		{"a property the Go type lacks", func(_, d map[string]any) {
			d["driftLeaf"] = obj("type", "object", "properties", obj("max", obj("type", "integer"), "min", obj("type", "integer")))
		}, "$defs/driftLeaf has properties driftLeaf lacks: [min]"},
	}
	for _, tc := range schemaCases {
		t.Run("schema: "+tc.name, func(t *testing.T) {
			expectDrift(t, schemaDrift(parse(t, tc.edit), rootType), tc.want)
		})
	}

	// The same base schema against Go types that changed under it.
	typeCases := []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{"[]string becomes []int", reflect.TypeOf(struct {
			root
			Args []int `json:"args,omitempty"`
		}{}), "the root.args[]: the Go field is int"},
		{"map[string]string becomes map[string]int", reflect.TypeOf(struct {
			root
			Env map[string]int `json:"env,omitempty"`
		}{}), "the root.env{}: the Go field is int"},
		{"a new struct field the schema lacks", reflect.TypeOf(struct {
			root
			Retry *driftLeaf `json:"retry,omitempty"`
		}{}), "the root lacks the fields of"},
		{"an allOf with one $ref is a reference", rootType, ""},
	}
	for _, tc := range typeCases {
		t.Run("type: "+tc.name, func(t *testing.T) {
			var edit func(props, defs map[string]any)
			if tc.want == "" {
				edit = func(p, _ map[string]any) {
					p["leaves"] = obj("type", "array", "items", obj("allOf", []any{
						obj("$ref", "#/$defs/driftLeaf"), obj("required", []any{"max"}),
					}))
				}
			}
			d := schemaDrift(parse(t, edit), tc.typ)
			if tc.want == "" {
				if len(d) != 0 {
					t.Errorf("no difference expected, schemaDrift found: %v", d)
				}
				return
			}
			expectDrift(t, d, tc.want)
		})
	}
}

func expectDrift(t *testing.T, drift []string, want string) {
	t.Helper()
	for _, d := range drift {
		if strings.Contains(d, want) {
			return
		}
	}
	t.Errorf("schemaDrift found %v, want a difference with %q", drift, want)
}

// TestCheckedInScenariosValidate applies the schema to the scenarios the
// repository ships, which bench run reads: they decode strictly, pass
// Validate, and validate against scenario.schema.json.
func TestCheckedInScenariosValidate(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "benchmarking", "scenarios", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no scenario under benchmarking/scenarios")
	}
	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			rawYAML, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			s, err := DecodeYAMLStrict[Scenario](rawYAML)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if err := s.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			jsonBytes, err := yaml.YAMLToJSON(rawYAML)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateAgainstJSONSchema("scenario.schema.json", jsonBytes); err != nil {
				t.Errorf("schema: %v", err)
			}
		})
	}
}
