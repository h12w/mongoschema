package main

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/yaml.v2"
)

var errEmptyURL = errors.New("mongoschema: no URL specified")

func main() {
	if len(os.Args) < 2 {
		fmt.Println("mongoschema [config.yaml]")
		return
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	var g Generator
	if err := yaml.Unmarshal(buf, &g); err != nil {
		log.Fatal(err)
	}
	if err := g.Generate(); err != nil {
		log.Fatal(err)
	}
}

type Generator struct {
	URL           string       `yaml:"url"`
	DB            string       `yaml:"db"`
	Limit         uint         `yaml:"limit"`
	Comments      bool         `yaml:"comments"`
	IgnoredFields []string     `yaml:"ignored_fields"`
	Collections   []Collection `yaml:"collections"`
}

type Collection struct {
	Name   string `yaml:"name"`
	Struct string `yaml:"struct"`
}

func (s *Generator) connect() (*mgo.Session, error) {
	if s.URL == "" {
		return nil, errEmptyURL
	}

	session, err := mgo.Dial(s.URL)
	if err != nil {
		return nil, err
	}
	session.EnsureSafe(&mgo.Safe{})
	session.SetBatch(1000)
	session.SetMode(mgo.Eventual, true)
	return session, nil
}

func (s *Generator) Generate() error {
	session, err := s.connect()
	if err != nil {
		return err
	}
	defer session.Close()
	for _, c := range s.Collections {
		collection := session.DB(s.DB).C(c.Name)

		root := StructType{}
		iter := collection.Find(nil).Iter()
		m := bson.M{}
		var seen uint
		for iter.Next(m) {
			if s.Limit != 0 && seen == s.Limit {
				break
			}
			root.Merge(NewType(m, s), s)
			m = bson.M{}
			seen++
		}
		if err := iter.Close(); err != nil {
			return err
		}
		fmt.Println(c.Struct, root.GoType(s))
		fmt.Println()
	}
	return nil
}

type Type interface {
	GoType(gen *Generator) string
	Merge(t Type, gen *Generator) Type
}

type LiteralType struct {
	Literal string
}

func (l LiteralType) GoType(gen *Generator) string {
	return l.Literal
}

func (l LiteralType) Merge(t Type, gen *Generator) Type {
	if isNil(l) {
		return t
	}
	if isNil(t) {
		return l
	}
	if l.GoType(gen) == t.GoType(gen) {
		return l
	}
	return MixedType{l, t}
}

var NilType = LiteralType{Literal: "nil"}

type MixedType []Type

func (m MixedType) GoType(gen *Generator) string {
	if !gen.Comments {
		return "interface{}"
	}
	var b bytes.Buffer
	fmt.Fprint(&b, "interface{} /* ")
	for i, v := range m {
		fmt.Fprint(&b, v.GoType(gen))
		if i != len(m)-1 {
			fmt.Fprint(&b, ",")
		}
		fmt.Fprint(&b, " ")
	}
	fmt.Fprint(&b, " */")
	return b.String()
}

func (m MixedType) Merge(t Type, gen *Generator) Type {
	for _, e := range m {
		if e.GoType(gen) == t.GoType(gen) {
			return m
		}
	}
	return append(m, t)
}

type PrimitiveType uint

const (
	PrimitiveBinary PrimitiveType = iota
	PrimitiveBool
	PrimitiveDouble
	PrimitiveInt32
	PrimitiveInt64
	PrimitiveObjectId
	PrimitiveString
	PrimitiveTimestamp
	PrimitiveDBRef
)

func (p PrimitiveType) GoType(gen *Generator) string {
	switch p {
	case PrimitiveBinary:
		return "bson.Binary"
	case PrimitiveBool:
		return "bool"
	case PrimitiveDouble:
		return "float64"
	case PrimitiveInt32:
		return "int32"
	case PrimitiveInt64:
		return "int64"
	case PrimitiveString:
		return "string"
	case PrimitiveTimestamp:
		return "time.Time"
	case PrimitiveObjectId:
		return "bson.ObjectId"
	case PrimitiveDBRef:
		return "mgo.DBRef"
	}
	panic(fmt.Sprintf("unknown primitive: %d", uint(p)))
}

func (p PrimitiveType) Merge(t Type, gen *Generator) Type {
	if isNil(p) {
		return t
	}
	if isNil(t) {
		return p
	}
	switch p {
	case PrimitiveInt32, PrimitiveInt64:
		if t == PrimitiveDouble {
			return PrimitiveDouble
		}
	}
	switch t {
	case PrimitiveInt32, PrimitiveInt64:
		if p == PrimitiveDouble {
			return PrimitiveDouble
		}
	}

	if p.GoType(gen) == t.GoType(gen) {
		return p
	}
	return MixedType{p, t}
}

type SliceType struct {
	Type
}

func (s SliceType) GoType(gen *Generator) string {
	return fmt.Sprintf("[]%s", s.Type.GoType(gen))
}

func (s SliceType) Merge(t Type, gen *Generator) Type {
	if isNil(s) {
		return t
	}
	if isNil(t) {
		return s
	}
	if s.GoType(gen) == t.GoType(gen) {
		return s
	}

	// If the target type is a slice of structs, we merge into the first struct
	// type in our own slice type.
	if targetSliceType, ok := t.(SliceType); ok {
		if targetSliceStructType, ok := targetSliceType.Type.(StructType); ok {
			// We're a slice of structs.
			if ownSliceStructType, ok := s.Type.(StructType); ok {
				s.Type = ownSliceStructType.Merge(targetSliceStructType, gen)
				return s
			}

			// We're a slice of mixed types, one of which may or may not be a struct.
			if sliceMixedType, ok := s.Type.(MixedType); ok {
				for i, v := range sliceMixedType {
					if vStructType, ok := v.(StructType); ok {
						sliceMixedType[i] = vStructType.Merge(targetSliceStructType, gen)
						return s
					}
				}
				return SliceType{Type: append(sliceMixedType, targetSliceStructType)}
			}
		}
	}
	return MixedType{s, t}
}

type StructType map[string]Type

func (s StructType) GoType(gen *Generator) string {
	var buf bytes.Buffer
	fmt.Fprintln(&buf, "struct {")
	var keys sort.StringSlice
	for k := range s {
		if sscontains(gen.IgnoredFields, k) {
			continue
		}
		keys = append(keys, k)
	}
	sort.Sort(keys)

	for _, k := range keys {
		v := s[k]
		if isValidFieldName(k) {
			vGoType := v.GoType(gen)
			fmt.Fprintf(
				&buf,
				"%s %s `bson:\"%s,omitempty\" json:\"%s,omitempty\"`\n",
				makeFieldName(k),
				vGoType,
				k, k,
			)
		} else {
			if gen.Comments {
				fmt.Fprintf(&buf, "// skipping invalid field name %s\n", k)
			}
		}
	}
	fmt.Fprint(&buf, "}")
	return buf.String()
}

func (s StructType) Merge(t Type, gen *Generator) Type {
	if isNil(t) {
		return s
	}
	if isNil(s) {
		return t
	}
	if o, ok := t.(StructType); ok {
		for k, v := range o {
			if e, ok := s[k]; ok {
				s[k] = e.Merge(v, gen)
			} else {
				s[k] = v
			}
		}
		return s
	}
	return MixedType{s, t}
}

func NewType(v interface{}, gen *Generator) Type {
	switch i := v.(type) {
	default:
		if fmt.Sprint(v) == "{}" {
			return NilType
		}
		panic(fmt.Sprintf("cannot determine type for %v with go type %T", v, v))
	case nil:
		return NilType
	case bson.ObjectId:
		return PrimitiveObjectId
	case bson.M:
		return NewStructType(i, gen)
	case []interface{}:
		if len(i) == 0 {
			return SliceType{Type: NilType}
		}
		var s Type
		for _, v := range i {
			vt := NewType(v, gen)
			if isNil(vt) {
				continue
			}
			if s == nil {
				s = SliceType{Type: vt}
			} else {
				s.Merge(SliceType{Type: vt}, gen)
			}
		}
		if s == nil {
			return SliceType{Type: NilType}
		}
		return s
	case int, int64:
		return PrimitiveInt64
	case int32:
		return PrimitiveInt32
	case bool:
		return PrimitiveBool
	case string:
		return PrimitiveString
	case time.Time, bson.MongoTimestamp:
		return PrimitiveTimestamp
	case float32, float64:
		return PrimitiveDouble
	case bson.Binary:
		return PrimitiveBinary
	}
}

func NewStructType(m bson.M, gen *Generator) Type {
	if m["$db"] != nil && m["$ref"] != nil && m["$id"] != nil {
		return PrimitiveDBRef
	}
	s := StructType{}
	for k, v := range m {
		t := NewType(v, gen)
		if isNil(t) {
			continue
		}
		s[k] = t
	}
	return s
}

func isValidFieldName(n string) bool {
	if n == "" {
		return false
	}
	if strings.IndexAny(n, "!*") == -1 {
		return true
	}
	return false
}

var (
	dashUnderscoreReplacer = strings.NewReplacer("-", " ", "_", " ")
	capsRe                 = regexp.MustCompile(`([A-Z])`)
	spaceRe                = regexp.MustCompile(`(\w+)`)
	forcedUpperCase        = map[string]bool{"id": true, "url": true, "api": true}
)

func split(str string) []string {
	str = dashUnderscoreReplacer.Replace(str)
	str = capsRe.ReplaceAllString(str, " $1")
	return spaceRe.FindAllString(str, -1)
}

func makeFieldName(s string) string {
	parts := split(s)
	for i, part := range parts {
		if forcedUpperCase[strings.ToLower(part)] {
			parts[i] = strings.ToUpper(part)
		} else {
			parts[i] = strings.Title(part)
		}
	}
	camel := strings.Join(parts, "")
	runes := []rune(camel)
	for i, c := range runes {
		ok := unicode.IsLetter(c) || unicode.IsDigit(c)
		if i == 0 {
			ok = unicode.IsLetter(c)
		}
		if !ok {
			runes[i] = '_'
		}
	}
	return string(runes)
}

func sscontains(l []string, v string) bool {
	for _, e := range l {
		if e == v {
			return true
		}
	}
	return false
}

func isNil(t Type) bool {
	if t == NilType {
		return true
	}
	if sliceType, ok := t.(SliceType); ok {
		return isNil(sliceType.Type)
	}
	if mixedType, ok := t.(MixedType); ok {
		return len(mixedType) > 0
	}
	return t == nil
}
