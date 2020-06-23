// START: start
package main

import (
    "bytes"
    "html/template"

    "github.com/gogo/protobuf/proto"
    "github.com/gogo/protobuf/protoc-gen-gogo/descriptor"
    "github.com/gogo/protobuf/protoc-gen-gogo/generator"
    "github.com/travisjeffery/protoc-gen-setter/setter"
)

type Generator struct {
    *generator.Generator
    generator.PluginImports
}

func init() { generator.RegisterPlugin(&Generator{}) }

func (p *Generator) Name() string { return "setter" }

func (p *Generator) Init(g *generator.Generator) { p.Generator = g }

func (p *Generator) GenerateImports(file *generator.FileDescriptor) {}

func (p *Generator) Generate(file *generator.FileDescriptor) { // ... }
// END: start

// START: generate_start
type Field struct {
	Name, Type string
}
type Struct struct {
	Name   string
	Fields []Field
}
var structs []Struct
for _, msg := range file.Messages() {
	str := &Struct{Name: *msg.Name}
	appnd := func(f *descriptor.FieldDescriptorProto) {
		name := f.GetName()
		typ, _ := p.GoType(msg, f)
		str.Fields = append(str.Fields, Field{Name: name, Type: typ})
	}
	if proto.GetBoolExtension(msg.Options, setter.E_All, false) {
		for _, f := range msg.GetField() {
			appnd(f)
		}
	} else {
		for _, f := range msg.GetField() {
			if proto.GetBoolExtension(f.Options, setter.E_One, false) {
				appnd(f)
			}
		}
	}
	structs = append(structs, *str)
}

// ...
// END:generate_start

// START: generate_end
var tmpl = `
{{ range $struct := . }}
{{ range $field := $struct.Fields }}
func (t *{{ $struct.Name }}) Set(v {{ $field.Type }}) {
	t.{{ $field.Name }} = v
}
{{ end }}
{{ end }}
`
t := template.Must(template.New("setter").Parse(tmpl))
var buf bytes.Buffer
if err := t.Execute(&buf, structs); err != nil {
fmt.Fprintf(os.Stderr, "failed to execute template: %v %v", err)
	os.Exit(1)
}
p.P(buf.String())
// END: generate_end
