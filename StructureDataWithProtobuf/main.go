package main

import (
	"github.com/gogo/protobuf/vanity/command"
)

func main() {
	req := command.Read()
	gen := &Generator{}
	res := command.GeneratePlugin(req, gen, "_setter.go")
	command.Write(res)
}
