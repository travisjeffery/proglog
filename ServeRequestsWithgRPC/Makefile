compile:
	protoc api/v1/*.proto \
		--gogo_out=\
Mgogoproto/gogo.proto=github.com/gogo/protobuf/proto,plugins=grpc:. \
		--proto_path=\
$$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf) \
		--proto_path=.
