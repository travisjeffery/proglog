CONFIG_PATH=${HOME}/.proglog/

init:
	mkdir -p ${CONFIG_PATH}

gencert:
	cfssl gencert \
		-initca configs/certs/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=configs/certs/ca-config.json \
		-profile=server \
		configs/certs/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=configs/certs/ca-config.json \
		-profile=client \
		configs/certs/client-csr.json | cfssljson -bare client

	mv *.pem *.csr ${CONFIG_PATH}

test:
	go test -race ./...


compile:
	protoc api/v1/*.proto \
		--gogo_out=plugins=grpc:. \
		--proto_path=. \
		--proto_path=vendor \
		--proto_path=vendor/github.com/gogo/protobuf/protobuf
