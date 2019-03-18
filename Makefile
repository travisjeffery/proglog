CONFIG_PATH=${HOME}/.proglog/

test:
	go test ./...

init:
	mkdir -p ${CONFIG_PATH}

gencert:
	cfssl gencert \
		-initca certs/ca-csr.json | cfssljson -bare ca

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=server \
		certs/server-csr.json | cfssljson -bare server

	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=certs/ca-config.json \
		-profile=client \
		certs/client-csr.json | cfssljson -bare client

	mv *.pem *.csr ${CONFIG_PATH}
