module github.com/travisjeffery/proglog

require (
	github.com/casbin/casbin v1.9.1
	github.com/golang/protobuf v1.5.2
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/raft v1.1.1
	github.com/hashicorp/raft-boltdb v0.0.0-20191021154308-4207f1bf0617
	github.com/hashicorp/serf v0.8.5
	github.com/soheilhy/cmux v0.1.4
	github.com/spf13/cobra v1.0.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.8.1
	github.com/travisjeffery/go-dynaport v0.0.0-20171218080632-f8768fb615d5
	github.com/tysontate/gommap v0.0.0-20190103205956-899e1273fb5c
	go.opencensus.io v0.24.0
	go.uber.org/zap v1.10.0
	golang.org/x/net v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f
	google.golang.org/grpc v1.52.0
	google.golang.org/grpc/examples v0.0.0-20230222033013-5353eaa44095 // indirect
	google.golang.org/protobuf v1.28.1
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
)

replace github.com/hashicorp/raft-boltdb => github.com/travisjeffery/raft-boltdb v0.0.0-20201002143322-bc94ee46437b

go 1.13
