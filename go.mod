module github.com/OraCache/oracache

go 1.27.3

require (
	github.com/OraCache/oracache/gen v0.0.0
	go.etcd.io/raft/v3 v3.6.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237
	google.golang.org/grpc v1.64.1
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
)

replace github.com/OraCache/oracache/gen => ./gen
