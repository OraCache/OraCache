module github.com/OraCache/oracache/cmd/oracache

go 1.27.3

require (
	github.com/OraCache/oracache v0.0.0
	github.com/OraCache/oracache/gen v0.0.0
	github.com/alecthomas/kong v1.6.1
	google.golang.org/grpc v1.64.1
	google.golang.org/protobuf v1.34.2
)

require (
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/sys v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240318140521-94a12d6c2237 // indirect
)

replace github.com/OraCache/oracache => ../..

replace github.com/OraCache/oracache/gen => ../../gen
