# Tinode's Protocol Buffer and gRPC definitions

Definitions for Tinode gRPC client and plugins.

To generate `Go` bindings add the following comment to your code and run `go generate`
```
//go:generate protoc --proto_path=../pbx --go_out=plugins=grpc:../pbx ../pbx/model.proto
```
