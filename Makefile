.PHONY: proto
proto:
	protoc --gogofast_out=. -I=.:../../../ epaxosprotobuf/epaxos.proto
	protoc --gogofast_out=. -I=.:../../../ genericsmrprotobuf/genericsmrprotobuf.proto

.PHONY: protogorums
protogorums:
	protoc --gorums_out=plugins=grpc+gorums:. -I=.:../../../ epaxosprotobuf/epaxos.proto
	protoc --gogofast_out=. -I=.:../../../ genericsmrprotobuf/genericsmrprotobuf.proto
