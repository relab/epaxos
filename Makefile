.PHONY: proto
proto:
	protoc --gogofast_out=. -I=.:../../../ epaxosprotobuf/epaxos.proto
	protoc --gogofast_out=. -I=.:../../../ genericsmrprotobuf/genericsmrprotobuf.proto
