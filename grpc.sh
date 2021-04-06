
## protocbuf 3.15
#protoc -I rpc/ rpc/carousel.proto \
#  --go_out=rpc/ --go_opt=paths=source_relative \
#  --go-grpc_out=rpc/ --go-grpc_opt=paths=source_relative

## protocbuf 3.0
#protoc -I rpc/ prc/carousel.proto --go_out=plugins=grpc:rpc
