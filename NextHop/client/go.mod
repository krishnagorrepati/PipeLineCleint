module client

go 1.13

require (
	Week-4/telemetry v0.0.0-00010101000000-000000000000
	github.com/golang/protobuf v1.5.2
	google.golang.org/protobuf v1.27.1
	network/packet v0.0.0-00010101000000-000000000000
)

replace network/packet => ../packet

replace Week-4/telemetry => ../telemetry
