module WEEK-4/server

go 1.16

require (
	Week-4/telemetry v0.0.0-00010101000000-000000000000
	google.golang.org/protobuf v1.27.1
	network/packet v0.0.0-00010101000000-000000000000
)

replace Week-4/telemetry => ../telemetry

replace network/packet => ../packet
