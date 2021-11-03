package main

import (
	"log"
	"net"
        "fmt"
        "encoding/binary"
        "time"
//       "math/rand"

//	"google.golang.org/protobuf/proto"

	telem "Week-4/telemetry"
//        "samples"

	"bufio"
	"encoding/json"
        "github.com/golang/protobuf/proto"
        "github.com/golang/protobuf/jsonpb"
	"io/ioutil"
	"os"
	"strings"
//	network "network/packet"
)
const (
        ENC_ST_MAX_DGRAM          uint32 = 64 * 1024
        ENC_ST_MAX_PAYLOAD        uint32 = 1024 * 1024
        ENC_ST_HDR_MSG_FLAGS_NONE uint16 = 0
        ENC_ST_HDR_MSG_SIZE       uint32 = 12
        ENC_ST_HDR_VERSION        uint16 = 1
)

type encapSTHdrMsgType uint16

const (
        ENC_ST_HDR_MSG_TYPE_UNSED encapSTHdrMsgType = iota
        ENC_ST_HDR_MSG_TYPE_TELEMETRY_DATA
        ENC_ST_HDR_MSG_TYPE_HEARTBEAT
)

type encapSTHdrMsgEncap uint16

const (
        ENC_ST_HDR_MSG_ENCAP_UNSED encapSTHdrMsgEncap = iota
        ENC_ST_HDR_MSG_ENCAP_GPB
        ENC_ST_HDR_MSG_ENCAP_JSON
        ENC_ST_HDR_MSG_ENCAP_GPB_COMPACT
        ENC_ST_HDR_MSG_ENCAP_GPB_KV
)

const (
//	address = "localhost:50051"
	address = "10.105.236.227:5432"
//        address = "10.105.227.59:5432"
)
type encapSTHdr struct {
        MsgType       encapSTHdrMsgType
        MsgEncap      encapSTHdrMsgEncap
        MsgHdrVersion uint16
        Msgflag       uint16
        Msglen        uint32
}


func main() {
	// Set up a connection to the server.
	conn, err := net.Dial("tcp", address)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
               fmt.Printf("did not connect: %v", err)
	}
	defer conn.Close()

	//var cid uint64 = 2
	//bp := "tcp"
	//sid := "venu"
	//mv := "1.2"
	//var csT uint64 = 100
	//var mts uint64 = 5
	//var ts uint64 = 2
	//n := "venu"
	//agd := true
	//vbt := pb.TelemetryField_BoolValue{
	//			BoolValue : true,
	//		}
	//var ced uint64 = 5
      /*  c := telem.Telemetry{
                        CollectionId:        cid,
                        EncodingPath:        bp,
                        CollectionStartTime: csT,
                        CollectionEndTime:   csT,
                } */
 
/*	c := pb.Telemetry{
		CollectionId:           &cid,
		BasePath:               &bp,
		SubscriptionIdentifier: &sid,
		ModelVersion:           &mv,
		CollectionStartTime:    &csT,
		MsgTimestamp:           &mts,
		Fields: []*pb.TelemetryField{
			{
				Timestamp:   &ts,
				Name:        &n,
				AugmentData: &agd,
				//ValueByType: &vbt,
				Fields: []*pb.TelemetryField{},
			},
		},
		CollectionEndTime: &ced,
	}*/
       
       for { 
		//msg, err := proto.Marshal(&c)
		fullmsg := MDTSampleTelemetryTableFetchOne(
			   SAMPLE_TELEMETRY_DATABASE_BASIC)
		gpbMessage := fullmsg.SampleStreamGPB
	 

		 
		hdr := encapSTHdr{
			MsgType:       ENC_ST_HDR_MSG_TYPE_TELEMETRY_DATA,
			MsgEncap:      ENC_ST_HDR_MSG_ENCAP_GPB,
			MsgHdrVersion: ENC_ST_HDR_VERSION,
			Msgflag:       ENC_ST_HDR_MSG_FLAGS_NONE,
			Msglen:        uint32(len(gpbMessage)),
		}


		err2 := binary.Write(conn, binary.BigEndian, &hdr)
		if err2 != nil {
			fmt.Println("Failed to write data header")
			return
		}
		if err != nil {
			log.Fatalln("Failed to encode address book:", err)
		}
		wrote, err := conn.Write(gpbMessage)
		if err != nil {
			fmt.Println("Failed write data 1")
			return
		}
		fmt.Println("Wrote %d, expect %d for data 1",
         			wrote, len(gpbMessage))
	/*	if wrote != len(msg) {
			fmt.Println("Wrote %d, expect %d for data 1",
				wrote, len(msg))
			return
		} */
                time.Sleep(1*time.Second) 
      }
/*	s := network.NewStream(1024)
	s.OnError(func(err network.IOError) {
		conn.Close()
	})

	s.SetConnection(conn)

	s.Outgoing <- network.New(0, []byte(msg))

	recv := <-s.Incoming

	log.Print(recv.Data)
*/

}


type sampleTelemetryTable []SampleTelemetryTableEntry

type SampleTelemetryTableEntry struct {
	Sample             *telem.Telemetry
	SampleStreamGPB    []byte
	SampleStreamJSON   []byte
	SampleStreamJSONKV []byte
	Leaves             int
	Events             int
}

type SampleTelemetryDatabaseID int

const (
	SAMPLE_TELEMETRY_DATABASE_BASIC SampleTelemetryDatabaseID = iota
)

var sampleTelemetryDatabase map[SampleTelemetryDatabaseID]sampleTelemetryTable

func MDTSampleTelemetryTableFetchOne(
	dbindex SampleTelemetryDatabaseID) *SampleTelemetryTableEntry {

	if len(sampleTelemetryDatabase) <= int(dbindex) {
		return nil
	}

	table := sampleTelemetryDatabase[dbindex]
	return &table[0]
}

type MDTContext interface{}
type MDTSampleCallback func(sample *SampleTelemetryTableEntry, context MDTContext) (abort bool)

//
// MDTSampleTelemetryTableIterate iterates over table of samples
// calling caller with function MDTSampleCallback and opaque context
// MDTContext provided, for every known sample. The number of samples
// iterated over is returned.
func MDTSampleTelemetryTableIterate(
	dbindex SampleTelemetryDatabaseID,
	fn MDTSampleCallback,
	c MDTContext) (applied int) {

	if len(sampleTelemetryDatabase) <= int(dbindex) {
		return 0
	}
	count := 0
	table := sampleTelemetryDatabase[dbindex]
	for _, entry := range table {
		count++
		if fn(&entry, c) {
			break
		}
	}

	return count
}

func MDTLoadMetrics() string {
	b, e := ioutil.ReadFile("mdt_msg_samples/dump.metrics")
	if e == nil {
		return string(b)
	}
	return ""
}

func init() {

	sampleTelemetryDatabase = make(map[SampleTelemetryDatabaseID]sampleTelemetryTable)

	sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC] = sampleTelemetryTable{}

	marshaller := &jsonpb.Marshaler{
		EmitDefaults: true,
		OrigName:     true,
	}

	kv, err := os.Open("dump.jsonkv")
	if err != nil {
		kv, err = os.Open("mdt_msg_samples/dump.jsonkv")
		if err != nil {
			fmt.Println(err)
		}
	}
	defer kv.Close()

	dump := bufio.NewReader(kv)
	decoder := json.NewDecoder(dump)

	_, err = decoder.Token()
	if err != nil {
		fmt.Println(err)
	}

	// Read the messages and build the db.
	for decoder.More() {
		var m telem.Telemetry

		err := jsonpb.UnmarshalNext(decoder, &m)
		if err != nil {
			fmt.Println(err)
		}
                m.CollectionStartTime = getCurrTime()
                m.MsgTimestamp = getCurrTime()
                dataGpb := m.DataGpbkv
                var keys, content []*telem.TelemetryField
                for _, item:= range dataGpb {
                   item.Timestamp =  getCurrTime()
                   fileds := item.Fields
                   for _, f := range fileds {
                       f.Timestamp =  getCurrTime()
                       if (f.Name == "keys") {
                          keys = f.Fields
                       } else if  f.Name == "content" {
                          content = f.Fields
                       } 
                   }
                }

                fmt.Println("Keys:---------")
		for _, item:= range keys {
		       item.Timestamp =  getCurrTime()
                       fmt.Println(item)
                }
		
                fmt.Println("Content:---------")
                for _, item:= range content {
		       item.Timestamp =  getCurrTime()
                       if item.Name == "total-cpu-one-minute" {
                         // item.ValueByType = Week-4.TelemetryField_Uint32Value(rand.Intn(200)) 
                       }
                       if item.Name == "bytes-received" {
 			  //item.ValueByType = telem.TelemetryField_Uint32Value(5)
                       }
                       fmt.Println(item)
                }

                m.CollectionEndTime = getCurrTime() + 10
		gpbstream, err := proto.Marshal(&m)
		if err != nil {
			fmt.Println(err)
		}

		jsonstream, err := marshaller.MarshalToString(&m)
		if err != nil {
			fmt.Println(err)
		}

		entry := SampleTelemetryTableEntry{
			Sample:             &m,
			SampleStreamGPB:    gpbstream,
			SampleStreamJSONKV: json.RawMessage(jsonstream),
			Leaves:             strings.Count(jsonstream, "\"name\""),
			Events:             strings.Count(jsonstream, "\"content\""),
		}

		sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC] =
			append(sampleTelemetryDatabase[SAMPLE_TELEMETRY_DATABASE_BASIC], entry)
	}
}

func getCurrTime() uint64 {
     now := time.Now()
     nanos := now.UnixNano()
     millis := uint64(nanos / 1000000)
     return millis
}
