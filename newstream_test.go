package main

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vektah/eventstore-tests/shared"
	"github.com/vektah/eventstore-tests/streams"
	"google.golang.org/grpc"
)

const username = "admin"
const password = "changeme"
const addr = "localhost:2113"

func TestNewStreams(t *testing.T) {
	client := connect()

	streamName := fmt.Sprintf("eventstore-tests-%v", uuid.New())

	stream, err := client.Read(context.Background(), &streams.ReadReq{
		Options: &streams.ReadReq_Options{
			ReadDirection: streams.ReadReq_Options_Forwards,
			FilterOption:  &streams.ReadReq_Options_NoFilter{},
			UuidOption:    &streams.ReadReq_Options_UUIDOption{Content: &streams.ReadReq_Options_UUIDOption_Structured{}},
			CountOption:   &streams.ReadReq_Options_Subscription{},
			StreamOption: &streams.ReadReq_Options_Stream{
				Stream: &streams.ReadReq_Options_StreamOptions{
					StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(streamName)},
					RevisionOption:   &streams.ReadReq_Options_StreamOptions_Start{},
				},
			},
		},
	})
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				panic("eof should never happen when following")
			} else if err != nil {
				panic(err)
			}

			switch content := resp.Content.(type) {
			case *streams.ReadResp_StreamNotFound_:
				panic("not found")
			case *streams.ReadResp_Confirmation:
				fmt.Println("started subscription", content.Confirmation.SubscriptionId)
			case *streams.ReadResp_Event:
				evt := content.Event.Event

				fmt.Println("got event", evt.Metadata["type"], evt.StreamIdentifier.String(), evt.CommitPosition)
				if string(evt.StreamIdentifier.StreamName) == streamName {
					wg.Done()
				}

			default:
				panic(fmt.Errorf("eventstore.stream.OpenStream: unhandled init message %T", content))
			}
		}
	}()

	write(client, streamName, -1, "first")
	time.Sleep(20 * time.Millisecond)
	write(client, streamName, 0, "second")

	wg.Wait()
}

func connect() streams.StreamsClient {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(creds{username, password}),
	}

	conn, err := grpc.DialContext(context.Background(), addr, opts...)
	if err != nil {
		panic(err)
	}

	return streams.NewStreamsClient(conn)
}

func write(client streams.StreamsClient, streamName string, pos int, eventType string) {
	var start *streams.AppendReq_Options
	if pos == -1 {
		start = &streams.AppendReq_Options{
			StreamIdentifier:       &shared.StreamIdentifier{StreamName: []byte(streamName)},
			ExpectedStreamRevision: &streams.AppendReq_Options_NoStream{},
		}
	} else {
		start = &streams.AppendReq_Options{
			StreamIdentifier:       &shared.StreamIdentifier{StreamName: []byte(streamName)},
			ExpectedStreamRevision: &streams.AppendReq_Options_Revision{Revision: uint64(pos)},
		}
	}

	appender, err := client.Append(context.Background())
	if err != nil {
		panic(err)
	}

	err = appender.Send(&streams.AppendReq{Content: &streams.AppendReq_Options_{Options: start}})
	if err != nil {
		panic(err)
	}

	err = appender.Send(&streams.AppendReq{
		Content: &streams.AppendReq_ProposedMessage_{
			ProposedMessage: &streams.AppendReq_ProposedMessage{
				Id: &shared.UUID{Value: &shared.UUID_String_{String_: uuid.New().String()}},
				Metadata: map[string]string{
					"type":         eventType,
					"content-type": "application/json",
				},
				Data: []byte(`{}`),
			},
		},
	})

	resp, err := appender.CloseAndRecv()
	if err != nil {
		panic(err)
	}
	fmt.Println("write resp", resp)
}
