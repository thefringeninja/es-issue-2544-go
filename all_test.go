package main

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vektah/eventstore-tests/streams"
)

func TestHang(t *testing.T) {
	streamName := fmt.Sprintf("eventstore-tests-hang-%v", uuid.New())
	const batchSize = 18
	const batches = 20

	ready := make(chan struct{})
	done := make(chan struct{})
	ctx, _ := context.WithCancel(context.Background())

	go func() {
		var seen [batchSize*batches + 2]bool

		reader, conn := connect()
		defer conn.Close()
		stream, err := reader.Read(ctx, &streams.ReadReq{
			Options: &streams.ReadReq_Options{
				ReadDirection: streams.ReadReq_Options_Forwards,
				FilterOption:  &streams.ReadReq_Options_NoFilter{},
				UuidOption:    &streams.ReadReq_Options_UUIDOption{Content: &streams.ReadReq_Options_UUIDOption_Structured{}},
				CountOption:   &streams.ReadReq_Options_Subscription{},
				StreamOption: &streams.ReadReq_Options_All{
					All: &streams.ReadReq_Options_AllOptions{
						AllOption: &streams.ReadReq_Options_AllOptions_Start{},
					},
				},
			},
		})

		if err != nil {
			panic(err)
		}

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

				if string(evt.StreamIdentifier.StreamName) != streamName {
					continue
				}

				// log.Println("got event", evt.StreamRevision, evt.Metadata["type"], evt.StreamIdentifier.String(), evt.CommitPosition)
				if seen[evt.StreamRevision] {
					t.Error("saw event twice", evt.StreamRevision, evt.Metadata["type"], evt.StreamIdentifier.String(), evt.CommitPosition)
				}
				seen[evt.StreamRevision] = true

				switch evt.Metadata["type"] {
				case "start":
					close(ready)
				case "complete":
					for i, v := range seen {
						if !v {
							t.Error(fmt.Errorf("missed event %d", i))
						}
					}
					close(done)
					return
				default:
					time.Sleep(800 * time.Microsecond)
				}

			default:
				panic(fmt.Errorf("eventstore.stream.OpenStream: unhandled init message %T", content))
			}
		}
	}()

	lastPos := 0

	time.Sleep(10 * time.Millisecond)

	writer, conn := connect()
	defer conn.Close()

	write(writer, streamName, -1, "start", 1)

	<-ready

	for i := 0; i < batches; i++ {
		write(writer, streamName, lastPos, "event", 10)
		write(writer, streamName, lastPos+10, "event", batchSize-10)
		lastPos += batchSize

		time.Sleep(62 * time.Millisecond)
	}

	write(writer, streamName, lastPos, "complete", 1)

	select {
	case <-time.After(10 * time.Second):
		panic("reader never saw the completion event")
	case <-done:
	}
}
