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
)

func TestRepeatedEvents(t *testing.T) {
	streamName := fmt.Sprintf("eventstore-tests-%v", uuid.New())
	const batchSize = 18
	const batches = 4

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		var seen [batchSize*batches + 1]bool

		reader := connect()
		stream, err := reader.Read(context.Background(), &streams.ReadReq{
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

				//log.Println("got event", evt.StreamRevision, evt.Metadata["type"], evt.StreamIdentifier.String(), evt.CommitPosition)
				if seen[evt.StreamRevision] {
					t.Error("saw event twice", evt.StreamRevision, evt.Metadata["type"], evt.StreamIdentifier.String(), evt.CommitPosition)
				}
				seen[evt.StreamRevision] = true

				if evt.Metadata["type"] == "complete" {
					for i, v := range seen {
						if !v {
							t.Error(fmt.Errorf("missed event %d", i))
						}
					}
					wg.Done()
					return
				}

			default:
				panic(fmt.Errorf("eventstore.stream.OpenStream: unhandled init message %T", content))
			}
		}
	}()

	lastPos := -1

	time.Sleep(10 * time.Millisecond)

	writer := connect()
	for i := 0; i < batches; i++ {
		write(writer, streamName, lastPos, "event", batchSize)
		lastPos += batchSize

		time.Sleep(10 * time.Millisecond) // enough time to drain the server send buffer
	}

	write(writer, streamName, lastPos, "complete", 1)

	wg.Wait()
}
