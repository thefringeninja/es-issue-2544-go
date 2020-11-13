package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/uuid"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
)

func TestHangWithClient(t *testing.T) {
	const batchSize = 18
	const batches = 20
	done := make(chan struct{})
	config, err := client.ParseConnectionString("esdb://localhost:2113/?tls=false")
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	esc, err := client.NewClient(config)

	err = esc.Connect()
	if err != nil {
		panic(err)
	}

	postFix, _ := uuid.NewV4()
	streamName := fmt.Sprintf("eventstore-tests-hang-%v", postFix)

	subscription, err := esc.SubscribeToAll(context.Background(), position.EndPosition, false, func(event messages.RecordedEvent) {
		if event.StreamID != streamName {
			return
		}

		switch event.SystemMetadata["type"] {
		case "complete":
			close(done)
		default:
			time.Sleep(800 * time.Microsecond)
		}

	}, nil, func(reason string) {
		fmt.Print(reason)
	})

	if err != nil {
		panic(err)
	}

	err = subscription.Start()

	if err != nil {
		panic(err)
	}

	lastPos := uint64(0)

	time.Sleep(10 * time.Millisecond)

	esc.AppendToStream(context.Background(), streamName, streamrevision.StreamRevisionNoStream, createEvents("start", 1))

	for i := 0; i < batches; i++ {
		esc.AppendToStream(context.Background(), streamName, streamrevision.NewStreamRevision(lastPos), createEvents("event", 10))
		esc.AppendToStream(context.Background(), streamName, streamrevision.NewStreamRevision(lastPos+10), createEvents("event", batchSize-10))
		lastPos += batchSize

		time.Sleep(62 * time.Millisecond)
	}

	esc.AppendToStream(context.Background(), streamName, streamrevision.NewStreamRevision(lastPos), createEvents("complete", 1))

	<-done

	select {
	case <-time.After(10 * time.Second):
		panic("reader never saw the completion event")
	case <-done:
	}
}

func createEvents(eventType string, count int) []messages.ProposedEvent {
	events := make([]messages.ProposedEvent, count)
	for i := range events {
		eventId, _ := uuid.NewV4()
		events[i] = messages.ProposedEvent{
			EventID:     eventId,
			EventType:   eventType,
			ContentType: "application/json",
			Data:        []byte(`{}`),
		}
	}

	return events
}
