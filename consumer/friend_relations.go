package consumer

import (
	"context"
	"log"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type FriendRelationConsumer struct {
	Id        int
	TableName string
	Reporter  *scyllacdc.PeriodicProgressReporter
}

func (c *FriendRelationConsumer) End() error {
	_ = c.Reporter.SaveAndStop(context.Background())
	return nil
}

func (c *FriendRelationConsumer) Consume(ctx context.Context, ch scyllacdc.Change) error {
	log.Printf("%v", ch.Delta)
	return nil
}
