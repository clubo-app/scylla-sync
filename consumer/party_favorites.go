package consumer

import (
	"context"
	"log"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type PartyFavoritesConsumer struct {
	Id        int
	TableName string
	Reporter  *scyllacdc.PeriodicProgressReporter
}

func (c *PartyFavoritesConsumer) End() error {
	_ = c.Reporter.SaveAndStop(context.Background())
	return nil
}

func (c *PartyFavoritesConsumer) Consume(ctx context.Context, ch scyllacdc.Change) error {
	log.Printf("%v", ch.Delta)
	return nil
}
