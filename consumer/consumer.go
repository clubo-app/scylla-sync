package consumer

import (
	"context"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type Consumer struct {
	Id            int
	TableName     string
	Reporter      *scyllacdc.PeriodicProgressReporter
	FriendHandler FriendRelationHandler
	PartyHandler  PartyFavoritesHandler
}

func (c *Consumer) End() error {
	_ = c.Reporter.SaveAndStop(context.Background())
	return nil
}

func (c *Consumer) Consume(ctx context.Context, ch scyllacdc.Change) error {
	if c.TableName == FRIEND_RELATIONS_TABLE {
		c.FriendHandler.Consume(ctx, ch)
	} else if c.TableName == PARTY_FAVORITES_TABLE {
		c.PartyHandler.Consume(ctx, ch)
	}
	return nil
}
