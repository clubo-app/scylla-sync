package consumer

import (
	"context"
	"log"

	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type Consumer struct {
	Id            int
	TableName     string
	Reporter      *scyllacdc.PeriodicProgressReporter
	FriendHandler FriendRelationConsumer
	PartyHandler  FavoritePartiesConsumer
}

func (c *Consumer) End() error {
	_ = c.Reporter.SaveAndStop(context.Background())
	return nil
}

func (c *Consumer) Consume(ctx context.Context, ch scyllacdc.Change) error {
	log.Println(ch)
	if c.TableName == FRIEND_RELATIONS_TABLE {
		c.FriendHandler.Consume(ctx, ch)
	} else if c.TableName == FAVORITE_PARTIES_TABLE {
		c.PartyHandler.Consume(ctx, ch)
	}
	return nil
}
