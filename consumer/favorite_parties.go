package consumer

import (
	"context"
	"log"

	"github.com/clubo-app/packages/stream"
	"github.com/clubo-app/protobuf/events"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FavoritePartiesConsumer struct {
	Id        int
	TableName string
	Stream    stream.Stream
	Reporter  *scyllacdc.PeriodicProgressReporter
}

func (c *FavoritePartiesConsumer) End() error {
	_ = c.Reporter.SaveAndStop(context.Background())
	return nil
}

func (c *FavoritePartiesConsumer) Consume(ctx context.Context, ch scyllacdc.Change) error {
	for _, change := range ch.Delta {
		var err error
		switch change.GetOperation() {
		case scyllacdc.Insert:
			_ = c.processInsert(ctx, change)
		case scyllacdc.RowDelete:
			_ = c.processDelete(ctx, change)
		default:
			log.Println("unsupported operation: " + change.GetOperation().String())
		}
		if err != nil {
			return err
		}
	}
	c.Reporter.Update(ch.Time)
	return nil
}

func (c *FavoritePartiesConsumer) processInsert(ctx context.Context, change *scyllacdc.ChangeRow) error {
	user_id, _ := change.GetValue("user_id")
	party_id, _ := change.GetValue("party_id")
	favorited_at, _ := change.GetValue("favorited_at")

	uId := ParseString(user_id)
	pId := ParseString(party_id)
	fAt := ParseTimestamp(favorited_at)

	if uId != "" && pId != "" && fAt != nil {
		e := events.PartyFavorited{
			UserId:      uId,
			PartyId:     pId,
			FavoritedAt: fAt,
		}
		err := c.Stream.PublishEvent(&e)
		if err != nil {
			log.Println(err)
		}

		return err
	}

	return nil
}

func (c *FavoritePartiesConsumer) processDelete(ctx context.Context, change *scyllacdc.ChangeRow) error {
	user_id, _ := change.GetValue("user_id")
	party_id, _ := change.GetValue("party_id")

	uId := ParseString(user_id)
	pId := ParseString(party_id)
	if uId != "" && pId != "" {
		e := events.PartyUnfavorited{
			UserId:        uId,
			PartyId:       pId,
			UnfavoritedAt: timestamppb.Now(),
		}
		log.Println("Publishing: ", e)
		err := c.Stream.PublishEvent(&e)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}
