package consumer

import (
	"context"
	"log"

	"github.com/clubo-app/packages/stream"
	"github.com/clubo-app/protobuf/events"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type FriendRelationConsumer struct {
	Id        int
	TableName string
	Stream    stream.Stream
	Reporter  *scyllacdc.PeriodicProgressReporter
}

func (c *FriendRelationConsumer) End() error {
	_ = c.Reporter.SaveAndStop(context.Background())
	return nil
}

func (c *FriendRelationConsumer) Consume(ctx context.Context, ch scyllacdc.Change) error {
	for _, change := range ch.Delta {
		switch change.GetOperation() {
		case scyllacdc.Update:
			_ = c.processUpdateOrInsert(ctx, change)
		case scyllacdc.Insert:
			_ = c.processUpdateOrInsert(ctx, change)
		default:
			log.Println("unsupported operation: " + change.GetOperation().String())
		}
	}
	c.Reporter.Update(ch.Time)
	return nil
}

func (c *FriendRelationConsumer) processUpdateOrInsert(ctx context.Context, change *scyllacdc.ChangeRow) error {
	user_id, _ := change.GetValue("user_id")
	friend_id, _ := change.GetValue("friend_id")
	accepted_at, _ := change.GetValue("accepted_at")
	requested_at, _ := change.GetValue("requested_at")

	uId := ParseString(user_id)
	fId := ParseString(friend_id)
	aAt := ParseTimestamp(accepted_at)
	rAt := ParseTimestamp(requested_at)

	var err error
	if aAt == nil {
		e := events.FriendRequested{
			UserId:      uId,
			FriendId:    fId,
			RequestedAt: rAt,
		}

		err = c.Stream.PublishEvent(&e)
	} else {
		e := events.FriendCreated{
			UserId:     uId,
			FriendId:   fId,
			AcceptedAt: aAt,
		}

		err = c.Stream.PublishEvent(&e)
	}

	if err != nil {
		log.Println(err)
	}

	return err
}

func (c *FriendRelationConsumer) processDelete(ctx context.Context, change *scyllacdc.ChangeRow) error {
	log.Println(change)
	user_id, _ := change.GetValue("user_id")
	friend_id, _ := change.GetValue("friend_id")

	uId := ParseString(user_id)
	fId := ParseString(friend_id)
	if uId != "" && fId != "" {
		e := events.FriendRemoved{
			UserId:   uId,
			FriendId: fId,
		}
		log.Println("Publishing: ", e)
		err := c.Stream.PublishEvent(&e)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}
