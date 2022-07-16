package consumer

import (
	"context"
	"errors"
	"log"

	"github.com/clubo-app/packages/stream"
	"github.com/clubo-app/protobuf/events"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type FriendRelationHandler struct {
	stream stream.Stream
}

func NewFriendRelationHandler(st stream.Stream) FriendRelationHandler {
	return FriendRelationHandler{stream: st}
}

func (c *FriendRelationHandler) Consume(ctx context.Context, ch scyllacdc.Change) error {
	for _, change := range ch.Delta {
		var err error
		switch change.GetOperation() {
		case scyllacdc.Update:
			err = c.processUpdateOrInsert(ctx, change)
		case scyllacdc.Insert:
			err = c.processUpdateOrInsert(ctx, change)
		default:
			return errors.New("unsupported operation: " + change.GetOperation().String())
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *FriendRelationHandler) processUpdateOrInsert(ctx context.Context, change *scyllacdc.ChangeRow) error {
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

		err = c.stream.PublishEvent(&e)
	} else {
		e := events.FriendCreated{
			UserId:     uId,
			FriendId:   fId,
			AcceptedAt: aAt,
		}

		err = c.stream.PublishEvent(&e)
	}

	if err != nil {
		log.Println(err)
	}

	return err
}
