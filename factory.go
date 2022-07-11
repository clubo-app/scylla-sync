package main

import (
	"context"
	"time"

	"github.com/clubo-app/scylla-sync-service/consumer"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type myFactory struct {
	logger scyllacdc.Logger
}

func (f *myFactory) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
	reporter := scyllacdc.NewPeriodicProgressReporter(f.logger, time.Minute, input.ProgressReporter)
	reporter.Start(ctx)
	if input.TableName == FRIEND_RELATIONS {
		return &consumer.FriendRelationConsumer{}, nil
	} else {
		return &consumer.PartyFavoritesConsumer{}, nil
	}
}
