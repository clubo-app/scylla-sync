package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/clubo-app/packages/stream"
	"github.com/clubo-app/scylla-sync-service/consumer"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type factory struct {
	logger scyllacdc.Logger
	stream stream.Stream
	nextID int
}

func newFactory(logger scyllacdc.Logger, stream stream.Stream) *factory {
	return &factory{
		logger: logger,
		stream: stream,
	}
}

const reportPeriod = 1 * time.Minute

func (f *factory) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
	f.nextID++

	reporter := scyllacdc.NewPeriodicProgressReporter(f.logger, time.Minute, input.ProgressReporter)
	reporter.Start(ctx)

	splitTableName := strings.SplitN(input.TableName, ".", 2)
	if len(splitTableName) < 2 {
		return nil, fmt.Errorf("table name is not fully qualified: %s", input.TableName)
	}

	if splitTableName[1] == consumer.FRIEND_RELATIONS_TABLE {
		return &consumer.FriendRelationConsumer{
			Id:        f.nextID - 1,
			TableName: splitTableName[1],
			Stream:    f.stream,
			Reporter:  reporter,
		}, nil
	} else if splitTableName[1] == consumer.FAVORITE_PARTIES_TABLE {
		return &consumer.FavoritePartiesConsumer{
			Id:        f.nextID - 1,
			TableName: splitTableName[1],
			Stream:    f.stream,
			Reporter:  reporter,
		}, nil
	}
	return nil, errors.New("Unsupported Table")
}
