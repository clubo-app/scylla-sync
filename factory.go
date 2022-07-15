package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/clubo-app/scylla-sync-service/consumer"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type factory struct {
	logger scyllacdc.Logger
	fh     consumer.FriendRelationHandler
	ph     consumer.PartyFavoritesHandler
}

func newFactory(logger scyllacdc.Logger, fh consumer.FriendRelationHandler, ph consumer.PartyFavoritesHandler) factory {
	return factory{
		logger: logger,
		fh:     fh,
		ph:     ph,
	}
}

const reportPeriod = time.Minute

func (f *factory) CreateChangeConsumer(ctx context.Context, input scyllacdc.CreateChangeConsumerInput) (scyllacdc.ChangeConsumer, error) {
	reporter := scyllacdc.NewPeriodicProgressReporter(f.logger, time.Minute, input.ProgressReporter)
	reporter.Start(ctx)

	splitTableName := strings.SplitN(input.TableName, ".", 2)
	if len(splitTableName) < 2 {
		return nil, fmt.Errorf("table name is not fully qualified: %s", input.TableName)
	}

	return &consumer.Consumer{
		TableName:     splitTableName[1],
		Reporter:      scyllacdc.NewPeriodicProgressReporter(f.logger, reportPeriod, input.ProgressReporter),
		FriendHandler: f.fh,
		PartyHandler:  f.ph,
	}, nil
}
