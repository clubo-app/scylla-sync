package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/clubo-app/packages/stream"
	"github.com/clubo-app/scylla-sync-service/config"
	"github.com/clubo-app/scylla-sync-service/consumer"
	"github.com/nats-io/nats.go"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

const (
	FRIEND_RELATIONS = "friend_relations"
	PARTY_FAVORITES  = "party_favorites"
)

func main() {
	c, err := config.LoadConfig()
	if err != nil {
		log.Fatalln(err)
	}

	opts := []nats.Option{nats.Name("Scylla-Sync Service")}
	nc, err := stream.Connect(c.NATS_CLUSTER, opts)
	if err != nil {
		log.Fatalln(err)
	}
	defer nc.Close()

	stream := stream.New(nc)

	sess, err := newCluster(c.CQL_KEYSPACE, c.CQL_HOSTS)
	if err != nil {
		log.Fatalln(err)
	}

	logger := log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.Lshortfile)

	fh := consumer.NewFriendRelationHandler(stream)
	ph := consumer.NewPartyFavoritesHandler(stream)

	factory := newFactory(logger, fh, ph)

	cfg := &scyllacdc.ReaderConfig{
		Session:               sess.Session,
		TableNames:            []string{c.CQL_KEYSPACE + "." + FRIEND_RELATIONS, c.CQL_KEYSPACE + "." + PARTY_FAVORITES},
		ChangeConsumerFactory: &factory,
		Logger:                logger,
		Advanced: scyllacdc.AdvancedReaderConfig{
			PostEmptyQueryDelay:    time.Second * 1,
			PostNonEmptyQueryDelay: time.Second * 1,
			PostFailedQueryDelay:   time.Second * 1,
			ConfidenceWindowSize:   time.Second * 1,
			QueryTimeWindowSize:    time.Second * 10,
			ChangeAgeLimit:         time.Second * 10,
		},
	}

	reader, err := scyllacdc.NewReader(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	signalC := make(chan os.Signal)
	go func() {
		<-signalC
		reader.Stop()

		<-signalC
		os.Exit(1)
	}()
	signal.Notify(signalC, os.Interrupt)

	if err := reader.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
