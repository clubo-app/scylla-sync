package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/clubo-app/packages/stream"
	"github.com/clubo-app/scylla-sync-service/config"
	"github.com/nats-io/nats.go"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
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

	sess, err := NewCluster(c.CQL_KEYSPACE, c.CQL_HOSTS)
	if err != nil {
		log.Fatalln(err)
	}

	cfg := &scyllacdc.ReaderConfig{
		Session:               sess.Session,
		TableNames:            []string{c.CQL_KEYSPACE + ".friend_relation", c.CQL_KEYSPACE + ".party_favorites"},
		ChangeConsumerFactory: &myFactory{},
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
