package consumer

import (
	"context"

	"github.com/clubo-app/packages/stream"
	scyllacdc "github.com/scylladb/scylla-cdc-go"
)

type PartyFavoritesHandler struct {
	stream stream.Stream
}

func NewPartyFavoritesHandler(st stream.Stream) PartyFavoritesHandler {
	return PartyFavoritesHandler{stream: st}
}

func (c *PartyFavoritesHandler) Consume(ctx context.Context, ch scyllacdc.Change) error {
	return nil
}
