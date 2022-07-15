package consumer

import (
	"log"
	"reflect"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	FRIEND_RELATIONS_TABLE = "friend_relations"
	PARTY_FAVORITES_TABLE  = "party_favorites"
)

func ParseString(i any) string {
	log.Println(reflect.TypeOf(i))
	if i != nil {
		return *i.(*string)
	}
	return ""
}

func ParseTimestamp(i any) *timestamppb.Timestamp {
	if !reflect.ValueOf(i).IsNil() {
		time := i.(*time.Time)
		if time.IsZero() {
			return nil
		}

		stamp := timestamppb.New(*time)
		return stamp
	}

	return nil
}
