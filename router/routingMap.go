package router

import (
	"github.com/spf13/viper"
)

type RoutingMap map[int][]string

const (
	// inputs
	MOVIE_AFTER_2000 = iota
	RATINGS
	CREDITS
	MOVIE_FILTER_ONLY_ONE
	ML
	JOIN_RATINGS
	JOIN_CREDITS

	// outputs
	OUTPUT_JOIN_MOVIE_CREDITS        = "join_movie_credits"
	OUTPUT_JOIN_MOVIE_RATINGS        = "join_movie_ratings"
	OUTPUT_GROUP_BY_OVERVIEW_AVERAGE = "group_by_overview_average"
	OUTPUT_GROUP_BY_ACTOR_COUNT      = "group_by_actor_count"
	OUTPUT_GROUP_BY_MOVIE_AVERAGE    = "group_by_movie_average"
	OUTPUT_GROUP_BY_COUNTRY_SUM      = "group_by_country_sum"
)

func GetRoutingMap(v *viper.Viper) RoutingMap {
	outputRoutingKeys := getOutputRoutingKeys(v)

	// The routing map defines how messages are routed from input to output exchanges.
	return RoutingMap{
		MOVIE_AFTER_2000:      {outputRoutingKeys[OUTPUT_JOIN_MOVIE_CREDITS], outputRoutingKeys[OUTPUT_JOIN_MOVIE_RATINGS]},
		CREDITS:               {outputRoutingKeys[OUTPUT_JOIN_MOVIE_CREDITS]},
		RATINGS:               {outputRoutingKeys[OUTPUT_JOIN_MOVIE_RATINGS]},
		ML:                    {outputRoutingKeys[OUTPUT_GROUP_BY_OVERVIEW_AVERAGE]},
		JOIN_CREDITS:          {outputRoutingKeys[OUTPUT_GROUP_BY_ACTOR_COUNT]},
		JOIN_RATINGS:          {outputRoutingKeys[OUTPUT_GROUP_BY_MOVIE_AVERAGE]},
		MOVIE_FILTER_ONLY_ONE: {outputRoutingKeys[OUTPUT_GROUP_BY_COUNTRY_SUM]},
	}
}

func getOutputRoutingKeys(v *viper.Viper) map[string]string {
	return map[string]string{
		OUTPUT_JOIN_MOVIE_CREDITS:        v.GetString("worker.output.routingkeys.join-movie-credits"),
		OUTPUT_JOIN_MOVIE_RATINGS:        v.GetString("worker.output.routingkeys.join-movie-ratings"),
		OUTPUT_GROUP_BY_OVERVIEW_AVERAGE: v.GetString("worker.output.routingkeys.group-by-overview-average"),
		OUTPUT_GROUP_BY_ACTOR_COUNT:      v.GetString("worker.output.routingkeys.group-by-actor-count"),
		OUTPUT_GROUP_BY_MOVIE_AVERAGE:    v.GetString("worker.output.routingkeys.group-by-movie-average"),
		OUTPUT_GROUP_BY_COUNTRY_SUM:      v.GetString("worker.output.routingkeys.group-by-country-sum"),
	}
}
