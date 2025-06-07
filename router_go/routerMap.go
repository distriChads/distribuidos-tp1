package router

import (
	"strings"

	"github.com/spf13/viper"
)

type RoutingMap map[string][]string

func GetRoutingMap(v *viper.Viper) RoutingMap {
	inputRoutingKeys := getInputRoutingKeys(v)
	outputRoutingKeys := getOutputRoutingKeys(v)

	// The routing map defines how messages are routed from input to output exchanges.
	return RoutingMap{
		inputRoutingKeys["after_2000"]:       {outputRoutingKeys["join_movie_credits"], outputRoutingKeys["join_movie_ratings"]},
		inputRoutingKeys["credits"]:          {outputRoutingKeys["join_movie_credits"]},
		inputRoutingKeys["ratings"]:          {outputRoutingKeys["join_movie_ratings"]},
		inputRoutingKeys["ml"]:               {outputRoutingKeys["group_by_overview_average"]},
		inputRoutingKeys["join_credits"]:     {outputRoutingKeys["group_by_actor_count"]},
		inputRoutingKeys["join_ratings"]:     {outputRoutingKeys["group_by_movie_average"]},
		inputRoutingKeys["only_one_country"]: {outputRoutingKeys["group_by_country_sum"]},
	}
}

func getInputRoutingKeys(v *viper.Viper) map[string]string {
	keys := strings.Split(v.GetString("worker.exchange.input.routingkeys"), ",")

	// Input routing keys are expected to be in a specific order but could change its name
	return map[string]string{
		"after_2000":       strings.TrimSpace(keys[0]),
		"ratings":          strings.TrimSpace(keys[1]),
		"credits":          strings.TrimSpace(keys[2]),
		"only_one_country": strings.TrimSpace(keys[3]),
		"ml":               strings.TrimSpace(keys[4]),
		"join_ratings":     strings.TrimSpace(keys[5]),
		"join_credits":     strings.TrimSpace(keys[6]),
	}
}

func getOutputRoutingKeys(v *viper.Viper) map[string]string {
	return map[string]string{
		"join_movie_credits":        v.GetString("worker.output.routingkeys.join-movie-credits"),
		"join_movie_ratings":        v.GetString("worker.output.routingkeys.join-movie-ratings"),
		"group_by_overview_average": v.GetString("worker.output.routingkeys.group-by-overview-average"),
		"group_by_actor_count":      v.GetString("worker.output.routingkeys.group-by-actor-count"),
		"group_by_movie_average":    v.GetString("worker.output.routingkeys.group-by-movie-average"),
		"group_by_country_sum":      v.GetString("worker.output.routingkeys.group-by-country-sum"),
	}
}
