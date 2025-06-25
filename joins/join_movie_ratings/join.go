package join_movie_ratings

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/joins/common_join"
	"strconv"
	"strings"
)

type JoinMovieRatingByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieRatingById struct {
	*common_join.CommonJoin
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE
// ---------------------------------
const ID = 0
const SCORE = 1

func storeMovieWithId(line string, movies_by_id map[string]string) {
	parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
	movies_by_id[parts[ID]] = line

}

// ---------------------------------
// MESSAGE FORMAT: MOVIE_ID|SCORE
// ---------------------------------

// get the movie_id, if there is nothing to join, continues
// if there is data, add the message to the hasher buffer
func (f *JoinMovieRatingById) joinMovieWithRating(lines []string, movies_by_id map[string]string) {

	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_data := movies_by_id[parts[ID]]
		if movie_data == "" {
			continue
		}
		id, err := strconv.Atoi(parts[ID])
		if err != nil {
			continue
		}
		f.Buffer.AddMessage(id, movie_data+worker.MESSAGE_SEPARATOR+parts[SCORE])
	}
}

func NewJoinMovieRatingById(config JoinMovieRatingByIdConfig, storage_base_dir string, eofCounter int) *JoinMovieRatingById {
	join := common_join.NewCommonJoin(config.WorkerConfig, storage_base_dir, eofCounter)
	return &JoinMovieRatingById{
		CommonJoin: join,
	}
}

func (f *JoinMovieRatingById) RunWorker(ctx context.Context, starting_message string) error {
	return f.CommonJoin.RunWorker(ctx, starting_message, f.joinMovieWithRating, storeMovieWithId)
}
