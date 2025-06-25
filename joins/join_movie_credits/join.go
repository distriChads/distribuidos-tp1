package join_movie_credits

import (
	"context"
	worker "distribuidos-tp1/common/worker/worker"
	"distribuidos-tp1/joins/common_join"
	"strconv"
	"strings"
)

type JoinMovieCreditsByIdConfig struct {
	worker.WorkerConfig
}

type JoinMovieCreditsById struct {
	*common_join.CommonJoin
}

// ---------------------------------
// MESSAGE FORMAT: ID|TITLE
// ---------------------------------
const ID = 0
const ACTORS = 1

func storeMovieWithId(line string, movies_by_id map[string]string) {
	parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
	movies_by_id[parts[ID]] = parts[ID]
}

// ---------------------------------
// MESSAGE FORMAT: MOVIE_ID|ACTORS
// ---------------------------------

func (f *JoinMovieCreditsById) joinMovieWithCredits(lines []string, movies_by_id map[string]string) {
	for _, line := range lines {
		parts := strings.Split(line, worker.MESSAGE_SEPARATOR)
		movie_id := movies_by_id[parts[ID]]

		if len(movie_id) == 0 {
			continue
		}

		data := movie_id + worker.MESSAGE_SEPARATOR + parts[ACTORS]
		id, err := strconv.Atoi(parts[ID])
		if err != nil {
			continue
		}
		f.Buffer.AddMessage(id, data)
	}
}

func NewJoinMovieCreditsById(config JoinMovieCreditsByIdConfig, storageBaseDir string, eofCounter int) *JoinMovieCreditsById {
	join := common_join.NewCommonJoin(config.WorkerConfig, storageBaseDir, eofCounter)
	return &JoinMovieCreditsById{
		CommonJoin: join,
	}
}

func (f *JoinMovieCreditsById) RunWorker(ctx context.Context, starting_message string) error {
	return f.CommonJoin.RunWorker(ctx, starting_message, f.joinMovieWithCredits, storeMovieWithId)
}
