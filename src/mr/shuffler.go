package mr

import (
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

type Shuffler interface {
	Shuffle(inputs []string) ([]string, error)
}

const (
	shuffleFilePrefix = "shuffle"
	maxShuffleItems   = 2000
)

type InMemoryShuffler struct {
	cnt int

	storer  JsonStore
	results map[string]*ShuffleResult
	logger  *zap.Logger
}

func NewInMemoryShuffler(folder string) (*InMemoryShuffler, error) {
	shuffler := &InMemoryShuffler{
		results: make(map[string]*ShuffleResult),
		storer:  NewJsonStore(folder),
	}
	if err := shuffler.initLogger(); err != nil {
		return nil, fmt.Errorf("fail to init logger, %w", err)
	}
	return shuffler, nil
}

func (ims *InMemoryShuffler) initLogger() error {
	logger, err := GetBaseLogger()
	if err != nil {
		return err
	}
	ims.logger = logger.With(
		zap.String(LoggerComponent, "inMemoryShuffler"),
	)
	return nil
}

func (ims *InMemoryShuffler) Shuffle(inputs []string) ([]string, error) {
	ims.logger.Info("start shuffling", zap.Int("len", len(inputs)))
	ims.logger.Debug("shuffle inputs", zap.String("input", strings.Join(inputs, ",")))

	for _, input := range inputs {
		if err := ims.shuffle(input); err != nil {
			return nil, fmt.Errorf("fail to shuffle %s, err=%w", input, err)
		}
	}

	ims.logger.Info("shuffle success, store now", zap.Int("len", len(ims.results)))
	return ims.storeShuffleBatch()
}

func (ims *InMemoryShuffler) newOutputPath() string {
	ims.cnt++
	output := fmt.Sprintf("%s-%d-%d.json", shuffleFilePrefix, time.Now().Unix(), ims.cnt)
	return output
}

func (ims *InMemoryShuffler) shuffle(input string) error {
	kvs, err := ims.storer.RetrieveKV(input)
	if err != nil {
		return fmt.Errorf("fail to retrieve kv from %s, err=%w", input, err)
	}

	for _, kv := range kvs {
		result, ok := ims.results[kv.Key]
		if !ok {
			result = &ShuffleResult{
				Key: kv.Key,
			}
			ims.results[kv.Key] = result
		}
		result.Values = append(result.Values, kv.Value)
	}
	return nil
}

func (ims *InMemoryShuffler) storeShuffleBatch() ([]string, error) {
	var (
		batch   []*ShuffleResult
		output  string
		outputs []string
	)

	for _, res := range ims.results {
		if output == "" {
			output = ims.newOutputPath()
			outputs = append(outputs, output)
		}
		if len(batch) >= maxShuffleItems {
			ims.logger.Debug(
				"store shuffle",
				zap.String("key", res.Key),
				zap.String("output", output),
			)
			if err := ims.storer.StoreShufflingBatch(output, batch); err != nil {
				return nil, fmt.Errorf("fail to store shuffling, %w", err)
			}
			output = ""
			batch = batch[:0]
		}
		batch = append(batch, res)
	}

	ims.logger.Info("store successfully")
	return outputs, nil
}
