package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type JsonStore struct {
	folder string
}

func NewJsonStore(folder string) JsonStore {
	return JsonStore{folder: folder}
}

func (store JsonStore) path(filename string) string {
	return filepath.Join(store.folder, filename)
}

func (store JsonStore) readFile(filename string) ([]byte, error) {
	return ioutil.ReadFile(store.path(filename))
}

func (store JsonStore) StoreKV(filename string, kv []KeyValue) error {
	f, err := os.Create(store.path(filename))
	if err != nil {
		return fmt.Errorf("fail to create file, %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := json.NewEncoder(f).Encode(&kv); err != nil {
		return err
	}
	return nil
}

func (store JsonStore) RetrieveKV(filename string) ([]KeyValue, error) {
	var ret []KeyValue

	f, err := os.Open(store.path(filename))
	if err != nil {
		return nil, fmt.Errorf("fail to open file, %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := json.NewDecoder(f).Decode(&ret); err != nil {
		return nil, err
	}
	return ret, nil
}

type ShuffleResult struct {
	Key    string
	Values []string
}

func (store JsonStore) StoreShufflingBatch(filename string, result []*ShuffleResult) error {
	f, err := os.Create(store.path(filename))
	if err != nil {
		return fmt.Errorf("fail to open file, %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := json.NewEncoder(f).Encode(result); err != nil {
		return err
	}
	return nil
}

func (store JsonStore) RetrieveShufflingBatch(filename string) ([]*ShuffleResult, error) {
	var ret []*ShuffleResult

	f, err := os.Open(store.path(filename))
	if err != nil {
		return nil, fmt.Errorf("fail to open file, %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := json.NewDecoder(f).Decode(&ret); err != nil {
		return nil, err
	}
	return ret, nil
}
