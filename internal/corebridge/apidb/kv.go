package apidb

import (
	"encoding/json"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

var ErrNotFound = errors.New("apidb: not found")

func (d *DB) view(fn func(txn *badger.Txn) error) error {
	return d.db.View(fn)
}

func (d *DB) update(fn func(txn *badger.Txn) error) error {
	return d.db.Update(fn)
}

func getJSON(txn *badger.Txn, key []byte, dest any) error {
	item, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		return err
	}
	return item.Value(func(val []byte) error {
		return json.Unmarshal(val, dest)
	})
}

func putJSON(txn *badger.Txn, key []byte, val any) error {
	raw, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return txn.Set(key, raw)
}

func deleteKey(txn *badger.Txn, key []byte) error {
	return txn.Delete(key)
}

func deletePrefix(txn *badger.Txn, prefix string) error {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	p := []byte(prefix)
	for it.Seek(p); it.ValidForPrefix(p); it.Next() {
		if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
			return err
		}
	}
	return nil
}

func listPrefixJSON[T any](txn *badger.Txn, prefix string, skipIndex func([]byte) bool) ([]T, error) {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	p := []byte(prefix)
	var out []T
	for it.Seek(p); it.ValidForPrefix(p); it.Next() {
		k := it.Item().KeyCopy(nil)
		if skipIndex != nil && skipIndex(k) {
			continue
		}
		if isIndexKey(k) {
			continue
		}
		var rec T
		if err := it.Item().Value(func(val []byte) error {
			return json.Unmarshal(val, &rec)
		}); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func countPrefix(txn *badger.Txn, prefix string) (int, error) {
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	p := []byte(prefix)
	n := 0
	for it.Seek(p); it.ValidForPrefix(p); it.Next() {
		k := it.Item().KeyCopy(nil)
		if isIndexKey(k) {
			continue
		}
		n++
	}
	return n, nil
}

func getString(txn *badger.Txn, key []byte) (string, error) {
	item, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return "", ErrNotFound
		}
		return "", err
	}
	var s string
	err = item.Value(func(val []byte) error {
		return json.Unmarshal(val, &s)
	})
	return s, err
}

func putString(txn *badger.Txn, key []byte, val string) error {
	raw, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return txn.Set(key, raw)
}

func getBytes(txn *badger.Txn, key []byte) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return item.ValueCopy(nil)
}

func putBytes(txn *badger.Txn, key []byte, val []byte) error {
	return txn.Set(key, val)
}

func lookupID(txn *badger.Txn, indexKey []byte) (string, error) {
	idBytes, err := getBytes(txn, indexKey)
	if err != nil {
		return "", err
	}
	return string(idBytes), nil
}

func setIndex(txn *badger.Txn, indexKey []byte, id string) error {
	return txn.Set(indexKey, []byte(id))
}

func clearIndex(txn *badger.Txn, indexKey []byte) error {
	err := txn.Delete(indexKey)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil
	}
	return err
}

func initSchema(txn *badger.Txn) error {
	_, err := txn.Get([]byte(keyMetaSchemaVer))
	if err == nil {
		return nil
	}
	if !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	}
	raw, err := json.Marshal(schemaVersion)
	if err != nil {
		return err
	}
	return txn.Set([]byte(keyMetaSchemaVer), raw)
}

func scanErrWrap(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) {
		return err
	}
	return fmt.Errorf("%s: %w", op, err)
}
