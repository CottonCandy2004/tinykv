package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
    db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
    opts := badger.DefaultOptions
    opts.Dir = conf.DBPath
    opts.ValueDir = conf.DBPath

    db, err := badger.Open(opts)
    if err != nil {
        panic(err)
    }
    return &StandAloneStorage{db: db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

type StandAloneReader struct {
	txn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
    // 使用 engine_util 封装的 GetCFFromTxn 函数读取数据
    value, err := engine_util.GetCFFromTxn(r.txn, cf, key)
    // 如果是 Key not found 错误，返回 nil 而不是错误
    if err == badger.ErrKeyNotFound {
        return nil, nil // 键不存在时返回 nil
    }
    return value, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	// 使用 engine_util 封装的 NewCFIterator 函数创建迭代器
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneReader) Close() {
	// 关闭事务
	r.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
    txn := s.db.NewTransaction(false)
    return &StandAloneReader{txn: txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// 使用 Badger 写事务进行批量写入
	txn := s.db.NewTransaction(true) // 可写事务
	defer txn.Discard()

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.db, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			del := m.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.db, del.Cf, del.Key)
			if err != nil {
				if err == badger.ErrKeyNotFound {
					continue
				}
				return err
			}
		}
	}
	return txn.Commit()
}
