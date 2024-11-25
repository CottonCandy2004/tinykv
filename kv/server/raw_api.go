package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/Connor1996/badger"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	value, err := reader.GetCF(req.Cf, req.Key)
	resp := &kvrpcpb.RawGetResponse{}
	if err != nil {
        // 如果发生错误，返回错误信息
        resp.Error = err.Error()
    } else if value == nil {
        // 如果值为 nil，则说明键不存在，设置 NotFound 为 true
        resp.NotFound = true
	} else {
		resp.Value = value
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
    	modify := storage.Modify{
		Data: storage.Put{
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
    modify := storage.Modify{
		Data: storage.Delete{
			Cf:  req.Cf,
			Key: req.Key,
		},
	}
	err := server.storage.Write(req.Context, []storage.Modify{modify})
	// 如果删除操作失败，并且错误是 Key Not Found，则返回空的响应
	if err != nil {
		if err == badger.ErrKeyNotFound {
			// 如果 key 不存在，返回成功响应（不报错）
			return &kvrpcpb.RawDeleteResponse{}, nil
		}
		// 其他错误，返回错误信息
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, nil
	}
	return &kvrpcpb.RawDeleteResponse{}, nil	
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	it := reader.IterCF(req.Cf)
	defer it.Close()

	var kvs []*kvrpcpb.KvPair
	for it.Seek(req.StartKey); it.Valid() && len(kvs) < int(req.Limit); it.Next() {
		item := it.Item()

		// Use ValueCopy to safely retrieve the value.
		value, err := item.ValueCopy(nil)
		if err != nil {
			return &kvrpcpb.RawScanResponse{
				Error: err.Error(),
			}, nil
		}

		// Append the key-value pair to the result.
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
	}

	// Return the scan results.
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
