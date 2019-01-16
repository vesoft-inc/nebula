/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef KVSTORE_INCLUDE_KVSTORE_H_
#define KVSTORE_INCLUDE_KVSTORE_H_

#include "base/Base.h"
#include "kvstore/include/ResultCode.h"
#include "kvstore/include/Iterator.h"
#include <rocksdb/merge_operator.h>
#include <rocksdb/compaction_filter.h>

namespace nebula {
namespace kvstore {


using KVCallback = std::function<void(ResultCode code, HostAddr hostAddr)>;

struct KVOptions {
    /**
     * Local address, it would be used for search related meta information on meta server.
     * */
    HostAddr local_;
    /**
     *  Paths for data. It would be used by rocksdb engine.
     *  Be careful! We should ensure each "paths" has only one instance, otherwise
     *  it would mix up the data on disk.
     * */
    std::vector<std::string> dataPaths_;
    /**
     * Custom MergeOperator used in rocksdb.merge method.
     * */
    std::shared_ptr<rocksdb::MergeOperator> mergeOp_{nullptr};
    /**
     * Custom CompactionFilter used in compaction.
     * */
    std::shared_ptr<rocksdb::CompactionFilterFactory> cfFactory_{nullptr};
};


class KVStore {
public:
    /**
     * Create one new instance each time.
     * */
    static KVStore* instance(KVOptions options);

    virtual ~KVStore() = default;

    virtual ResultCode get(GraphSpaceID spaceId,
                           PartitionID  partId,
                           const std::string& key,
                           std::string* value) = 0;
    /**
     * Get all results in range [start, end)
     * */
    virtual ResultCode range(GraphSpaceID spaceId,
                             PartitionID  partId,
                             const std::string& start,
                             const std::string& end,
                             std::unique_ptr<StorageIter>* iter) = 0;

    /**
     * Get all results with prefix.
     * */
    virtual ResultCode prefix(GraphSpaceID spaceId,
                              PartitionID  partId,
                              const std::string& prefix,
                              std::unique_ptr<StorageIter>* iter) = 0;


    virtual ResultCode asyncMultiPut(GraphSpaceID spaceId,
                                     PartitionID  partId,
                                     std::vector<KV> keyValues,
                                     KVCallback cb) = 0;

protected:
    KVStore() = default;
};

}  // namespace kvstore
}  // namespace nebula
#endif  // KVSTORE_INCLUDE_KVSTORE_H_

