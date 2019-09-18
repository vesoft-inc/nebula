/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_STORAGE_INDEXBASEPROCESSOR_H
#define NEBULA_STORAGE_INDEXBASEPROCESSOR_H

#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

template<typename RESP>
class IndexBaseProcessor : public BaseProcessor<RESP>{
public:
    explicit IndexBaseProcessor(kvstore::KVStore* kvstore, meta::SchemaManager* schemaMan)
            : BaseProcessor<RESP>(kvstore, schemaMan) {}

    virtual ~IndexBaseProcessor() = default;

protected:
    void finishProcess(cpp2::ResultCode thriftResult);
    void doIndexCreate(PartitionID partId);

    cpp2::ErrorCode doBatchPut(GraphSpaceID spaceId, PartitionID partId,
                               std::vector<kvstore::KV> data);

    StatusOr<std::string> assembleEdgeIndexKey(GraphSpaceID spaceId, PartitionID partId,
                                               std::string key, std::string val);

    StatusOr<std::string> assembleVertexIndexKey(GraphSpaceID spaceId, PartitionID partId,
                                                 std::string key, std::string val);

    void pushPartsResultCode(std::vector<std::pair<cpp2::ErrorCode, PartitionID>> items) {
        for (auto item : items) {
            cpp2::ResultCode thriftRet;
            thriftRet.set_code(item.first);
            thriftRet.set_part_id(item.second);
            this->result_.failed_codes.emplace_back(std::move(thriftRet));
        }
    }

protected:
    GraphSpaceID                     spaceId_;
    nebula::cpp2::IndexType          indexType_;
    nebula::cpp2::IndexID            indexId_;
    std::map<int32_t, std::vector<std::string>> props_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/IndexBaseProcessor.inl"

#endif  // NEBULA_STORAGE_INDEXBASEPROCESSOR_H
