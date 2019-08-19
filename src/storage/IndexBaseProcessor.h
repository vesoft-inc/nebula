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
    void doIndexCreate(PartitionID partId);

    cpp2::ErrorCode pastIndexCreate(PartitionID partId, const folly::StringPiece &raw);

    cpp2::ErrorCode doBatchPut(GraphSpaceID spaceId, PartitionID partId,
                               std::vector<kvstore::KV> data);

    cpp2::ErrorCode doDelete(GraphSpaceID spaceId, PartitionID partId,
                             const std::string &key);

    void  doBatchDelete(GraphSpaceID spaceId, PartitionID partId, std::string prefix);

    StatusOr<std::string> assembleEdgeIndexKey(GraphSpaceID spaceId, PartitionID partId,
                                               std::string key, std::string val);

    StatusOr<std::string> assembleVertexIndexKey(GraphSpaceID spaceId, PartitionID partId,
                                                 std::string key, std::string val);

    StatusOr<std::vector<kvstore::KV>> parseIndexKey(PartitionID partId,
            nebula::cpp2::IndexType type, const std::pair<std::string, std::string> data);

    cpp2::ErrorCode pastDoInsert(PartitionID partId, nebula::cpp2::IndexType type,
                                 const std::pair<std::string, std::string> data);

    cpp2::ErrorCode pastDoDelete(PartitionID partId, nebula::cpp2::IndexType type,
                                 const std::pair<std::string, std::string> data);

    cpp2::ErrorCode pastDoUpdate(PartitionID partId, nebula::cpp2::IndexType type,
                                 const std::pair<std::string, std::string> oldData,
                                 const std::pair<std::string, std::string> newData);

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
