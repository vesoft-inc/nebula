/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef STORAGE_SCANINDEXBASEPROCESSOR_H
#define STORAGE_SCANINDEXBASEPROCESSOR_H

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "stats/Stats.h"

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
class LookUpIndexBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~LookUpIndexBaseProcessor() = default;

protected:
    explicit LookUpIndexBaseProcessor(kvstore::KVStore* kvstore,
                                    meta::SchemaManager* schemaMan,
                                    stats::Stats* stats,
                                    folly::Executor* executor = nullptr,
                                    VertexCache* cache = nullptr)
            : BaseProcessor<RESP>(kvstore, schemaMan, stats)
            , executor_(executor)
            , vertexCache_(cache) {}

    void putResultCodes(cpp2::ErrorCode code, const std::vector<PartitionID>& parts) {
        for (auto& p : parts) {
            this->pushResultCode(code, p);
        }
        this->onFinished();
    }

    cpp2::ErrorCode prepareLookUp(const REQ& req, bool isVertex);

    cpp2::ErrorCode prepareExpr(const REQ& req);

protected:
    GraphSpaceID                           spaceId_;
    int32_t                                tagOrEdge_;
    nebula::cpp2::IndexItem                index_;
    std::unique_ptr<Expression>            exp_;
    bool                                   returnColsNeed_{false};
    std::string                            prefix_;
    std::pair<std::string, std::string>    range_;

    std::shared_ptr<SchemaWriter>          schema_{nullptr};
    folly::Executor*                       executor_{nullptr};
    VertexCache*                           vertexCache_{nullptr};
    std::vector<cpp2::VertexIndexData>     vertexRows_;
    std::vector<cpp2::Edge>                edgeRows_;
};

}  // namespace storage
}  // namespace nebula

#include "LookUpIndexBaseProcessor.inl"


#endif  // STORAGE_SCANINDEXBASEPROCESSOR_H

