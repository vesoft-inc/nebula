/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/mutate/AddEdgesProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"

namespace nebula {
namespace storage {

void AddEdgesProcessor::process(const cpp2::AddEdgesRequest& req) {
    spaceId_ = req.get_space_id();
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    callingNum_ = req.parts.size();
    auto iRet = schemaMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        for (auto& index : iRet.value()) {
            indexes_.emplace_back(index);
        }
    }
    CHECK_NOTNULL(kvstore_);
    if (indexes_.empty()) {
        std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges){
            auto partId = partEdges.first;
            std::vector<kvstore::KV> data;
            std::for_each(partEdges.second.begin(), partEdges.second.end(), [&](auto& edge){
                VLOG(4) << "PartitionID: " << partId << ", VertexID: " << edge.key.src
                        << ", EdgeType: " << edge.key.edge_type << ", EdgeRanking: "
                        << edge.key.ranking << ", VertexID: "
                        << edge.key.dst << ", EdgeVersion: " << version;
                auto key = NebulaKeyUtils::edgeKey(partId, edge.key.src, edge.key.edge_type,
                                                   edge.key.ranking, edge.key.dst, version);
                data.emplace_back(std::move(key), std::move(edge.get_props()));
            });
            doPut(spaceId_, partId, std::move(data));
        });
    } else {
        std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges){
            auto partId = partEdges.first;
            const auto &edges = partEdges.second;
            auto atomic = [&]() -> std::string {
                return addEdges(version, partId, edges);
            };
            auto callback = [partId, this](kvstore::ResultCode code) {
                handleAsync(spaceId_, partId, code);
            };
            this->kvstore_->asyncAtomicOp(spaceId_, partId, atomic, callback);
        });
    }
}

std::string AddEdgesProcessor::addEdges(int64_t version, PartitionID partId,
                                        const std::vector<cpp2::Edge>& edges) {
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    std::map<std::string, std::pair<std::string,
             std::pair<cpp2::EdgeKey, cpp2::IndexItem>>> newIndexes;
    std::for_each(edges.begin(), edges.end(), [&](auto& edge){
        auto prop = edge.get_props();
        auto type = edge.key.edge_type;
        auto srcId = edge.key.src;
        auto rank = edge.key.ranking;
        auto dstId = edge.key.dst;
        VLOG(4) << "PartitionID: " << partId << ", VertexID: " << srcId
                << ", EdgeType: " << type << ", EdgeRanking: " << rank
                << ", VertexID: " << dstId << ", EdgeVersion: " << version;
        auto key = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, version);
        batchHolder->put(std::move(key), std::move(prop));
        for (auto& index : indexes_) {
            /**
             * skip the in-edge.
             **/
            if (index.get_schema() == type) {
                cpp2::EdgeKey ek(apache::thrift::FragileConstructor::FRAGILE,
                        srcId, type, rank, dstId);
                auto ekVal = NebulaKeyUtils::edgePrefix(partId, srcId, type, rank, dstId);
                newIndexes[ekVal] = std::make_pair(edge.get_props(), std::make_pair(ek, index));
            }
        }
    });
    for (auto& index : newIndexes) {
        auto ni = newIndex(partId, index.second);
        batchHolder->put(std::move(ni), "");
        auto oi = obsoleteIndex(partId, index.second.second);
        if (!oi.empty()) {
            batchHolder->remove(std::move(oi));
        }
    }

    return encodeBatchValue(batchHolder->getBatch());
}

std::string AddEdgesProcessor::obsoleteIndex(PartitionID partId,
        const std::pair<cpp2::EdgeKey, cpp2::IndexItem>& pair) {
    auto key = pair.first;
    auto index = pair.second;
    auto prefix = NebulaKeyUtils::edgePrefix(partId, key.src, key.edge_type, key.ranking, key.dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return "";
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                  iter->val(),
                                                  spaceId_,
                                                  key.edge_type);
        const auto& cols = index.get_cols();
        auto values = collectIndexValues(reader.get(), cols);
        auto indexKey = NebulaKeyUtils::edgeIndexKey(partId, index.get_index_id(),
                                                     key.src, key.ranking,
                                                     key.dst, values);
        std::string val;
        auto result = kvstore_->get(spaceId_, partId, indexKey, &val);
        if (result == kvstore::ResultCode::SUCCEEDED) {
            return indexKey;
        }
    }
    return "";
}

std::string AddEdgesProcessor::newIndex(PartitionID partId,
        const std::pair<std::string, std::pair<cpp2::EdgeKey, cpp2::IndexItem>>& index) {
    auto key = index.second.first;
    auto indexItem = index.second.second;
    auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                               index.first,
                                               spaceId_,
                                               key.edge_type);
    auto values = collectIndexValues(reader.get(), indexItem.get_cols());
    return NebulaKeyUtils::edgeIndexKey(partId, indexItem.get_index_id(),
                                        key.src, key.ranking,
                                        key.dst, values);
}

}  // namespace storage
}  // namespace nebula
