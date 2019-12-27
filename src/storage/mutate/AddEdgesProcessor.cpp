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
                VLOG(3) << "PartitionID: " << partId << ", VertexID: " << edge.key.src
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

    /*
     * Define the map newIndexes to avoid inserting duplicate edge.
     * This map means :
     * map<edge_unique_key, pair<edge_prop, IndexItem>> ,
     * -- edge_unique_key is only used as the unique key , for example:
     * insert below edges in the same request:
     *     kv(part1_src1_edgeType1_rank1_dst1 , v1)
     *     kv(part1_src1_edgeType1_rank1_dst1 , v2)
     *     kv(part1_src1_edgeType1_rank1_dst1 , v3)
     *     kv(part1_src1_edgeType1_rank1_dst1 , v4)
     *
     * Ultimately, kv(part1_src1_edgeType1_rank1_dst1 , v4) . It's just what I need.
     */
    std::map<std::string, std::pair<std::string, nebula::cpp2::IndexItem>> newIndexes;
    std::for_each(edges.begin(), edges.end(), [&](auto& edge){
        auto prop = edge.get_props();
        auto type = edge.key.edge_type;
        auto srcId = edge.key.src;
        auto rank = edge.key.ranking;
        auto dstId = edge.key.dst;
        VLOG(3) << "PartitionID: " << partId << ", VertexID: " << srcId
                << ", EdgeType: " << type << ", EdgeRanking: " << rank
                << ", VertexID: " << dstId << ", EdgeVersion: " << version;
        for (auto& index : indexes_) {
            /**
             * skip the in-edge.
             **/
            if (index.get_tagOrEdge() == type) {
                auto ekVal = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, version);
                newIndexes[ekVal] = std::make_pair(edge.get_props(), index);
            }
        }
    });
    for (auto& index : newIndexes) {
        /*
         * step 1 , Delete old version index if exists.
         */
        auto oi = obsoleteIndex(partId, index.first, index.second.second);
        if (!oi.empty()) {
            batchHolder->remove(std::move(oi));
        }
        /*
         * step 2 , Insert new edge data
         */
        auto key = index.first;
        auto prop = index.second.first;
        batchHolder->put(std::move(key), std::move(prop));
        /*
         * step 3 , Insert new edge index
         */
        auto ni = newIndex(partId, index.first, index.second);
        batchHolder->put(std::move(ni), "");
    }

    return encodeBatchValue(batchHolder->getBatch());
}

std::string AddEdgesProcessor::obsoleteIndex(PartitionID partId,
                                             const folly::StringPiece& rawKey,
                                             const nebula::cpp2::IndexItem& index) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId,
                                             NebulaKeyUtils::getSrcId(rawKey),
                                             NebulaKeyUtils::getEdgeType(rawKey),
                                             NebulaKeyUtils::getRank(rawKey),
                                             NebulaKeyUtils::getDstId(rawKey));
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
                                                  index.get_tagOrEdge());
        const auto& cols = index.get_cols();
        auto values = collectIndexValues(reader.get(), cols);
        auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                     index.get_index_id(),
                                                     NebulaKeyUtils::getSrcId(rawKey),
                                                     NebulaKeyUtils::getRank(rawKey),
                                                     NebulaKeyUtils::getDstId(rawKey),
                                                     values);
        return indexKey;
    }
    return "";
}

std::string AddEdgesProcessor::newIndex(PartitionID partId,
                                        const folly::StringPiece& rawKey,
                                        const std::pair<std::string,
                                                        nebula::cpp2::IndexItem>& index) {
    auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                               index.first,
                                               spaceId_,
                                               index.second.get_tagOrEdge());
    auto values = collectIndexValues(reader.get(), index.second.get_cols());
    return NebulaKeyUtils::edgeIndexKey(partId,
                                        index.second.get_index_id(),
                                        NebulaKeyUtils::getSrcId(rawKey),
                                        NebulaKeyUtils::getRank(rawKey),
                                        NebulaKeyUtils::getDstId(rawKey),
                                        values);
}

}  // namespace storage
}  // namespace nebula
