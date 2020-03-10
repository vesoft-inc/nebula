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
    auto iRet = indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }
    CHECK_NOTNULL(kvstore_);
    if (indexes_.empty()) {
        std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges) {
            auto partId = partEdges.first;
            std::vector<kvstore::KV> data;
            std::for_each(partEdges.second.begin(), partEdges.second.end(), [&](auto& edge) {
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
        std::for_each(req.parts.begin(), req.parts.end(), [&](auto& partEdges) {
            auto partId = partEdges.first;
            auto atomic = [version, partId, edges = std::move(partEdges.second), this]()
                          -> std::string {
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

    std::for_each(edges.begin(), edges.end(), [&](auto& edge) {
        auto prop = edge.get_props();
        auto type = edge.key.edge_type;
        auto srcId = edge.key.src;
        auto rank = edge.key.ranking;
        auto dstId = edge.key.dst;
        VLOG(3) << "PartitionID: " << partId << ", VertexID: " << srcId
                << ", EdgeType: " << type << ", EdgeRanking: " << rank
                << ", VertexID: " << dstId << ", EdgeVersion: " << version;
        auto key = NebulaKeyUtils::edgeKey(partId, srcId, type, rank, dstId, version);

        std::string val;
        std::unique_ptr<RowReader> nReader;
        for (auto& index : indexes_) {
            if (type != index->get_schema_id().get_edge_type()) {
                continue;
            }

            if (val.empty()) {
                val = findObsoleteIndex(partId, srcId, type, rank, dstId);
            }
            if (!val.empty()) {
                auto reader = RowReader::getEdgePropReader(schemaMan_,
                                                           val,
                                                           spaceId_,
                                                           type);
                auto oi = indexKey(partId, reader.get(), key, index);
                if (!oi.empty()) {
                    batchHolder->remove(std::move(oi));
                }
            }
            if (nReader == nullptr) {
                nReader = RowReader::getEdgePropReader(schemaMan_,
                                                       prop,
                                                       spaceId_,
                                                       type);
            }
            auto ni = indexKey(partId, nReader.get(), key, index);
            batchHolder->put(std::move(ni), "");
        }
        batchHolder->put(std::move(key), std::move(prop));
    });
    return encodeBatchValue(batchHolder->getBatch());
}

std::string AddEdgesProcessor::findObsoleteIndex(PartitionID partId,
                                                 VertexID srcId,
                                                 EdgeType type,
                                                 EdgeRanking rank,
                                                 VertexID dstId) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId, srcId, type, rank, dstId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret)
                   << ", spaceId " << this->spaceId_;
        return "";
    }
    if (iter && iter->valid()) {
        return iter->val().str();
    }
    return "";
}

std::string AddEdgesProcessor::indexKey(PartitionID partId,
                                        RowReader* reader,
                                        const folly::StringPiece& rawKey,
                                        std::shared_ptr<nebula::cpp2::IndexItem> index) {
    auto values = collectIndexValues(reader, index->get_fields());
    return NebulaKeyUtils::edgeIndexKey(partId,
                                        index->get_index_id(),
                                        NebulaKeyUtils::getSrcId(rawKey),
                                        NebulaKeyUtils::getRank(rawKey),
                                        NebulaKeyUtils::getDstId(rawKey),
                                        values);
}

}  // namespace storage
}  // namespace nebula
