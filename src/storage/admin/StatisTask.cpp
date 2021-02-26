/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/MurmurHash2.h"
#include "kvstore/Common.h"
#include "storage/admin/StatisTask.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

cpp2::ErrorCode
StatisTask::getSchemas(GraphSpaceID spaceId) {
    CHECK_NOTNULL(env_->schemaMan_);
    auto tags = env_->schemaMan_->getAllVerTagSchema(spaceId);
    if (!tags.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    auto edges = env_->schemaMan_->getAllVerEdgeSchema(spaceId);
    if (!edges.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    for (auto tag : tags.value()) {
        auto tagId = tag.first;
        auto tagNameRet = env_->schemaMan_->toTagName(spaceId, tagId);
        if (!tagNameRet.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId << " tagId " << tagId;
            continue;
        }
        tags_.emplace(tagId, std::move(tagNameRet.value()));
    }

    for (auto edge : edges.value()) {
        auto edgeType = edge.first;
        auto edgeNameRet = env_->schemaMan_->toEdgeName(spaceId, std::abs(edgeType));
        if (!edgeNameRet.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId << " edgeType "
                    << std::abs(edgeType);
            continue;
        }
        edges_.emplace(edgeType, std::move(edgeNameRet.value()));
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

ErrorOr<cpp2::ErrorCode, std::vector<AdminSubTask>>
StatisTask::genSubTasks() {
    spaceId_ = ctx_.parameters_.space_id;
    auto parts = ctx_.parameters_.parts;
    subTaskSize_ = parts.size();

    auto ret = getSchemas(spaceId_);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Space not found, spaceId: " << spaceId_;
        return ret;
    }

    std::vector<AdminSubTask> tasks;
    for (const auto& part : parts) {
        std::function<kvstore::ResultCode()> task = std::bind(&StatisTask::genSubTask,
                                                              this, spaceId_, part,
                                                              tags_, edges_);
        tasks.emplace_back(std::move(task));
    }
    return tasks;
}

// Statis the specified tags and edges
kvstore::ResultCode
StatisTask::genSubTask(GraphSpaceID spaceId,
                       PartitionID part,
                       std::unordered_map<TagID, std::string> tags,
                       std::unordered_map<EdgeType, std::string> edges) {
    auto vIdLenRet = env_->schemaMan_->getSpaceVidLen(spaceId);
    if (!vIdLenRet.ok()) {
        LOG(ERROR) << "Get space vid length failed";
        return kvstore::ResultCode::ERR_SPACE_NOT_FOUND;
    }

    auto vIdType = this->env_->schemaMan_->getSpaceVidType(spaceId);
    if (!vIdType.ok()) {
        LOG(ERROR) << "Get space vid type failed";
        return kvstore::ResultCode::ERR_SPACE_NOT_FOUND;
    }

    auto vIdLen = vIdLenRet.value();
    bool isIntId = (vIdType.value() == meta::cpp2::PropertyType::INT64);
    auto partitionNumRet = env_->schemaMan_->getPartsNum(spaceId);
    if (!partitionNumRet.ok()) {
        LOG(ERROR) << "Get space partition number failed";
        return kvstore::ResultCode::ERR_SPACE_NOT_FOUND;
    }

    auto partitionNum = partitionNumRet.value();
    LOG(INFO) << "Start statis task";
    CHECK_NOTNULL(env_->kvstore_);
    auto vertexPrefix = NebulaKeyUtils::vertexPrefix(part);
    std::unique_ptr<kvstore::KVIterator> vertexIter;
    auto edgePrefix = NebulaKeyUtils::edgePrefix(part);
    std::unique_ptr<kvstore::KVIterator> edgeIter;

    // When the storage occurs leader change, continue to read data from the follower
    // instead of reporting an error.
    auto ret = env_->kvstore_->prefix(spaceId, part, vertexPrefix, &vertexIter, true);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Statis task failed";
        return ret;
    }
    ret = env_->kvstore_->prefix(spaceId, part, edgePrefix, &edgeIter, true);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Statis task failed";
        return ret;
    }

    std::unordered_map<TagID, int64_t>       tagsVertices;
    std::unordered_map<EdgeType, int64_t>    edgetypeEdges;
    std::unordered_map<PartitionID, int64_t> relevancy;
    int64_t                                  spaceVertices = 0;
    int64_t                                  spaceEdges = 0;

    for (auto tag : tags) {
        tagsVertices[tag.first] = 0;
    }

    for (auto edge : edges) {
        edgetypeEdges[edge.first] = 0;
    }

    VertexID                              lastVertexId = "";

    // Only statis valid vetex data, no multi version
    // For example
    // Vid  tagId
    // 1     1
    // 2     2  (invalid data, for example, tag data without tag schema)
    // 2     3
    // 2     5
    // 3     1
    while (vertexIter && vertexIter->valid()) {
        auto key = vertexIter->key();
        auto vId = NebulaKeyUtils::getVertexId(vIdLen, key).str();
        auto tagId = NebulaKeyUtils::getTagId(vIdLen, key);

        auto it = tagsVertices.find(tagId);
        if (it == tagsVertices.end()) {
            // Invalid data
            vertexIter->next();
            continue;
        }

        if (vId == lastVertexId) {
            tagsVertices[tagId] += 1;
        } else {
            tagsVertices[tagId] += 1;
            spaceVertices++;
            lastVertexId  = vId;
        }
        vertexIter->next();
    }

    // Only statis valid edge data, no multi version
    // For example
    // src edgetype rank dst
    // 1    1       1    2
    // 2    2       1    3  (invalid data, for example, edge data without edge schema)
    // 2    3       1    4
    // 2    3       1    5
    while (edgeIter && edgeIter->valid()) {
        auto key = edgeIter->key();

        auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen, key);
        // Because edge lock in toss and edge are the same except for the last byte.
        // But only the in-edge has a lock.
        if (edgeType < 0 || edgetypeEdges.find(edgeType) == edgetypeEdges.end()) {
            edgeIter->next();
            continue;
        }

        auto destination = NebulaKeyUtils::getDstId(vIdLen, key).str();

        spaceEdges++;
        edgetypeEdges[edgeType] += 1;

        uint64_t vid = 0;
        if (isIntId) {
            memcpy(static_cast<void*>(&vid), destination.data(), 8);
        } else {
            nebula::MurmurHash2 hash;
            vid = hash(destination.data());
        }

        PartitionID partId = vid % partitionNum + 1;
        relevancy[partId]++;
        edgeIter->next();
    }

    nebula::meta::cpp2::StatisItem statisItem;

    // convert tagId/edgeType to tagName/edgeName
    for (auto &tagElem : tagsVertices) {
        auto iter = tags_.find(tagElem.first);
        if (iter != tags_.end()) {
            statisItem.tag_vertices.emplace(iter->second, tagElem.second);
        }
    }
    for (auto &edgeElem : edgetypeEdges) {
        auto iter = edges_.find(edgeElem.first);
        if (iter != edges_.end()) {
            statisItem.edges.emplace(iter->second, edgeElem.second);
        }
    }

    statisItem.set_space_vertices(spaceVertices);
    statisItem.set_space_edges(spaceEdges);

    using Correlativiyties = std::vector<nebula::meta::cpp2::Correlativity>;
    Correlativiyties correlativity;
    for (const auto& entry : relevancy) {
        nebula::meta::cpp2::Correlativity partProportion;
        partProportion.set_part_id(entry.first);
        LOG(INFO) << "parts edges " << entry.second << " total " << spaceEdges;
        double proportion = static_cast<double>(entry.second) / static_cast<double>(spaceEdges);
        partProportion.set_proportion(proportion);
        LOG(INFO) << "Part " << entry.first << " proportion " << proportion;
        correlativity.emplace_back(std::move(partProportion));
    }

    std::sort(correlativity.begin(), correlativity.end(),
              [&] (const auto& l, const auto& r) {
                  return l.proportion < r.proportion;
              });

    std::unordered_map<PartitionID, Correlativiyties> partCorelativity;
    partCorelativity[part] = correlativity;
    statisItem.set_part_corelativity(std::move(partCorelativity));
    statistics_.emplace(part, std::move(statisItem));
    LOG(INFO) << "Statis task finished";
    return kvstore::ResultCode::SUCCEEDED;
}

void StatisTask::finish(cpp2::ErrorCode rc) {
    FLOG_INFO("task(%d, %d) finished, rc=[%d]", ctx_.jobId_, ctx_.taskId_,
              static_cast<int>(rc));
    nebula::meta::cpp2::StatisItem  result;
    result.set_status(nebula::meta::cpp2::JobStatus::FAILED);

    if (rc == cpp2::ErrorCode::SUCCEEDED && statistics_.size() == subTaskSize_) {
        result.space_vertices = 0;
        result.space_edges = 0;
        for (auto& elem : statistics_) {
            auto item = elem.second;
            result.space_vertices += item.space_vertices;
            result.space_edges += item.space_edges;

            for (auto& tagElem : item.tag_vertices) {
                auto tagId = tagElem.first;
                auto iter = result.tag_vertices.find(tagId);
                if (iter == result.tag_vertices.end()) {
                    result.tag_vertices.emplace(tagId, tagElem.second);
                } else {
                    result.tag_vertices[tagId] += tagElem.second;
                }
            }

            for (auto& edgeElem : item.edges) {
                auto edgetype = edgeElem.first;
                auto iter = result.edges.find(edgetype);
                if (iter == result.edges.end()) {
                    result.edges.emplace(edgetype, edgeElem.second);
                } else {
                    result.edges[edgetype] += edgeElem.second;
                }
            }
        }
        result.set_status(nebula::meta::cpp2::JobStatus::FINISHED);
        ctx_.onFinish_(rc, result);
    } else if (rc != cpp2::ErrorCode::SUCCEEDED) {
        ctx_.onFinish_(rc, result);
    } else {
        LOG(ERROR) << "The number of subtasks is not equal to the number of parts";
        ctx_.onFinish_(cpp2::ErrorCode::E_PART_NOT_FOUND, result);
    }
}

}  // namespace storage
}  // namespace nebula
