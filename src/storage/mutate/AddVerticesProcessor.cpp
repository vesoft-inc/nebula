/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/AddVerticesProcessor.h"
#include "common/NebulaKeyUtils.h"
#include <algorithm>
#include <limits>
#include "time/WallClock.h"
#include "codec/RowWriterV2.h"
#include "storage/StorageFlags.h"

DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

void AddVerticesProcessor::process(const cpp2::AddVerticesRequest& req) {
    auto version =
        std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
    // Switch version to big-endian, make sure the key is in ordered.
    version = folly::Endian::big(version);

    spaceId_ = req.get_space_id();
    const auto& partVertices = req.get_parts();
    const auto& propNamesMap = req.get_prop_names();

    CHECK_NOTNULL(env_->schemaMan_);
    auto ret = env_->schemaMan_->getSpaceVidLen(spaceId_);
    if (!ret.ok()) {
        LOG(ERROR) << ret.status();
        for (auto& part : partVertices) {
            pushResultCode(cpp2::ErrorCode::E_INVALID_SPACEVIDLEN, part.first);
        }
        onFinished();
        return;
    }
    spaceVidLen_ = ret.value();
    callingNum_ = partVertices.size();

    CHECK_NOTNULL(env_->indexMan_);
    auto iRet = env_->indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    CHECK_NOTNULL(env_->kvstore_);
    if (indexes_.empty()) {
        for (auto& part : partVertices) {
            auto partId = part.first;
            const auto& newVertices = part.second;

            std::vector<kvstore::KV> data;
            data.reserve(32);
            for (auto& newVertex : newVertices) {
                auto vid = newVertex.get_id();
                const auto& newTags = newVertex.get_tags();

                if (!NebulaKeyUtils::isValidVidLen(spaceVidLen_, vid)) {
                    LOG(ERROR) << "Space " << spaceId_ << ", vertex length invalid, "
                               << " space vid len: " << spaceVidLen_ << ",  vid is " << vid;
                    pushResultCode(cpp2::ErrorCode::E_INVALID_VID, partId);
                    onFinished();
                    return;
                }

                for (auto& newTag : newTags) {
                    auto tagId = newTag.get_tag_id();
                    VLOG(3) << "PartitionID: " << partId << ", VertexID: " << vid
                            << ", TagID: " << tagId << ", TagVersion: " << version;

                    auto key = NebulaKeyUtils::vertexKey(spaceVidLen_, partId, vid,
                                                         tagId, version);
                    auto schema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
                    if (!schema) {
                        LOG(ERROR) << "Space " << spaceId_ << ", Tag " << tagId << " invalid";
                        pushResultCode(cpp2::ErrorCode::E_TAG_NOT_FOUND, partId);
                        onFinished();
                        return;
                    }

                    auto props = newTag.get_props();
                    auto iter = propNamesMap.find(tagId);
                    std::vector<std::string> propNames;
                    if (iter != propNamesMap.end()) {
                        propNames = iter->second;
                    }

                    auto retEnc = encodeRowVal(schema.get(), propNames, props);
                    if (!retEnc.ok()) {
                        LOG(ERROR) << retEnc.status();
                        pushResultCode(cpp2::ErrorCode::E_DATA_TYPE_MISMATCH, partId);
                        onFinished();
                        return;
                    }
                    data.emplace_back(std::move(key), std::move(retEnc.value()));

                    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
                        vertexCache_->evict(std::make_pair(vid, tagId), partId);
                        VLOG(3) << "Evict cache for vId " << vid
                                << ", tagId " << tagId;
                    }
                }
            }
            doPut(spaceId_, partId, std::move(data));
        }
    } else {
        LOG(FATAL) << "Unimplement";
    }
}

}  // namespace storage
}  // namespace nebula
