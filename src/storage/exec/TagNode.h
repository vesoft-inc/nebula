/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_TAGNODE_H_
#define STORAGE_EXEC_TAGNODE_H_

#include "common/base/Base.h"
#include "storage/exec/RelNode.h"

namespace nebula {
namespace storage {

class TagNode final : public RelNode {
public:
    TagNode(TagContext* ctx,
            StorageEnv* env,
            GraphSpaceID spaceId,
            PartitionID partId,
            size_t vIdLen,
            const VertexID& vId,
            FilterNode* filter,
            nebula::Row* row)
        : tagContext_(ctx)
        , env_(env)
        , spaceId_(spaceId)
        , partId_(partId)
        , vIdLen_(vIdLen)
        , vId_(vId)
        , filter_(filter)
        , resultRow_(row) {}

    folly::Future<kvstore::ResultCode> execute() override {
        for (auto& tc : tagContext_->propContexts_) {
            nebula::DataSet dataSet;
            VLOG(1) << "partId " << partId_ << ", vId " << vId_ << ", tagId " << tc.first
                    << ", prop size " << tc.second.size();
            auto ret = processTagProps(tc.first, tc.second, dataSet);
            if (ret == kvstore::ResultCode::SUCCEEDED) {
                resultRow_->columns.emplace_back(std::move(dataSet));
                continue;
            } else if (ret == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
                resultRow_->columns.emplace_back(NullType::__NULL__);
                continue;
            } else {
                return ret;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

    kvstore::ResultCode processTagProps(TagID tagId,
                                        const std::vector<PropContext>& returnProps,
                                        nebula::DataSet& dataSet) {
        // use latest schema to check if value is expired for ttl
        auto schemaIter = tagContext_->schemas_.find(tagId);
        CHECK(schemaIter != tagContext_->schemas_.end());
        const auto* latestSchema = schemaIter->second.back().get();
        if (FLAGS_enable_vertex_cache && tagContext_->vertexCache_ != nullptr) {
            auto result = tagContext_->vertexCache_->get(std::make_pair(vId_, tagId), partId_);
            if (result.ok()) {
                auto value = std::move(result).value();
                auto ret = collectTagPropIfValid(latestSchema, value, tagId,
                                                 returnProps, dataSet);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    VLOG(1) << "Evict from cache vId " << vId_ << ", tagId " << tagId;
                    tagContext_->vertexCache_->evict(std::make_pair(vId_, tagId), partId_);
                }
                return ret;
            } else {
                VLOG(1) << "Miss cache for vId " << vId_ << ", tagId " << tagId;
            }
        }

        auto prefix = NebulaKeyUtils::vertexPrefix(vIdLen_, partId_, vId_, tagId);
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = env_->kvstore_->prefix(spaceId_, partId_, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            VLOG(1) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
            return ret;
        }
        if (iter && iter->valid()) {
            // Will decode the properties according to the schema version
            // stored along with the properties
            ret = collectTagPropIfValid(latestSchema, iter->val(), tagId,
                                        returnProps, dataSet);
            if (ret == kvstore::ResultCode::SUCCEEDED &&
                FLAGS_enable_vertex_cache &&
                tagContext_->vertexCache_ != nullptr) {
                tagContext_->vertexCache_->insert(std::make_pair(vId_, tagId),
                                                  iter->val().str(), partId_);
                VLOG(1) << "Insert cache for vId " << vId_ << ", tagId " << tagId;
            }
            return ret;
        } else {
            VLOG(1) << "Missed partId " << partId_ << ", vId " << vId_ << ", tagId " << tagId;
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
    }

    kvstore::ResultCode collectTagPropIfValid(const meta::NebulaSchemaProvider* schema,
                                              folly::StringPiece value,
                                              TagID tagId,
                                              const std::vector<PropContext>& returnProps,
                                              nebula::DataSet& dataSet) {
        auto reader = RowReader::getTagPropReader(env_->schemaMan_, spaceId_, tagId, value);
        if (!reader) {
            VLOG(1) << "Can't get tag reader of " << tagId;
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        auto ttl = getTagTTLInfo(tagId);
        if (ttl.hasValue()) {
            auto ttlValue = ttl.value();
            if (CommonUtils::checkDataExpiredForTTL(schema, reader.get(),
                                                    ttlValue.first, ttlValue.second)) {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }
        }
        return collectTagProps(tagId, reader.get(), returnProps, dataSet);
    }

    kvstore::ResultCode collectTagProps(TagID tagId,
                                        RowReader* reader,
                                        const std::vector<PropContext>& props,
                                        nebula::DataSet& dataSet) {
        nebula::Row row;
        for (auto& prop : props) {
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId;
            if (reader != nullptr) {
                auto status = readValue(reader, prop);
                if (!status.ok()) {
                    return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
                }
                auto value = std::move(status.value());

                if (prop.tagFiltered_) {
                    filter_->fillTagProp(tagId, prop.name_, value);
                }
                if (prop.returned_) {
                    row.columns.emplace_back(std::move(value));
                }
            }
        }
        dataSet.rows.emplace_back(std::move(row));
        return kvstore::ResultCode::SUCCEEDED;
    }

    folly::Optional<std::pair<std::string, int64_t>> getTagTTLInfo(TagID tagId) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto tagFound = tagContext_->ttlInfo_.find(tagId);
        if (tagFound != tagContext_->ttlInfo_.end()) {
            ret.emplace(tagFound->second.first, tagFound->second.second);
        }
        return ret;
    }

private:
    TagContext* tagContext_;
    StorageEnv* env_;
    GraphSpaceID spaceId_;
    PartitionID partId_;
    size_t vIdLen_;
    VertexID vId_;
    FilterNode* filter_;
    nebula::Row* resultRow_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_TAGNODE_H_
