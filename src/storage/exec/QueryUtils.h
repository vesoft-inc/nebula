/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_QUERYUTILS_H_
#define STORAGE_EXEC_QUERYUTILS_H_

namespace nebula {
namespace storage {

#include "common/base/Base.h"
#include "storage/CommonUtils.h"
#include "storage/query/QueryBaseProcessor.h"

class QueryUtils final {
public:
    static StatusOr<nebula::Value> readValue(RowReader* reader, const PropContext& ctx) {
        auto value = reader->getValueByName(ctx.name_);
        if (value.type() == Value::Type::NULLVALUE) {
            // read null value
            auto nullType = value.getNull();
            if (nullType == NullType::BAD_DATA ||
                nullType == NullType::BAD_TYPE ||
                nullType == NullType::UNKNOWN_PROP) {
                VLOG(1) << "Fail to read prop " << ctx.name_;
                if (ctx.field_ != nullptr) {
                    if (ctx.field_->hasDefault()) {
                        return ctx.field_->defaultValue();
                    } else if (ctx.field_->nullable()) {
                        return NullType::__NULL__;
                    }
                }
            } else if (nullType == NullType::__NULL__ || nullType == NullType::NaN) {
                return value;
            }
            return Status::Error(folly::stringPrintf("Fail to read prop %s ", ctx.name_.c_str()));
        }
        return value;
    }

    static StatusOr<nebula::Value> readEdgeProp(VertexIDSlice srcId,
                                                EdgeType edgeType,
                                                EdgeRanking edgeRank,
                                                VertexIDSlice dstId,
                                                RowReader* reader,
                                                const PropContext& prop) {
        switch (prop.propInKeyType_) {
            // prop in value
            case PropContext::PropInKeyType::NONE: {
                return readValue(reader, prop);
            }
            case PropContext::PropInKeyType::SRC: {
                return srcId.subpiece(0, srcId.find_first_of('\0'));
            }
            case PropContext::PropInKeyType::TYPE: {
                return edgeType;
            }
            case PropContext::PropInKeyType::RANK: {
                return edgeRank;
            }
            case PropContext::PropInKeyType::DST: {
                return dstId.subpiece(0, dstId.find_first_of('\0'));
            }
        }
        return Status::Error(folly::stringPrintf("Invalid property %s", prop.name_.c_str()));
    }

    // return none if no valid ttl, else return the ttl property name and time
    static folly::Optional<std::pair<std::string, int64_t>>
    getEdgeTTLInfo(EdgeContext* edgeContext, EdgeType edgeType) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto edgeFound = edgeContext->ttlInfo_.find(std::abs(edgeType));
        if (edgeFound != edgeContext->ttlInfo_.end()) {
            ret.emplace(edgeFound->second.first, edgeFound->second.second);
        }
        return ret;
    }

    // return none if no valid ttl, else return the ttl property name and time
    static folly::Optional<std::pair<std::string, int64_t>>
    getTagTTLInfo(TagContext* tagContext, TagID tagId) {
        folly::Optional<std::pair<std::string, int64_t>> ret;
        auto tagFound = tagContext->ttlInfo_.find(tagId);
        if (tagFound != tagContext->ttlInfo_.end()) {
            ret.emplace(tagFound->second.first, tagFound->second.second);
        }
        return ret;
    }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_EXEC_QUERYUTILS_H_
