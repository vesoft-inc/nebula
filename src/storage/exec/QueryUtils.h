/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_QUERYUTILS_H_
#define STORAGE_EXEC_QUERYUTILS_H_

#include "common/base/Base.h"
#include "common/expression/Expression.h"
#include "storage/CommonUtils.h"
#include "storage/query/QueryBaseProcessor.h"
#include "utils/DefaultValueContext.h"

namespace nebula {
namespace storage {

class QueryUtils final {
public:
    enum class ReturnColType : uint16_t {
        kVid,
        kTag,
        kSrc,
        kType,
        kRank,
        kDst,
        kOther,
    };

    static ReturnColType toReturnColType(const std::string& name) {
        if (name == kVid) {
            return ReturnColType::kVid;
        } else if (name == kTag) {
            return ReturnColType::kTag;
        } else if (name == kSrc) {
            return ReturnColType::kSrc;
        } else if (name == kType) {
            return ReturnColType::kType;
        } else if (name == kRank) {
            return ReturnColType::kRank;
        } else if (name == kDst) {
            return ReturnColType::kDst;
        } else {
            return ReturnColType::kOther;
        }
    }

    static StatusOr<nebula::Value> readValue(RowReader* reader,
                                             const std::string& propName,
                                             const meta::SchemaProviderIf::Field* field) {
        auto value = reader->getValueByName(propName);
        if (value.type() == Value::Type::NULLVALUE) {
            // read null value
            auto nullType = value.getNull();

            if (nullType == NullType::UNKNOWN_PROP) {
                VLOG(1) << "Fail to read prop " << propName;
                if (field->hasDefault()) {
                    DefaultValueContext expCtx;
                    auto expr = field->defaultValue()->clone();
                    return Expression::eval(expr, expCtx);
                } else if (field->nullable()) {
                    return NullType::__NULL__;
                }
            } else if (nullType == NullType::__NULL__) {
                // Need to check whether the field is nullable
                if (field->nullable()) {
                    return value;
                }
            }
            return Status::Error(folly::stringPrintf("Fail to read prop %s ", propName.c_str()));
        }
        return value;
    }

    // read prop value, If the RowReader contains this field,
    // read from the rowreader, otherwise read the default value
    // or null value from the latest schema
    static StatusOr<nebula::Value> readValue(RowReader* reader,
                                             const std::string& propName,
                                             const meta::NebulaSchemaProvider* schema) {
        auto field = schema->field(propName);
        if (!field) {
            return Status::Error(folly::stringPrintf("Fail to read prop %s ",
                                                     propName.c_str()));
        }
        return readValue(reader, propName, field);
    }


    static StatusOr<nebula::Value> readEdgeProp(folly::StringPiece key,
                                                size_t vIdLen,
                                                bool isIntId,
                                                RowReader* reader,
                                                const PropContext& prop) {
        switch (prop.propInKeyType_) {
            // prop in value
            case PropContext::PropInKeyType::NONE: {
                return readValue(reader, prop.name_, prop.field_);
            }
            case PropContext::PropInKeyType::SRC: {
                auto srcId = NebulaKeyUtils::getSrcId(vIdLen, key);
                if (isIntId) {
                    return *reinterpret_cast<const int64_t*>(srcId.data());
                } else {
                    return srcId.subpiece(0, srcId.find_first_of('\0')).toString();
                }
            }
            case PropContext::PropInKeyType::TYPE: {
                auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen, key);
                return edgeType;
            }
            case PropContext::PropInKeyType::RANK: {
                auto edgeRank = NebulaKeyUtils::getRank(vIdLen, key);
                return edgeRank;
            }
            case PropContext::PropInKeyType::DST: {
                auto dstId = NebulaKeyUtils::getDstId(vIdLen, key);
                if (isIntId) {
                    return *reinterpret_cast<const int64_t*>(dstId.data());
                } else {
                    return dstId.subpiece(0, dstId.find_first_of('\0')).toString();
                }
            }
            default:
                LOG(FATAL) << "Should not read here";
        }
        return Status::Error(folly::stringPrintf("Invalid property %s", prop.name_.c_str()));
    }

    static StatusOr<nebula::Value> readVertexProp(folly::StringPiece key,
                                                  size_t vIdLen,
                                                  bool isIntId,
                                                  RowReader* reader,
                                                  const PropContext& prop) {
        switch (prop.propInKeyType_) {
            // prop in value
            case PropContext::PropInKeyType::NONE: {
                return readValue(reader, prop.name_, prop.field_);
            }
            case PropContext::PropInKeyType::VID: {
                auto vId = NebulaKeyUtils::getVertexId(vIdLen, key);
                if (isIntId) {
                    return *reinterpret_cast<const int64_t*>(vId.data());
                } else {
                    return vId.subpiece(0, vId.find_first_of('\0')).toString();
                }
            }
            case PropContext::PropInKeyType::TAG: {
                auto tag = NebulaKeyUtils::getTagId(vIdLen, key);
                return tag;
            }
            default:
                LOG(FATAL) << "Should not read here";
        }
        return Status::Error(folly::stringPrintf("Invalid property %s", prop.name_.c_str()));
    }

    static Status collectVertexProps(folly::StringPiece key,
                                     size_t vIdLen,
                                     bool isIntId,
                                     RowReader* reader,
                                     const std::vector<PropContext>* props,
                                     nebula::List& list) {
        for (const auto& prop : *props) {
            if (prop.returned_) {
                VLOG(2) << "Collect prop " << prop.name_;
                auto value = QueryUtils::readVertexProp(key, vIdLen, isIntId, reader, prop);
                if (!value.ok()) {
                    return value.status();
                }
                list.emplace_back(std::move(value).value());
            }
        }
        return Status::OK();
    }

    static Status collectEdgeProps(folly::StringPiece key,
                                   size_t vIdLen,
                                   bool isIntId,
                                   RowReader* reader,
                                   const std::vector<PropContext>* props,
                                   nebula::List& list) {
        for (const auto& prop : *props) {
            if (prop.returned_) {
                VLOG(2) << "Collect prop " << prop.name_;
                auto value = QueryUtils::readEdgeProp(key, vIdLen, isIntId, reader, prop);
                if (!value.ok()) {
                    return value.status();
                }
                list.emplace_back(std::move(value).value());
            }
        }
        return Status::OK();
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
