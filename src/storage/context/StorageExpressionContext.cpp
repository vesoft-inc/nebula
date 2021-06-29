/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/context/StorageExpressionContext.h"
#include <utils/IndexKeyUtils.h>
#include "utils/NebulaKeyUtils.h"
#include "utils/DefaultValueContext.h"

namespace nebula {
namespace storage {

Value StorageExpressionContext::readValue(const std::string& propName) const {
    if (!schema_) {
        return Value::kNullValue;
    }
    auto field = schema_->field(propName);
    if (!field) {
       return Value::kNullValue;
    }
    auto value = reader_->getValueByName(propName);
    if (value.type() == Value::Type::NULLVALUE) {
        // read null value
        auto nullType = value.getNull();
        if (nullType == NullType::UNKNOWN_PROP && field->hasDefault()) {
            DefaultValueContext expCtx;
            auto expr = field->defaultValue()->clone();
            return Value(Expression::eval(expr, expCtx));
        }
        return Value::kNullValue;
    }
    return value;
}

// Get the specified property from the tag, such as tag_name.prop_name
Value StorageExpressionContext::getTagProp(const std::string& tagName,
                                           const std::string& prop) const {
    if (isIndex_) {
        return getIndexValue(prop, false);
    }
    return getSrcProp(tagName, prop);
}

// Get the specified property from the edge, such as edgename.prop_name
Value StorageExpressionContext::getEdgeProp(const std::string& edgeName,
                                            const std::string& prop) const {
    if (isIndex_) {
        return getIndexValue(prop, true);
    }
    if (isEdge_ && reader_ != nullptr) {
        if (edgeName != name_) {
            return Value::kEmpty;
        }
        if (prop == kSrc) {
            auto srcId = NebulaKeyUtils::getSrcId(vIdLen_, key_);
            if (isIntId_) {
                int64_t val;
                memcpy(reinterpret_cast<void*>(&val), srcId.begin(), sizeof(int64_t));
                return val;
            } else {
                return srcId.subpiece(0, srcId.find_first_of('\0')).toString();
            }
        } else if (prop == kDst) {
            auto dstId = NebulaKeyUtils::getDstId(vIdLen_, key_);
            if (isIntId_) {
                int64_t val;
                memcpy(reinterpret_cast<void*>(&val), dstId.begin(), sizeof(int64_t));
                return val;
            } else {
                return dstId.subpiece(0, dstId.find_first_of('\0')).toString();
            }
        } else if (prop == kRank) {
            return NebulaKeyUtils::getRank(vIdLen_, key_);
        } else if (prop == kType) {
            return NebulaKeyUtils::getEdgeType(vIdLen_, key_);
        } else {
            return readValue(prop);
        }
    } else {
        auto iter = edgeFilters_.find(std::make_pair(edgeName, prop));
        if (iter == edgeFilters_.end()) {
            return Value::kEmpty;
        }
        VLOG(1) << "Hit srcProp filter for edge " << edgeName << ", prop " << prop;
        return iter->second;
    }
}

Value StorageExpressionContext::getSrcProp(const std::string& tagName,
                                           const std::string& prop) const {
    if (!isEdge_ && reader_ != nullptr) {
        if (tagName != name_) {
            return Value::kEmpty;
        }
        if (prop == kVid) {
            auto vId = NebulaKeyUtils::getVertexId(vIdLen_, key_);
            if (isIntId_) {
                int64_t val;
                memcpy(reinterpret_cast<void*>(&val), vId.begin(), sizeof(int64_t));
                return val;
            } else {
                return vId.subpiece(0, vId.find_first_of('\0')).toString();
            }
        } else if (prop == kTag) {
            return NebulaKeyUtils::getTagId(vIdLen_, key_);
        } else {
            return readValue(prop);
        }
    } else {
        auto iter = tagFilters_.find(std::make_pair(tagName, prop));
        if (iter == tagFilters_.end()) {
            return Value::kEmpty;
        }
        VLOG(1) << "Hit srcProp filter for tag " << tagName << ", prop " << prop;
        return iter->second;
    }
}

Value StorageExpressionContext::getIndexValue(const std::string& prop, bool isEdge) const {
    // TODO (sky) : Handle string type values.
    //              when field type is FIXED_STRING type,
    //              actual length of the value is called "len"
    //              FIXED_STRING.type.len is called "fixed_len"
    //              if (len > fixed_len) : v = value.substr(0, fixed_len)
    //              if (len < fixed_len) : v.append(fixed_len - len, '\0')
    return IndexKeyUtils::getValueFromIndexKey(vIdLen_, key_, prop,
                                               fields_, isEdge, hasNullableCol_);
}

}  // namespace storage
}  // namespace nebula
