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
            return Value(Expression::eval(field->defaultValue(), expCtx));
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
    if (reader_ != nullptr) {
        if (tagName != name_) {
            return Value::kNullValue;
        }
        if (prop == kVid) {
            return NebulaKeyUtils::getVertexId(vIdLen_, key_);
        } else {
            return readValue(prop);
        }
    } else {
        auto iter = tagFilters_.find(std::make_pair(tagName, prop));
        if (iter == tagFilters_.end()) {
            return Value::kNullValue;
        }
        return iter->second;
    }
}
// Get the specified property from the edge, such as edgename.prop_name
Value StorageExpressionContext::getEdgeProp(const std::string& edgeName,
                                            const std::string& prop) const {
    if (isIndex_) {
        return getIndexValue(prop, true);
    }
    if (isEdge_ && reader_ != nullptr) {
        if (edgeName != name_) {
            return Value::kNullValue;
        }
        if (prop == kSrc) {
            return NebulaKeyUtils::getSrcId(vIdLen_, key_);
        } else if (prop == kDst) {
            return NebulaKeyUtils::getDstId(vIdLen_, key_);
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
            return Value::kNullValue;
        }
        VLOG(1) << "Hit srcProp filter for edge " << edgeName << ", prop " << prop;
        return iter->second;
    }
}

Value StorageExpressionContext::getSrcProp(const std::string& tagName,
                                           const std::string& prop) const {
    if (!isEdge_ && reader_ != nullptr) {
        if (tagName != name_) {
            return Value::kNullValue;
        }
        return readValue(prop);
    } else {
        auto iter = tagFilters_.find(std::make_pair(tagName, prop));
        if (iter == tagFilters_.end()) {
            return Value::kNullValue;
        }
        VLOG(1) << "Hit srcProp filter for tag " << tagName << ", prop " << prop;
        return iter->second;
    }
}

Value StorageExpressionContext::getIndexValue(const std::string& prop, bool isEdge) const {
    return IndexKeyUtils::getValueFromIndexKey(vIdLen_, vColNum_, key_, prop,
                                               indexCols_, isEdge, hasNullableCol_);
}

}  // namespace storage
}  // namespace nebula
