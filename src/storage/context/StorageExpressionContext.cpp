/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/context/StorageExpressionContext.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

Value StorageExpressionContext::value_ = NullType::__NULL__;

// Get the specified property from the edge, such as edgename.prop_name
const Value& StorageExpressionContext::getEdgeProp(const std::string& edgeName,
                                                   const std::string& prop) const {
    if (edgeName != name_) {
        return Value::kNullValue;
    }
    if (prop == kSrc) {
        value_ = NebulaKeyUtils::getSrcId(vIdLen_, key_);
    } else if (prop == kDst) {
        value_ = NebulaKeyUtils::getDstId(vIdLen_, key_);
    } else if (prop == kRank) {
        value_ = NebulaKeyUtils::getRank(vIdLen_, key_);
    } else if (prop == kType) {
        value_ = NebulaKeyUtils::getEdgeType(vIdLen_, key_);
    } else {
        // todo(doodle): const reference... wait change return value type by value,
        // and we need to use schema to get default value if necessary
        value_ = reader_->getValueByName(prop);
    }
    return value_;
}

const Value& StorageExpressionContext::getSrcProp(const std::string& tagName,
                                                  const std::string& prop) const {
    auto iter = tagFilters_.find(std::make_pair(tagName, prop));
    if (iter == tagFilters_.end()) {
        return Value::kNullValue;
    }
    VLOG(1) << "Hit srcProp filter for tag " << tagName << ", prop " << prop;
    return iter->second;
}

}  // namespace storage
}  // namespace nebula
