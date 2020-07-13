/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/context/StorageExpressionContext.h"
#include "utils/NebulaKeyUtils.h"

namespace nebula {
namespace storage {

// Get the specified property from the edge, such as edgename.prop_name
Value StorageExpressionContext::getEdgeProp(const std::string& edgeName,
                                            const std::string& prop) const {
    if (reader_ != nullptr) {
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
            return reader_->getValueByName(prop);
        }
    }
    return Value::kNullValue;
}

Value StorageExpressionContext::getSrcProp(const std::string& tagName,
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
