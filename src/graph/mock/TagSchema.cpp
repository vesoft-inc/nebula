/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/mock/TagSchema.h"

namespace nebula {
namespace graph {

void TagSchema::addProperty(std::string name, ColumnType type) {
    properties_.addProperty(std::move(name), type);
}


const PropertyItem* TagSchema::getProperty(const std::string &name) const {
    return properties_.getProperty(name);
}


std::string TagSchema::toString() const {
    return folly::stringPrintf("%s[%u] %s",
                               name_.c_str(), id_, properties_.toString().c_str());
}


const PropertiesSchema* TagSchema::getPropertiesSchema() const {
    return &properties_;
}

}   // namespace graph
}   // namespace nebula
