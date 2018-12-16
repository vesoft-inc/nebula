/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "graph/mock/EdgeSchema.h"

namespace nebula {
namespace graph {

void EdgeSchema::addProperty(std::string name, ColumnType type) {
    properties_.addProperty(std::move(name), type);
}


const PropertyItem* EdgeSchema::getProperty(const std::string &name) {
    return properties_.getProperty(name);
}


std::string EdgeSchema::toString() const {
    return folly::stringPrintf("%s[%u] %s[%u]->%s[%u] %s",
                               name_.c_str(), id_,
                               srcTag_.c_str(), srcId_,
                               dstTag_.c_str(), dstId_,
                               properties_.toString().c_str());
}

}   // namespace graph
}   // namespace nebula
