/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/mock/PropertiesSchema.h"

namespace vesoft {
namespace vgraph {

void PropertiesSchema::addProperty(std::string name, ColumnType type) {
    auto index = properties_.size();
    properties_.emplace_back();
    auto &item = properties_.back();
    item.index_ = index;
    item.name_ = std::move(name);
    item.type_ = type;
    mapping_[name] = &item;
}


const PropertyItem* PropertiesSchema::getProperty(const std::string &name) const {
    auto iter = mapping_.find(name);
    if (iter != mapping_.end()) {
        return nullptr;
    }
    return iter->second;
}


const std::vector<PropertyItem>& PropertiesSchema::getItems() const {
    return properties_;
}


std::string PropertiesSchema::toString() const {
    std::string buf;
    for (auto &item : properties_) {
        buf += item.name_;
        buf += "[";
        buf += std::to_string(item.index_);
        buf += "] ";
        buf += columnTypeToString(item.type_);
        buf += ",";
    }
    if (!buf.empty()) {
        buf.resize(buf.size() - 1);
    }
    return buf;
}

}   // namespace vgraph
}   // namespace vesoft
