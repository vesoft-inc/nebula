/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "vgraphd/mock/SchemaManager.h"

namespace vesoft {
namespace vgraph {

void SchemaManager::addEdgeSchema(const std::string &name,
                                  const std::string &src,
                                  const std::string &dst,
                                  const std::vector<ColumnSpecification*> &specs) {
    // TODO(dutor) check existence of src & dst tag
    auto &schema = edges_[name];
    schema.setId(nextEdgeId_++);
    schema.setName(name);
    schema.setSrcTag(src);
    schema.setDstTag(dst);
    schema.setSrcId(tags_[src].id());
    schema.setDstId(tags_[dst].id());
    for (auto *spec : specs) {
        schema.addProperty(*spec->name(), spec->type());
    }
}


void SchemaManager::addTagSchema(const std::string &name,
                                 const std::vector<ColumnSpecification*> &specs) {
    auto &schema = tags_[name];
    schema.setId(nextTagId_++);
    schema.setName(name);
    for (auto *spec : specs) {
        schema.addProperty(*spec->name(), spec->type());
    }
}


const EdgeSchema* SchemaManager::getEdgeSchema(const std::string &name) {
    auto iter = edges_.find(name);
    if (iter == edges_.end()) {
        return nullptr;
    }
    return &iter->second;
}


const TagSchema* SchemaManager::getTagSchema(const std::string &name) {
    auto iter = tags_.find(name);
    if (iter == tags_.end()) {
        return nullptr;
    }
    return &iter->second;
}


void SchemaManager::print() const {
    for (auto &entry : edges_) {
        FLOG_INFO("Edge `%s': \n%s", entry.first.c_str(), entry.second.toString().c_str());
    }
    for (auto &entry : tags_) {
        FLOG_INFO("Tag `%s': \n%s", entry.first.c_str(), entry.second.toString().c_str());
    }
}

}   // namespace vgraph
}   // namespace vesoft
