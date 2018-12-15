/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_SCHEMAMANAGER_H_
#define VGRAPHD_SCHEMAMANAGER_H_

#include "base/Base.h"
#include "vgraphd/mock/EdgeSchema.h"
#include "vgraphd/mock/TagSchema.h"
#include "parser/MaintainSentences.h"

namespace vesoft {
namespace vgraph {

class SchemaManager final {
public:
    void addEdgeSchema(const std::string &name,
                       const std::string &src,
                       const std::string &dst,
                       const std::vector<ColumnSpecification*> &specs);
    void addTagSchema(const std::string &name,
                      const std::vector<ColumnSpecification*> &specs);

    const EdgeSchema* getEdgeSchema(const std::string &name);

    const TagSchema* getTagSchema(const std::string &name);

    void print() const;

private:
    uint32_t                                    nextEdgeId_{0};
    uint32_t                                    nextTagId_{0};
    std::unordered_map<std::string, EdgeSchema> edges_;
    std::unordered_map<std::string, TagSchema>  tags_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_SCHEMAMANAGER_H_
