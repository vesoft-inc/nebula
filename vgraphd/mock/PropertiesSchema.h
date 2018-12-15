/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_PROPERTIESSCHEMA_H_
#define VGRAPHD_PROPERTIESSCHEMA_H_

#include "base/Base.h"
#include "parser/Expressions.h"

namespace vesoft {
namespace vgraph {

struct PropertyItem {
    uint32_t                                    index_{0};
    std::string                                 name_;
    ColumnType                                  type_;
};


class PropertiesSchema final {
public:
    void addProperty(std::string name, ColumnType type);

    const PropertyItem* getProperty(const std::string &name) const;

    const std::vector<PropertyItem>& getItems() const;

    std::string toString() const;

private:
    using MappingType = std::unordered_map<std::string, const PropertyItem*>;
    std::vector<PropertyItem>                   properties_;
    MappingType                                 mapping_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_PROPERTIESSCHEMA_H_
