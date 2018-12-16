/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_TAGSCHEMA_H_
#define VGRAPHD_TAGSCHEMA_H_

#include "base/Base.h"
#include "vgraphd/mock/PropertiesSchema.h"

namespace vesoft {
namespace vgraph {

class TagSchema final {
public:
    uint32_t id() const {
        return id_;
    }

    void setId(uint32_t id) {
        id_ = id;
    }

    const std::string &name() const {
        return name_;
    }

    void setName(std::string name) {
        name_ = std::move(name);
    }

    void addProperty(std::string name, ColumnType type);

    const PropertyItem* getProperty(const std::string &name) const;

    const PropertiesSchema* getPropertiesSchema() const;

    std::string toString() const;

private:
    uint32_t                                    id_{0};
    std::string                                 name_;
    PropertiesSchema                            properties_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_TAGSCHEMA_H_
