/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_EDGESCHEMA_H_
#define VGRAPHD_EDGESCHEMA_H_

#include "base/Base.h"
#include "vgraphd/mock/PropertiesSchema.h"

namespace vesoft {
namespace vgraph {

class EdgeSchema final {
public:
    uint32_t id() const {
        return id_;
    }

    void setId(uint32_t id) {
        id_ = id;
    }

    const std::string& name() const {
        return name_;
    }

    void setName(std::string name) {
        name_ = std::move(name);
    }

    const std::string& srcTag() const {
        return srcTag_;
    }

    void setSrcTag(std::string tag) {
        srcTag_ = tag;
    }

    const std::string& dstTag() const {
        return dstTag_;
    }

    void setDstTag(std::string tag) {
        dstTag_ = tag;
    }

    uint32_t srcId() const {
        return srcId_;
    }

    void setSrcId(uint32_t id) {
        srcId_ = id;
    }

    uint32_t dstId() const {
        return dstId_;
    }

    void setDstId(uint32_t id) {
        dstId_ = id;
    }

    void addProperty(std::string name, ColumnType type);

    const PropertyItem* getProperty(const std::string &name);

    const PropertiesSchema* getPropertiesSchema() const {
        return &properties_;
    }

    std::string toString() const;

private:
    uint32_t                                    id_{0};
    uint32_t                                    srcId_{0};
    uint32_t                                    dstId_{0};
    std::string                                 name_;
    std::string                                 srcTag_;
    std::string                                 dstTag_;
    PropertiesSchema                            properties_;
};

}   // namespace vgraph
}   // namespace vesoft

#endif  // VGRAPHD_EDGESCHEMA_H_
