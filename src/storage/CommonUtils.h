/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMMON_H_
#define STORAGE_COMMON_H_

#include "base/Base.h"
#include "base/ConcurrentLRUCache.h"
#include "filter/Expressions.h"
#include "dataman/RowReader.h"

namespace nebula {
namespace storage {

using TagProp = std::pair<std::string, std::string>;

using VertexCache = ConcurrentLRUCache<std::pair<VertexID, TagID>, std::string>;

struct FilterContext {
    // key: <tagName, propName> -> propValue
    std::unordered_map<TagProp, VariantType> tagFilters_;
};

class PropContext {
public:
    enum PropInKeyType {
        NONE = 0x00,
        SRC  = 0x01,
        DST  = 0x02,
        TYPE = 0x03,
        RANK = 0x04,
    };

    PropContext() = default;

    // Some default props could be constructed by this ctor
    // For example, _src, _dst, _type, _rank
    PropContext(const char* name, EdgeType eType, int32_t retIndex,
                PropContext::PropInKeyType pikType) {
        prop_.name = name;
        prop_.owner = cpp2::PropOwner::EDGE;
        retIndex_ = retIndex;
        prop_.id.set_edge_type(eType);
        if (prop_.name == "_src" || prop_.name == "_dst") {
            type_.type = nebula::cpp2::SupportedType::VID;
        } else {
            type_.type = nebula::cpp2::SupportedType::INT;
        }
        pikType_ = pikType;
    }

    bool fromTagFilter() const {
        return (prop_.owner == cpp2::PropOwner::DEST || prop_.owner == cpp2::PropOwner::SOURCE)
                && filtered_;
    }

    const std::string& tagOrEdgeName() const {
        CHECK(filtered_);
        return tagOrEdgeName_;
    }

    void setTagOrEdgeName(const std::string& tagName) {
        filtered_ = true;
        tagOrEdgeName_ = tagName;
    }

    bool filtered() const {
        return filtered_;
    }

    cpp2::PropDef prop_;
    nebula::cpp2::ValueType type_;
    PropInKeyType pikType_ = PropInKeyType::NONE;
    mutable boost::variant<int64_t, double> sum_ = 0L;
    mutable int32_t count_    = 0;
    // The index in request return columns.
    int32_t retIndex_ = -1;
    // The prop should be returned
    bool    returned_ = false;

private:
    // If the prop is from tag, it is tagName otherwise it is edge name.
    // It is only valid when filtered_ is true.
    std::string tagOrEdgeName_;
    // The prop comes from filter.
    bool    filtered_ = false;
};

struct TagContext {
    TagContext() {
        props_.reserve(8);
    }

    TagID tagId_ = 0;
    std::vector<PropContext> props_;
    std::unordered_map<std::string, int32_t> propNameIndex_;

    PropContext* findProp(const std::string& propName) {
        auto it = propNameIndex_.find(propName);
        if (it != propNameIndex_.end()) {
            CHECK(it->second >= 0 && (size_t)it->second < props_.size());
            return &props_[it->second];
        }
        return nullptr;
    }

    void pushFilterProp(const std::string& tagName,
                        const std::string& propName,
                        const nebula::cpp2::ValueType& type) {
        PropContext pc;
        pc.prop_.name = propName;
        pc.type_ = type;
        pc.prop_.owner = cpp2::PropOwner::SOURCE;
        pc.retIndex_ = props_.size();
        pc.setTagOrEdgeName(tagName);
        props_.emplace_back(std::move(pc));
        propNameIndex_.emplace(propName, props_.size() - 1);
    }
};


bool checkDataExpiredForTTL(const meta::SchemaProviderIf* schema,
                            RowReader* reader,
                            const std::string& ttlCol,
                            int64_t ttlDuration);



}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMMON_H_
