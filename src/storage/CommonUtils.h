/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_COMMON_H_
#define STORAGE_COMMON_H_

#include "base/Base.h"
#include "filter/Expressions.h"

namespace nebula {
namespace storage {

class CommonUtils {
public:
    static std::string buildTagKeyInFilter(const std::string& tag, const std::string& prop) {
        return folly::stringPrintf("%s_%s", tag.c_str(), prop.c_str());
    }
};

struct FilterContext {
    // key: tagName + propName -> propValue
    // TODO(heng): figure out a better way.
    std::unordered_map<std::string, VariantType> tagFilters_;
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
    PropContext(const char* name, int32_t retIndex, PropContext::PropInKeyType pikType) {
        prop_.name = name;
        prop_.owner = cpp2::PropOwner::EDGE;
        retIndex_ = retIndex;
        type_.type = nebula::cpp2::SupportedType::INT;
        pikType_ = pikType;
    }

    bool fromTagFilter() const {
        return (prop_.owner == cpp2::PropOwner::DEST || prop_.owner == cpp2::PropOwner::SOURCE)
                && filtered_;
    }

    const std::string& tagFilterKey() const {
        CHECK(filtered_);
        return tagFilterKey_;
    }

    void setTagFilterKey(const std::string& tagName, const std::string& propName) {
        filtered_ = true;
        tagFilterKey_ = CommonUtils::buildTagKeyInFilter(tagName, propName);
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
    // filter Key inside tagFilters of FilterContext.  Only valid when filtered_ is true.
    // Make it private, because it should be accessed through tagFilterKey() and setTagFilterKey().
    std::string tagFilterKey_;
    // The prop comes from filter.
    bool    filtered_ = false;
};

struct TagContext {
    TagContext() {
        props_.reserve(8);
    }

    TagID tagId_ = 0;
    std::vector<PropContext> props_;

    PropContext* findProp(const std::string& propName) {
        for (auto& prop : props_) {
            if (prop.prop_.name == propName) {
                return &prop;
            }
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
        pc.setTagFilterKey(tagName, propName);
        props_.emplace_back(std::move(pc));
    }
};

struct EdgeContext {
    EdgeContext() {
        props_.reserve(8);
    }

    EdgeType edgeType_ = 0;
    std::vector<PropContext> props_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COMMON_H_

