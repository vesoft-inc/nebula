/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_COLLECTOR_H_
#define STORAGE_COLLECTOR_H_

#include "base/Base.h"
#include "dataman/RowWriter.h"
#include <boost/variant.hpp>

namespace nebula {
namespace storage {

struct PropContext {
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

    cpp2::PropDef prop_;
    nebula::cpp2::ValueType type_;
    PropInKeyType pikType_ = PropInKeyType::NONE;
    boost::variant<int64_t, double> sum_ = 0L;
    int32_t count_    = 0;
    // The index in request return columns.
    int32_t retIndex_ = -1;
};

struct TagContext {
    TagContext() {
        props_.reserve(8);
    }

    TagID tagId_ = 0;
    std::vector<PropContext> props_;
};

struct EdgeContext {
    EdgeContext() {
        props_.reserve(8);
    }

    EdgeType edgeType_ = 0;
    std::vector<PropContext> props_;
};


class Collector {
public:
    Collector() = default;

    virtual ~Collector() = default;

    virtual void collectInt32(ResultType ret, int32_t v, PropContext& prop) = 0;

    virtual void collectInt64(ResultType ret, int64_t v, PropContext& prop) = 0;

    virtual void collectFloat(ResultType ret, float v, PropContext& prop) = 0;

    virtual void collectDouble(ResultType ret, double v, PropContext& prop) = 0;

    virtual void collectString(ResultType ret, folly::StringPiece& v, PropContext& prop) = 0;
};


class PropsCollector : public Collector {
public:
    explicit PropsCollector(RowWriter* writer)
                : writer_(writer) {}

    void collectInt32(ResultType ret, int32_t v, PropContext& prop) override {
        collect<int32_t>(ret, v, prop);
    }

    void collectInt64(ResultType ret, int64_t v, PropContext& prop) override {
        collect<int64_t>(ret, v, prop);
    }

    void collectFloat(ResultType ret, float v, PropContext& prop) override {
        collect<float>(ret, v, prop);
    }

    void collectDouble(ResultType ret, double v, PropContext& prop) override {
        collect<double>(ret, v, prop);
    }

    void collectString(ResultType ret, folly::StringPiece& v, PropContext& prop) override {
        collect<folly::StringPiece>(ret, v, prop);
    }

    template<typename V>
    void collect(ResultType ret, V& v, PropContext& prop) {
        UNUSED(prop);
        if (ResultType::SUCCEEDED == ret) {
            (*writer_) << v;
        }
    }

private:
    RowWriter* writer_ = nullptr;
};


class StatsCollector : public Collector {
public:
    StatsCollector() = default;

    void collectInt32(ResultType ret, int32_t v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<int64_t>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectInt64(ResultType ret, int64_t v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<int64_t>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectFloat(ResultType ret, float v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<double>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectDouble(ResultType ret, double v, PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            prop.sum_ = boost::get<double>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectString(ResultType ret, folly::StringPiece& v, PropContext& prop) override {
        UNUSED(v);
        if (ret == ResultType::SUCCEEDED) {
            prop.count_++;
        }
    }
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COLLECTOR_H_

