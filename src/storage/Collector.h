/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
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
    mutable boost::variant<int64_t, double> sum_ = 0L;
    mutable int32_t count_    = 0;
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

    virtual void collectInt32(ResultType ret, int32_t v, const PropContext& prop) = 0;

    virtual void collectInt64(ResultType ret, int64_t v, const PropContext& prop) = 0;

    virtual void collectFloat(ResultType ret, float v, const PropContext& prop) = 0;

    virtual void collectDouble(ResultType ret, double v, const PropContext& prop) = 0;

    virtual void collectString(ResultType ret, folly::StringPiece& v, const PropContext& prop) = 0;
};


class PropsCollector : public Collector {
public:
    explicit PropsCollector(RowWriter* writer)
                : writer_(writer) {}

    void collectInt32(ResultType ret, int32_t v, const PropContext& prop) override {
        collect<int32_t>(ret, v, prop);
    }

    void collectInt64(ResultType ret, int64_t v, const PropContext& prop) override {
        collect<int64_t>(ret, v, prop);
    }

    void collectFloat(ResultType ret, float v, const PropContext& prop) override {
        collect<float>(ret, v, prop);
    }

    void collectDouble(ResultType ret, double v, const PropContext& prop) override {
        collect<double>(ret, v, prop);
    }

    void collectString(ResultType ret, folly::StringPiece& v, const PropContext& prop) override {
        collect<folly::StringPiece>(ret, v, prop);
    }

    template<typename V>
    void collect(ResultType ret, V& v, const PropContext& prop) {
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

    void collectInt32(ResultType ret, int32_t v, const PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            std::lock_guard<std::mutex> lg(lock_);
            prop.sum_ = boost::get<int64_t>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectInt64(ResultType ret, int64_t v, const PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            std::lock_guard<std::mutex> lg(lock_);
            prop.sum_ = boost::get<int64_t>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectFloat(ResultType ret, float v, const PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            std::lock_guard<std::mutex> lg(lock_);
            prop.sum_ = boost::get<double>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectDouble(ResultType ret, double v, const PropContext& prop) override {
        if (ret == ResultType::SUCCEEDED) {
            std::lock_guard<std::mutex> lg(lock_);
            prop.sum_ = boost::get<double>(prop.sum_) + v;
            prop.count_++;
        }
    }

    void collectString(ResultType ret, folly::StringPiece& v, const PropContext& prop) override {
        UNUSED(v);
        if (ret == ResultType::SUCCEEDED) {
            std::lock_guard<std::mutex> lg(lock_);
            prop.count_++;
        }
    }

private:
    std::mutex lock_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_COLLECTOR_H_

